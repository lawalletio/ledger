import { Debugger } from 'debug';
import type { NDKFilter, NostrEvent } from '@nostr-dev-kit/ndk';

import {
  balanceEvent,
  Kind,
  txErrorEvent,
  txOkEvent,
  REPUBLISH_INTERVAL_MS,
} from '@lib/events';
import {
  ExtBalance,
  getTxHandler,
  ITransaction,
  TransactionType,
  removeFromBalances,
} from '@lib/transactions';
import { requiredEnvVar, logger, nowInSeconds } from '@lib/utils';
import { Prisma, Token } from '@prisma/client';
import { Context } from '@type/request';

const log: Debugger = logger.extend('nostr:outboundTransaction');
const debug: Debugger = log.extend('debug');
const warn: Debugger = log.extend('warn');
const error: Debugger = log.extend('error');
const MAX_RETRIES = 10;

const filter: NDKFilter = {
  kinds: [Kind.REGULAR.valueOf()],
  '#p': [requiredEnvVar('NOSTR_PUBLIC_KEY')],
  '#t': [TransactionType.OUTBOUND.start],
  since: nowInSeconds() - 86000,
};

/**
 * Return the outbound-transaction handler
 *
 * Injects the context to the handler and also `ntry` based on which
 * the handler will decide if it must retry handling the event in case of
 * unknown error.
 */
const getHandler = (
  ctx: Context,
  ntry: number,
): ((nostrEvent: NostrEvent) => void) => {
  /**
   * Handle an outbound-transaction event
   *
   * If the author is a burner, burn the funds in its account account
   * and publish the result in nostr.
   *
   * Handles:
   *  - 'outbound-transaction-start'
   *
   * Publishes:
   *  - 'outbound-transaction-ok' if the funds were burned
   *  - 'outbound-transaction-error' if the funds were not burned
   */
  return getTxHandler(
    ctx,
    ntry,
    TransactionType.OUTBOUND,
    async (
      nostrEvent: NostrEvent,
      event: Prisma.EventCreateInput,
      intTx: ITransaction,
      tokens: Token[],
    ) => {
      if (event.author !== requiredEnvVar('MINTER_PUBLIC_KEY')) {
        warn('Non-burner is trying to burn. %s', event.id);
        await ctx.prisma.event.create({ data: event });
        ctx.outbox.publish(
          txErrorEvent('Author cannot burn this token', intTx),
        );
      }

      ctx.prisma
        .$transaction(async (tx) => {
          debug('Starting transaction for %s', event.id);

          const transaction = await tx.transaction.create({
            data: {
              transactionType: { connect: { id: intTx.txTypeId } },
              event: { create: event },
              payload: event.payload,
            },
          });

          let balances = await tx.balance.findMany({
            where: {
              accountId: intTx.senderId,
              OR: tokens.map((t) => ({ tokenId: t.id })),
            },
            include: { snapshot: true, token: true },
          });
          balances = await removeFromBalances(
            balances,
            event,
            tx,
            transaction,
            intTx,
          );
          return balances;
        })
        .then((balances: ExtBalance[]) => {
          debug('Transaction completed ok: %s', event.id);
          const okEvent = txOkEvent(intTx);
          // Add original internal-start eventId
          okEvent.tags.concat(nostrEvent.tags.filter((t) => t[0] == 'e'));
          ctx.outbox.publish(txOkEvent(intTx));
          balances.forEach((b) =>
            ctx.outbox.publish(balanceEvent(b, event.id)),
          );
          if (balances.length !== 0) {
            setTimeout(async () => {
              (
                await ctx.prisma.balance.findMany({
                  where: {
                    accountId: balances[0].accountId,
                    tokenId: {
                      in: balances.map((b) => {
                        return b.tokenId;
                      }),
                    },
                  },
                  include: { snapshot: true, token: true },
                })
              ).forEach((b) => {
                debug(
                  `RE-publishing events balance:${b.token.name}:${b.accountId}`,
                );
                ctx.outbox.publish(balanceEvent(b, b.eventId));
              });
            }, REPUBLISH_INTERVAL_MS);
          }
          debug('Ok published');
          log('Finished handling event %s', event.id);
        })
        .catch(async (e) => {
          if (e.code === 'P2025') {
            log('Failing because not enough funds. %s', event.id);
            await ctx.prisma.event.create({ data: event });
            ctx.outbox.publish(txErrorEvent('Not enough funds', intTx));
          } else {
            warn('Transaction failed, reason: %O', e);
            if (ntry < MAX_RETRIES) {
              log('Retrying event %s', event.id);
              getHandler(ctx, ++ntry)(nostrEvent);
            } else {
              error('Too many retries for %s, failing transaction', event.id);
              await ctx.prisma.event.create({ data: event });
              ctx.outbox.publish(txErrorEvent('Network Error', intTx));
            }
          }
        });
    },
  );
};

export { filter, getHandler };
