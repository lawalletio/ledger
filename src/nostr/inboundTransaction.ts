import { Debugger } from 'debug';
import type { NDKFilter, NostrEvent } from '@nostr-dev-kit/ndk';

import { balanceEvent, Kind, txErrorEvent, txOkEvent } from '@lib/events';
import {
  ExtBalance,
  getTxHandler,
  ITransaction,
  TransactionType,
  addToBalances,
  createBalances,
} from '@lib/transactions';
import { requiredEnvVar, logger, nowInSeconds } from '@lib/utils';
import { Prisma, Token } from '@prisma/client';
import { Context } from '@type/request';

const log: Debugger = logger.extend('nostr:inboundTransaction');
const debug: Debugger = log.extend('debug');
const warn: Debugger = log.extend('warn');
const error: Debugger = log.extend('error');
const MAX_RETRIES = 10;

const filter: NDKFilter = {
  kinds: [Kind.REGULAR.valueOf()],
  '#p': [requiredEnvVar('NOSTR_PUBLIC_KEY')],
  '#t': [TransactionType.INBOUND.start],
  since: nowInSeconds() - 86000,
};

/**
 * Return the inbound-transaction handler
 *
 * Injects the context and `ntry` based on which the handler will decide
 * if it must retry handling the event in case of unknown error.
 */
const getHandler = (
  ctx: Context,
  ntry: number,
): ((nostrEvent: NostrEvent) => void) => {
  /**
   * Handle an inbound-transaction event
   *
   * If the author is a minter, create the funds in the receiver's
   * account and publish the result in nostr.
   *
   * Handles:
   *  - 'inbound-transaction-start'
   *
   * Publishes:
   *  - 'inbound-transaction-ok' if the funds were minted
   *  - 'inbound-transaction-error' if the funds were not minted
   */
  return getTxHandler(
    ctx,
    ntry,
    TransactionType.INBOUND,
    async (
      nostrEvent: NostrEvent,
      event: Prisma.EventCreateInput,
      intTx: ITransaction,
      tokens: Token[],
    ) => {
      if (event.author !== requiredEnvVar('MINTER_PUBLIC_KEY')) {
        warn('Non-minter is trying to mint. %s', event.id);
        await ctx.prisma.event.create({ data: event });
        ctx.outbox.publish(
          txErrorEvent('Author cannot mint this token', intTx),
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
              accountId: intTx.receiverId,
              OR: tokens.map((t) => ({ tokenId: t.id })),
            },
            include: { snapshot: true, token: true },
          });
          const newBalances = tokens.filter(
            (tk) => balances.map((t) => t.tokenId).indexOf(tk.id) < 0,
          );
          balances = await addToBalances(
            balances,
            event,
            tx,
            transaction,
            intTx,
          );
          balances = balances.concat(
            await createBalances(newBalances, event, tx, transaction, intTx),
          );
          return balances;
        })
        .then((balances: ExtBalance[]) => {
          ctx.outbox.publish(txOkEvent(intTx));
          balances.forEach((b) =>
            ctx.outbox.publish(balanceEvent(b, event.id)),
          );
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
              await getHandler(++ntry)(nostrEvent);
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
