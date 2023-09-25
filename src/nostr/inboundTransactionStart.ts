import { Debugger } from 'debug';
import type { NDKFilter, NostrEvent } from '@nostr-dev-kit/ndk';
import { Decimal, NotFoundError } from '@prisma/client/runtime/library';
import { Context } from '@type/request';

import { balanceEvent, Kind, txErrorEvent, txOkEvent } from '@lib/events';
import {
  ExtBalance,
  getTxHandler,
  ITransaction,
  snapshotCreate,
  TransactionType,
} from '@lib/transactions';
import { requiredEnvVar, logger, nowInSeconds } from '@lib/utils';
import { Prisma, Token } from '@prisma/client';

const log: Debugger = logger.extend('nostr:inboundTransactionStart');
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
 * Injects the context to the handler and also `ntry` based on which
 * the handler will decide if it must retry handling the event in case of
 * unknown error.
 */
const getHandler = (
  ctx: Context,
  ntry: number,
): ((nostrEvent: NostrEvent) => void) => {
  /**
   * Handle an inbound-transaction event
   *
   * If the sender has enough funds, move the funds
   * from the sender's balance to the receiver's and publish the result
   * in nostr
   *
   * Handles:
   *  - 'inbound-transaction-start'
   *
   * Publishes:
   *  - 'inbound-transaction-ok' if the funds were transferred
   *  - 'inbound-transaction-error' if the funds were not transferred
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
      // TODO: check if author can mint
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

          const balances = await tx.balance.findMany({
            where: {
              accountId: intTx.receiverId,
              OR: tokens.map((t) => ({ tokenId: t.id })),
            },
            include: { snapshot: true, token: true },
          });
          const newBalances = tokens.filter(
            (tk) => balances.map((t) => t.tokenId).indexOf(tk.id) < 0,
          );
          for (const balance of balances) {
            const txAmount: number = intTx.content.tokens[balance.token.name];
            balance.eventId = event.id;
            balance.snapshot = {
              ...balance.snapshot,
              amount: balance.snapshot.amount.add(txAmount),
              transactionId: transaction.id,
            };
            await tx.balance.update({
              where: {
                accountId_tokenId: {
                  tokenId: balance.tokenId,
                  accountId: balance.accountId,
                },
              },
              data: {
                event: { connect: { id: transaction.eventId } },
                snapshot: { create: snapshotCreate(balance, txAmount) },
              },
            });
          }
          for (const token of newBalances) {
            const txAmount = intTx.content.tokens[token.name];
            balances.push({
              token,
              snapshot: { amount: new Decimal(txAmount) },
              accountId: intTx.receiverId,
            } as ExtBalance);
            // Create two interconnected one-to-one records at the same time
            await tx.$executeRaw`
            WITH ins_balance AS (
              INSERT INTO balances (account_id, token_id, snapshot_id, event_id)
              VALUES (${intTx.receiverId}, ${token.id}::uuid,
                pg_catalog.gen_random_uuid(), ${event.id})
              RETURNING *
            )
            INSERT INTO balance_snapshots
              (id, amount, transaction_id, event_id, delta, token_id, account_id)
            SELECT snapshot_id, ${txAmount}, ${transaction.id}::uuid,
              ${event.id}, ${txAmount},  ${token.id}::uuid, ${intTx.receiverId}
            FROM   ins_balance;`;
          }
          return balances;
        })
        .then((balances: ExtBalance[]) => {
          debug('Transaction completed ok: %s', event.id);
          ctx.outbox.publish(txOkEvent(intTx));
          balances.forEach((b) =>
            ctx.outbox.publish(balanceEvent(b, event.id)),
          );
          debug('Ok published');
          log('Finished handling event %s', event.id);
        })
        .catch(async (e) => {
          if (e instanceof NotFoundError) {
            log('Failing because not enough funds. %s', event.id);
            await ctx.prisma.event.create({ data: event });
            ctx.outbox.publish(txErrorEvent('Not enough funds', intTx));
          } else {
            warn('Transaction failed, reason: %O', e);
            if (ntry < MAX_RETRIES) {
              log('Retrying event %s', event.id);
              await getHandler(ctx, ++ntry)(nostrEvent);
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
