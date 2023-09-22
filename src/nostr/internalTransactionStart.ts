import { Debugger } from 'debug';
import type { NDKFilter, NostrEvent } from '@nostr-dev-kit/ndk';
import { Decimal, NotFoundError } from '@prisma/client/runtime/library';
import { Context } from '@type/request';

import {
  balanceEvent,
  Kind,
  nostrEventToDB,
  txErrorEvent,
  txOkEvent,
} from '@lib/events';
import {
  BalancesByAccount,
  ExtBalance,
  getTxTypeId,
  InternalTransaction,
  snapshotCreate,
} from '@lib/transactions';
import { requiredEnvVar, logger, nowInSeconds } from '@lib/utils';

const log: Debugger = logger.extend('nostr:internalTransactionStart');
const debug: Debugger = log.extend('debug');
const warn: Debugger = log.extend('warn');
const error: Debugger = log.extend('error');
const MAX_RETRIES = 10;

const filter: NDKFilter = {
  kinds: [Kind.REGULAR.valueOf()],
  '#p': [requiredEnvVar('NOSTR_PUBLIC_KEY')],
  '#t': ['internal-transaction-start'],
  since: nowInSeconds() - 86000,
};

/**
 * Return the internal-transaction handler
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
   * Handle a internal-transaction event
   *
   * If it receives an event that haven't been handled before, the
   * token(s) exists and the sender has enough funds, move the funds
   * from the sender's balance to the receiver's and publish the result
   * in nostr
   *
   * Handles:
   *  - 'internal-transaction-start'
   *
   * Publishes:
   *  - 'internal-transaction-ok' if the funds were transferred
   *  - 'internal-transaction-error' if the funds were not transferred
   */
  return async (nostrEvent: NostrEvent) => {
    log('Received event %s', nostrEvent.id);
    debug('%O', {
      id: nostrEvent.id,
      author: nostrEvent.pubkey,
      tags: nostrEvent.tags,
      content: nostrEvent.content,
    });

    // Have we handled it before?
    const dbEvent = await ctx.prisma.event.findUnique({
      select: { id: true },
      where: { id: nostrEvent.id },
    });
    if (null !== dbEvent) {
      log('Already handled event %s', nostrEvent.id);
      return;
    }

    const event = nostrEventToDB(nostrEvent);
    const internalTx: InternalTransaction = {
      senderId: nostrEvent.pubkey,
      receiverId: nostrEvent.tags.filter((t) => t[0] == 'p')[1][1],
      eventId: event.id,
      content: event.payload,
    };

    // Tokens exist?
    const tokenNames: string[] = Object.keys(internalTx.content.tokens);
    const tokens = await ctx.prisma.token.findMany({
      where: { name: { in: tokenNames } },
    });
    if (tokens.length != tokenNames.length) {
      await ctx.prisma.event.create({ data: event });
      ctx.outbox.publish(txErrorEvent('Token not supported', internalTx));
      return;
    }

    // TODO: Store in memory on start-up or use prisma cache
    //  https://www.prisma.io/docs/data-platform/accelerate
    const txTypeId = await getTxTypeId(ctx.prisma, 'internal-transaction');
    if (txTypeId === undefined) {
      await ctx.prisma.event.create({ data: event });
      ctx.outbox.publish(txErrorEvent('Transaction not supported', internalTx));
      return;
    }

    ctx.prisma
      .$transaction(async (tx) => {
        debug('Starting transaction for %s', event.id);
        const balances = await tx.balance.findMany({
          where: {
            OR: [
              {
                // sender's balances
                accountId: internalTx.senderId,
                OR: tokens.map((t) => ({
                  tokenId: t.id,
                  snapshot: {
                    amount: { gte: internalTx.content.tokens[t.name] },
                  },
                })),
              },
              {
                // receiver's balances
                accountId: internalTx.receiverId,
                OR: tokens.map((t) => ({ tokenId: t.id })),
              },
            ],
          },
          include: { snapshot: true, token: true },
        });
        const balancesByAccount = balances.reduce(
          (group: BalancesByAccount, balance) => {
            if (internalTx.senderId === balance.accountId) {
              group.sender.push(balance);
            } else {
              group.receiver.push(balance);
            }
            return group;
          },
          { sender: [], receiver: [] },
        );
        // Does sender have the funds?
        if (balancesByAccount.sender.length != tokenNames.length) {
          throw new NotFoundError('', '');
        }

        const transaction = await tx.transaction.create({
          data: {
            transactionType: { connect: { id: txTypeId } },
            event: { create: event },
            payload: event.payload,
          },
        });

        const existingBalances = balancesByAccount.receiver.map(
          (b) => b.tokenId,
        );
        const newBalances = balancesByAccount.sender
          .map((b) => b.token)
          .filter((t) => existingBalances.indexOf(t.id) < 0);
        for (const balance of balancesByAccount.sender) {
          const txAmount: number =
            internalTx.content.tokens[balance.token.name];
          balance.eventId = event.id;
          balance.snapshot = {
            ...balance.snapshot,
            amount: balance.snapshot.amount.sub(txAmount),
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
              snapshot: { create: snapshotCreate(balance, -txAmount) },
            },
          });
        }
        for (const balance of balancesByAccount.receiver) {
          const txAmount = internalTx.content.tokens[balance.token.name];
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
          const txAmount = internalTx.content.tokens[token.name];
          balancesByAccount.receiver.push({
            token,
            snapshot: { amount: new Decimal(txAmount) },
            accountId: internalTx.receiverId,
          } as ExtBalance);
          // Create two interconnected one-to-one records at the same time
          await tx.$executeRaw`
          WITH ins_balance AS (
            INSERT INTO balances (account_id, token_id, snapshot_id, event_id)
            VALUES (${internalTx.receiverId}, ${token.id}::uuid,
              pg_catalog.gen_random_uuid(), ${event.id})
            RETURNING *
          )
          INSERT INTO balance_snapshots
            (id, amount, transaction_id, event_id, delta, token_id, account_id)
          SELECT snapshot_id, ${txAmount}, ${transaction.id}::uuid,
            ${event.id}, ${txAmount},  ${token.id}::uuid, ${internalTx.receiverId}
          FROM   ins_balance;`;
        }
        return balancesByAccount;
      })
      .then((balancesByAccount: BalancesByAccount) => {
        debug('Transaction completed ok: %s', event.id);
        ctx.outbox.publish(txOkEvent(internalTx));
        [...balancesByAccount.sender, ...balancesByAccount.receiver].forEach(
          (b) => ctx.outbox.publish(balanceEvent(b, event.id)),
        );
        debug('Ok published');
        log('Finished handling event %s', event.id);
      })
      .catch(async (e) => {
        if (e instanceof NotFoundError) {
          log('Failing because not enough funds. %s', event.id);
          await ctx.prisma.event.create({ data: event });
          ctx.outbox.publish(txErrorEvent('Not enough funds', internalTx));
          return;
        }
        warn('Transaction failed, reason: %O', e);
        if (ntry < MAX_RETRIES) {
          log('Retrying event %s', event.id);
          await getHandler(ctx, ++ntry)(nostrEvent);
        } else {
          error('Too many retries for %s, failing transaction', event.id);
          await ctx.prisma.event.create({ data: event });
          ctx.outbox.publish(txErrorEvent('Network Error', internalTx));
        }
      });
  };
};

export { filter, getHandler };
