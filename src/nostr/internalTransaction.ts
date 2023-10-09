import { Debugger } from 'debug';
import type { NDKFilter, NostrEvent } from '@nostr-dev-kit/ndk';
import { PrismaClientKnownRequestError } from '@prisma/client/runtime/library';

import { balanceEvent, Kind, txErrorEvent, txOkEvent } from '@lib/events';
import {
  BalancesByAccount,
  TransactionType,
  getTxHandler,
  ITransaction,
  addToBalances,
  createBalances,
  removeFromBalances,
} from '@lib/transactions';
import { requiredEnvVar, logger, nowInSeconds } from '@lib/utils';
import { Prisma, Token } from '@prisma/client';
import prisma from '@services/prisma';
import outbox from '@services/outbox';

const log: Debugger = logger.extend('nostr:internalTransaction');
const debug: Debugger = log.extend('debug');
const warn: Debugger = log.extend('warn');
const error: Debugger = log.extend('error');
const MAX_RETRIES = 10;

const filter: NDKFilter = {
  kinds: [Kind.REGULAR.valueOf()],
  '#p': [requiredEnvVar('NOSTR_PUBLIC_KEY')],
  '#t': [TransactionType.INTERNAL.start],
  since: nowInSeconds() - 86000,
};

/**
 * Return the internal-transaction handler
 *
 * Injects the context to the handler and also `ntry` based on which
 * the handler will decide if it must retry handling the event in case of
 * unknown error.
 */
const getHandler = (ntry: number): ((nostrEvent: NostrEvent) => void) => {
  /**
   * Handle an internal-transaction event
   *
   * If the sender has enough funds, move the funds
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
  return getTxHandler(
    ntry,
    TransactionType.INTERNAL,
    async (
      nostrEvent: NostrEvent,
      event: Prisma.EventCreateInput,
      intTx: ITransaction,
      tokens: Token[],
    ) => {
      prisma
        .$transaction(async (tx) => {
          debug('Starting transaction for %s', event.id);
          const balances = await tx.balance.findMany({
            where: {
              OR: [
                {
                  // sender's balances
                  accountId: intTx.senderId,
                  OR: tokens.map((t) => ({
                    tokenId: t.id,
                    snapshot: {
                      amount: { gte: intTx.content.tokens[t.name] },
                    },
                  })),
                },
                {
                  // receiver's balances
                  accountId: intTx.receiverId,
                  OR: tokens.map((t) => ({ tokenId: t.id })),
                },
              ],
            },
            include: { snapshot: true, token: true },
          });
          const balancesByAccount = balances.reduce(
            (group: BalancesByAccount, balance) => {
              if (intTx.senderId === balance.accountId) {
                group.sender.push(balance);
              } else {
                group.receiver.push(balance);
              }
              return group;
            },
            { sender: [], receiver: [] },
          );
          // Does sender have the funds?
          if (balancesByAccount.sender.length != tokens.length) {
            throw new PrismaClientKnownRequestError('', {
              code: 'P2025',
              clientVersion: '',
            });
          }

          const transaction = await tx.transaction.create({
            data: {
              transactionType: { connect: { id: intTx.txTypeId } },
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
          balancesByAccount.sender = await removeFromBalances(
            balancesByAccount.sender,
            event,
            tx,
            transaction,
            intTx,
          );
          balancesByAccount.receiver = await addToBalances(
            balancesByAccount.receiver,
            event,
            tx,
            transaction,
            intTx,
          );
          balancesByAccount.receiver = balancesByAccount.receiver.concat(
            await createBalances(newBalances, event, tx, transaction, intTx),
          );
          return balancesByAccount;
        })
        .then((balancesByAccount: BalancesByAccount) => {
          debug('Transaction completed ok: %s', event.id);
          outbox.publish(txOkEvent(intTx));
          [...balancesByAccount.sender, ...balancesByAccount.receiver].forEach(
            (b) => outbox.publish(balanceEvent(b, event.id)),
          );
          debug('Ok published');
          log('Finished handling event %s', event.id);
        })
        .catch(async (e) => {
          if (e.code === 'P2025') {
            log('Failing because not enough funds. %s', event.id);
            await prisma.event.create({ data: event });
            outbox.publish(txErrorEvent('Not enough funds', intTx));
            return;
          }
          warn('Transaction failed, reason: %O', e);
          if (ntry < MAX_RETRIES) {
            log('Retrying event %s', event.id);
            await getHandler(++ntry)(nostrEvent);
          } else {
            error('Too many retries for %s, failing transaction', event.id);
            await prisma.event.create({ data: event });
            outbox.publish(txErrorEvent('Network Error', intTx));
          }
        });
    },
  );
};

export { filter, getHandler };
