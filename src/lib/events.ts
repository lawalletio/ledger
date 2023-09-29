import { Debugger } from 'debug';
import { NostrEvent } from '@nostr-dev-kit/ndk';

import { ExtBalance, ITransaction } from '@lib/transactions';
import { logger, nowInSeconds, requiredEnvVar, requiredProp } from '@lib/utils';

export enum Kind {
  REGULAR = 1112,
  EPHEMERAL = 21111,
  PARAMETRIZED_REPLACEABLE = 31111,
}

const log: Debugger = logger.extend('nostr:events');
const warn: Debugger = log.extend('warn');

/**
 * Creates a response event for publishing on nostr after processing a
 * transaction.
 */
function txResultEvent(
  content: string,
  tx: ITransaction,
  success: boolean,
): NostrEvent {
  return {
    content: content,
    created_at: nowInSeconds(),
    kind: Kind.REGULAR.valueOf(),
    pubkey: requiredEnvVar('NOSTR_PUBLIC_KEY'),
    tags: [
      ['p', tx.senderId],
      ['p', tx.receiverId],
      ['e', tx.eventId],
      ['t', success ? tx.txType.ok : tx.txType.error],
    ],
  };
}

/**
 * Creates a database model of a nostr event, mainly for persisting it
 * to the database.
 */
export function nostrEventToDB(event: NostrEvent) {
  let payload: any;
  try {
    payload = JSON.parse(event.content);
  } catch {
    warn('Error parsing content %O', event.content);
  }
  return {
    id: requiredProp<string>(event, 'id'),
    signature: requiredProp<string>(event, 'sig'),
    author: event.pubkey,
    signer: event.pubkey,
    kind: requiredProp<number>(event, 'kind'),
    payload,
  };
}

/**
 * Creates an event that communicates the current balance of an account.
 */
export function balanceEvent(balance: ExtBalance, eventId: string): NostrEvent {
  return {
    content: '{}',
    created_at: nowInSeconds(),
    kind: Kind.PARAMETRIZED_REPLACEABLE.valueOf(),
    pubkey: requiredEnvVar('NOSTR_PUBLIC_KEY'),
    tags: [
      ['p', balance.accountId],
      ['d', `balance:${balance.token.name}:${balance.accountId}`],
      ['e', eventId],
      ['amount', balance.snapshot.amount.toString()],
    ],
  };
}

/**
 * Creates an event for publishing on nostr after correctly processing a
 * transaction.
 */
export function txOkEvent(tx: ITransaction): NostrEvent {
  return txResultEvent('{}', tx, true);
}

/**
 * Creates an event for publishing on nostr after encoutering an error
 * while processing a transaction.
 */
export function txErrorEvent(message: string, tx: ITransaction): NostrEvent {
  return txResultEvent(`{messages:["${message}"]}`, tx, false);
}
