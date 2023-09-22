import { NostrEvent } from '@nostr-dev-kit/ndk';

import { ExtBalance, InternalTransaction } from '@lib/transactions';
import { nowInSeconds, requiredEnvVar, requiredProp } from '@lib/utils';

export enum Kind {
  REGULAR = 1112,
  EPHEMERAL = 21111,
  PARAMETRIZED_REPLACEABLE = 31111,
}

/**
 * Creates a response event for publishing on nostr after processing a
 * transaction.
 */
function txResultEvent(
  content: string,
  tx: InternalTransaction,
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
      ['t', `internal-transaction-${success ? 'ok' : 'error'}`],
    ],
  };
}

/**
 * Creates a database model of a nostr event, mainly for persisting it
 * to the database.
 */
export function nostrEventToDB(event: NostrEvent) {
  return {
    id: requiredProp<string>(event, 'id'),
    signature: requiredProp<string>(event, 'sig'),
    author: event.pubkey,
    signer: event.pubkey,
    kind: requiredProp<number>(event, 'kind'),
    payload: JSON.parse(event.content),
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
export function txOkEvent(tx: InternalTransaction): NostrEvent {
  return txResultEvent('{}', tx, true);
}

/**
 * Creates an event for publishing on nostr after encoutering an error
 * while processing a transaction.
 */
export function txErrorEvent(
  message: string,
  tx: InternalTransaction,
): NostrEvent {
  return txResultEvent(`{messages:["${message}"]}`, tx, false);
}
