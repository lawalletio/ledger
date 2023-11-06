import { Debugger } from 'debug';
import { NostrEvent } from '@nostr-dev-kit/ndk';
import { nip26, Event } from 'nostr-tools';

import { ExtBalance, ITransaction } from '@lib/transactions';
import { logger, nowInSeconds, requiredEnvVar, requiredProp } from '@lib/utils';

export enum Kind {
  REGULAR = 1112,
  EPHEMERAL = 21111,
  PARAMETRIZED_REPLACEABLE = 31111,
}

const log: Debugger = logger.extend('nostr:events');
const warn: Debugger = log.extend('warn');

export const REPUBLISH_INTERVAL_MS: number = 1000; // 1s

/**
 * Creates a response event for publishing on nostr after processing a
 * transaction.
 */
function txResultEvent(
  content: string,
  tx: ITransaction,
  success: boolean,
): NostrEvent {
  let tags = [
    ['p', tx.senderId],
    ['p', tx.receiverId],
    ['e', tx.eventId],
    ['t', success ? tx.txType.ok : tx.txType.error],
  ];
  if (tx.extraTags) {
    tags = tags.concat(tx.extraTags);
  }
  return {
    content: content,
    created_at: nowInSeconds(),
    kind: Kind.REGULAR.valueOf(),
    pubkey: requiredEnvVar('NOSTR_PUBLIC_KEY'),
    tags,
  };
}

/**
 * Creates a model of a nostr event
 *
 * For persiting in the database and to handle NIP-26 author and signer
 */
export function nostrEventToDB(event: NostrEvent) {
  let payload: any;
  try {
    payload = JSON.parse(event.content, (k, v) => (isNaN(v) ? v : BigInt(v)));
  } catch {
    warn('Error parsing content %O', event.content);
  }

  const author = event.tags.some((t) => 'delegation' === t[0])
    ? nip26.getDelegator(event as Event<number>)
    : event.pubkey;

  if (null === author) {
    throw new Error('Invalid author');
  }

  return {
    id: requiredProp<string>(event, 'id'),
    signature: requiredProp<string>(event, 'sig'),
    author,
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
  return txResultEvent(
    JSON.stringify(tx.content, (_, v) =>
      typeof v === 'bigint' ? Number(v) : v,
    ),
    tx,
    true,
  );
}

/**
 * Creates an event for publishing on nostr after encoutering an error
 * while processing a transaction.
 */
export function txErrorEvent(message: string, tx: ITransaction): NostrEvent {
  return txResultEvent(`{messages:["${message}"]}`, tx, false);
}
