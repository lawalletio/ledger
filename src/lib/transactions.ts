import { Debugger } from 'debug';
import { NostrEvent } from '@nostr-dev-kit/ndk';
import { Prisma, PrismaClient } from '@prisma/client';
import { Context } from '@type/request';
import { logger } from '@lib/utils';
import { nostrEventToDB, txErrorEvent } from './events';

const log: Debugger = logger.extend('nostr:transactions');
const debug: Debugger = log.extend('debug');

export type ExtBalance = Prisma.BalanceGetPayload<{
  include: { token: true; snapshot: true };
}>;

export class TransactionType {
  static INTERNAL = new TransactionType('internal-transaction');

  static INBOUND = new TransactionType('inbound-transaction');

  static OUTBOUND = new TransactionType('outbound-transaction');

  name: string;

  constructor(name: string) {
    this.name = name;
  }

  get start(): string {
    return `${this.name}-start`;
  }

  get ok(): string {
    return `${this.name}-ok`;
  }

  get error(): string {
    return `${this.name}-error`;
  }
}

export type ITransaction = {
  txType: TransactionType;
  txTypeId: string;
  senderId: string;
  receiverId: string;
  eventId: string;
  content: {
    tokens: Record<string, number>;
    memo?: string;
  };
};

export type BalancesByAccount = {
  sender: ExtBalance[];
  receiver: ExtBalance[];
};

/**
 * Return the data needed for creating a balance snapshot
 */
export function snapshotCreate(
  balance: ExtBalance,
  delta: number,
): Prisma.BalanceSnapshotUncheckedCreateInput {
  return {
    prevSnapshotId: balance.snapshotId,
    amount: balance.snapshot.amount,
    transactionId: balance.snapshot.transactionId,
    eventId: balance.eventId,
    delta,
    tokenId: balance.tokenId,
    accountId: balance.accountId,
  };
}

export async function getTxTypeId(
  prisma: PrismaClient,
  typeName: string,
): Promise<string | undefined> {
  return prisma.transactionType
    .findFirst({
      where: { description: { equals: typeName } },
    })
    .then((t) => t?.id);
}

/**
 * Basic common checks for handling events
 *
 * If it receives an event that haven't been handled before and the
 * token(s) exists run the provided handler
 */
export function getTxHandler(
  ctx: Context,
  ntry: number,
  txType: TransactionType,
  handler: Function,
) {
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
    const tx: ITransaction = {
      txTypeId: '',
      txType: txType,
      senderId: nostrEvent.pubkey,
      receiverId: nostrEvent.tags.filter((t) => t[0] == 'p')[1][1],
      eventId: event.id,
      content: event.payload,
    };
    if (undefined === event.payload) {
      log('Unable to parse content for %s', event.id);
      event.payload = {};
      await ctx.prisma.event.create({ data: event });
      ctx.outbox.publish(txErrorEvent('Unparsable content', tx));
      return;
    }

    // Tokens exist?
    const tokenNames: string[] = Object.keys(tx.content.tokens);
    const tokens = await ctx.prisma.token.findMany({
      where: { name: { in: tokenNames } },
    });
    if (tokens.length != tokenNames.length) {
      await ctx.prisma.event.create({ data: event });
      log('Token not supported. %s', event.id);
      ctx.outbox.publish(txErrorEvent('Token not supported', tx));
      return;
    }

    // TODO: Store in memory on start-up or use prisma cache
    //  https://www.prisma.io/docs/data-platform/accelerate
    const txTypeId = await getTxTypeId(ctx.prisma, tx.txType.name);
    if (txTypeId === undefined) {
      await ctx.prisma.event.create({ data: event });
      ctx.outbox.publish(txErrorEvent('Transaction not supported', tx));
      return;
    }
    tx.txTypeId = txTypeId;
    await handler(nostrEvent, event, tx, tokens);
  };
}
