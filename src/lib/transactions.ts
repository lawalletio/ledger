import { Debugger } from 'debug';
import { NostrEvent } from '@nostr-dev-kit/ndk';
import { Prisma, PrismaClient, Token, Transaction } from '@prisma/client';
import { logger } from '@lib/utils';
import { nostrEventToDB, txErrorEvent } from '@lib/events';
import { Context } from '@type/request';

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
    tokens: Record<string, bigint>;
    memo?: string;
  };
  extraTags?: string[][];
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
  delta: bigint,
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

/**
 * Return the database id of a transaction type
 */
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
    debug('%O', event);
    const tx: ITransaction = {
      txTypeId: '',
      txType: txType,
      senderId: event.author as string,
      receiverId: nostrEvent.tags.filter((t) => t[0] == 'p')[1][1],
      eventId: event.id,
      content: event.payload,
      extraTags: nostrEvent.tags.filter((t) => t[0] === 'e'),
    };
    if (undefined === event.payload) {
      log('Unable to parse content for %s', event.id);
      event.payload = {};
      await ctx.prisma.event.create({ data: event });
      ctx.outbox.publish(txErrorEvent('Unparsable content', tx));
      return;
    }
    if (null === event.author) {
      log('Bad delegation for %s', event.id);
      tx.senderId = nostrEvent.pubkey;
      event.author = nostrEvent.pubkey;
      await ctx.prisma.event.create({ data: event });
      ctx.outbox.publish(txErrorEvent('Bad delegation', tx));
      return;
    }

    const tokenNames: string[] = Object.keys(tx.content.tokens);
    if (tokenNames.map((t) => tx.content.tokens[t]).some((n) => n < 0n)) {
      await ctx.prisma.event.create({ data: event });
      log('Token amount must be a positive number. %s', event.id);
      ctx.outbox.publish(
        txErrorEvent('Token amount must be a positive number', tx),
      );
      return;
    }
    // Tokens exist?
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

/**
 * Apply a transaction to an array of balances
 *
 * Applies a transaction to a balance, having into considation the
 * direction of the funds flow.
 */
async function alterBalances(
  balances: ExtBalance[],
  event: Prisma.EventCreateInput,
  tx: Prisma.TransactionClient,
  transaction: Transaction,
  intTx: ITransaction,
  isInflow: boolean,
): Promise<ExtBalance[]> {
  for (const balance of balances) {
    const txAmount = intTx.content.tokens[balance.token.name];
    const balAmount = isInflow
      ? balance.snapshot.amount + txAmount
      : balance.snapshot.amount - txAmount;
    balance.eventId = event.id;
    balance.snapshot = {
      ...balance.snapshot,
      amount: balAmount,
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
        snapshot: {
          create: snapshotCreate(balance, isInflow ? txAmount : -txAmount),
        },
      },
    });
  }
  return balances;
}

/**
 * Add token amounts to existing balances
 */
export async function addToBalances(
  balances: ExtBalance[],
  event: Prisma.EventCreateInput,
  tx: Prisma.TransactionClient,
  transaction: Transaction,
  intTx: ITransaction,
): Promise<ExtBalance[]> {
  return alterBalances(balances, event, tx, transaction, intTx, true);
}

/**
 * Remove token amounts from existing balances
 */
export async function removeFromBalances(
  balances: ExtBalance[],
  event: Prisma.EventCreateInput,
  tx: Prisma.TransactionClient,
  transaction: Transaction,
  intTx: ITransaction,
): Promise<ExtBalance[]> {
  return alterBalances(balances, event, tx, transaction, intTx, false);
}

/**
 * Create new balances with the tokens and amounts defined
 */
export async function createBalances(
  tokens: Token[],
  event: Prisma.EventCreateInput,
  tx: Prisma.TransactionClient,
  transaction: Transaction,
  intTx: ITransaction,
): Promise<ExtBalance[]> {
  const balances: ExtBalance[] = [];
  for (const token of tokens) {
    const txAmount = intTx.content.tokens[token.name];
    balances.push({
      token,
      snapshot: { amount: txAmount },
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
      event_id, ${txAmount}, token_id, account_id
    FROM   ins_balance;`;
  }
  return balances;
}
