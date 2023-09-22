import { Prisma, PrismaClient } from '@prisma/client';

export type ExtBalance = Prisma.BalanceGetPayload<{
  include: { token: true; snapshot: true };
}>;

export type InternalTransaction = {
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
