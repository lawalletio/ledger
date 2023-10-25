import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

const transactionTypes = [
  { description: 'internal-transaction' },
  { description: 'inbound-transaction' },
  { description: 'outbound-transaction' },
];

const tokens = [
  { name: 'BTC' },
];

async function main() {
  await prisma.token.createMany({ data: tokens });
  await prisma.transactionType.createMany({ data: transactionTypes });
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  })
;

