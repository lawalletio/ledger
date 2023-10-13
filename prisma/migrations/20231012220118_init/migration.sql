-- CreateTable
CREATE TABLE "tokens" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "name" TEXT NOT NULL,

    CONSTRAINT "tokens_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Transaction_types" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "description" TEXT NOT NULL,

    CONSTRAINT "Transaction_types_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "transactions" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "transaction_type_id" UUID NOT NULL,
    "payload" JSONB NOT NULL,
    "event_id" TEXT NOT NULL,

    CONSTRAINT "transactions_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "balance_snapshots" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "prev_id" UUID,
    "amount" BIGINT NOT NULL,
    "transaction_id" UUID NOT NULL,
    "event_id" TEXT NOT NULL,
    "delta" BIGINT NOT NULL,
    "token_id" UUID NOT NULL,
    "account_id" TEXT NOT NULL,

    CONSTRAINT "balance_snapshots_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "balances" (
    "account_id" TEXT NOT NULL,
    "token_id" UUID NOT NULL,
    "snapshot_id" UUID NOT NULL,
    "event_id" TEXT NOT NULL,

    CONSTRAINT "balances_pkey" PRIMARY KEY ("account_id","token_id")
);

-- CreateTable
CREATE TABLE "events" (
    "id" TEXT NOT NULL,
    "signature" TEXT NOT NULL,
    "author" TEXT NOT NULL,
    "signer" TEXT NOT NULL,
    "kind" INTEGER NOT NULL,
    "payload" JSONB NOT NULL,

    CONSTRAINT "events_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "tokens_name_idx" ON "tokens"("name");

-- CreateIndex
CREATE UNIQUE INDEX "transactions_event_id_key" ON "transactions"("event_id");

-- CreateIndex
CREATE UNIQUE INDEX "balance_snapshots_prev_id_key" ON "balance_snapshots"("prev_id");

-- CreateIndex
CREATE UNIQUE INDEX "balances_snapshot_id_key" ON "balances"("snapshot_id");

-- CreateIndex
CREATE UNIQUE INDEX "events_signature_key" ON "events"("signature");

-- AddForeignKey
ALTER TABLE "transactions" ADD CONSTRAINT "transactions_transaction_type_id_fkey" FOREIGN KEY ("transaction_type_id") REFERENCES "Transaction_types"("id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- AddForeignKey
ALTER TABLE "transactions" ADD CONSTRAINT "transactions_event_id_fkey" FOREIGN KEY ("event_id") REFERENCES "events"("id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- AddForeignKey
ALTER TABLE "balance_snapshots" ADD CONSTRAINT "balance_snapshots_prev_id_fkey" FOREIGN KEY ("prev_id") REFERENCES "balance_snapshots"("id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- AddForeignKey
ALTER TABLE "balance_snapshots" ADD CONSTRAINT "balance_snapshots_event_id_fkey" FOREIGN KEY ("event_id") REFERENCES "events"("id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- AddForeignKey
ALTER TABLE "balance_snapshots" ADD CONSTRAINT "balance_snapshots_token_id_account_id_fkey" FOREIGN KEY ("token_id", "account_id") REFERENCES "balances"("token_id", "account_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- AddForeignKey
ALTER TABLE "balance_snapshots" ADD CONSTRAINT "balance_snapshots_transaction_id_fkey" FOREIGN KEY ("transaction_id") REFERENCES "transactions"("id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- AddForeignKey
ALTER TABLE "balances" ADD CONSTRAINT "balances_token_id_fkey" FOREIGN KEY ("token_id") REFERENCES "tokens"("id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- AddForeignKey
ALTER TABLE "balances" ADD CONSTRAINT "balances_snapshot_id_fkey" FOREIGN KEY ("snapshot_id") REFERENCES "balance_snapshots"("id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- AddForeignKey
ALTER TABLE "balances" ADD CONSTRAINT "balances_event_id_fkey" FOREIGN KEY ("event_id") REFERENCES "events"("id") ON DELETE NO ACTION ON UPDATE NO ACTION;
