/*
  Warnings:

  - A unique constraint covering the columns `[name]` on the table `tokens` will be added. If there are existing duplicate values, this will fail.

*/
-- DropIndex
DROP INDEX "tokens_name_idx";

-- CreateIndex
CREATE UNIQUE INDEX "tokens_name_key" ON "tokens"("name");
