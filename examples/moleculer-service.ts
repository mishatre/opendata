import OpendataManager, { type DataRecord, type ManagerOptions } from "../src/index.ts";

/** Implement this interface using your SQL library. No database dependency is imposed. */
export interface ImportTransaction {
  upsertStagingRows(runId: string, snapshot: string, rows: DataRecord[]): Promise<void>;
  saveCheckpoint(runId: string, cursor: string): Promise<void>;
}
export interface ImportDatabase {
  /** Return an unfinished run, or create a new run without a cursor. */
  pendingOrNewRun(dataset: string): Promise<{ id: string; cursor?: string }>;
  transaction(work: (transaction: ImportTransaction) => Promise<void>): Promise<void>;
  /** Atomically publish staging rows and retire the run/checkpoint after successful EOF. */
  publishCompletedRun(runId: string): Promise<void>;
}
export interface ImportService {
  name: string;
  actions: { import: { handler(): Promise<{ imported: number }> } };
}

/** Pass the schema to broker.createService(...). Schedule one worker per dataset. */
export function createImportService(
  database: ImportDatabase,
  options: ManagerOptions,
): ImportService {
  return {
    name: "government-opendata-import",
    actions: {
      import: {
        async handler() {
          const dataset = new URL(options.baseUrl).href + "#" + options.identifier;
          const run = await database.pendingOrNewRun(dataset);
          const manager = await OpendataManager.create(options);
          let imported = 0;
          for await (const batch of manager.batches({
            ...(run.cursor ? { resumeFrom: run.cursor } : {}),
            batchRecords: 1000,
          })) {
            await database.transaction(async (transaction) => {
              await transaction.upsertStagingRows(run.id, batch.snapshot, batch.data);
              await transaction.saveCheckpoint(run.id, batch.nextCursor);
            });
            imported += batch.data.length;
          }
          await database.publishCompletedRun(run.id);
          return { imported };
        },
      },
    },
  };
}
