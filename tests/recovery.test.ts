import { expect, test } from "vite-plus/test";
import OpendataManager, { type Diagnostic } from "../src/index.ts";
import { collect, fixture, largeCsv } from "./helpers.ts";

test.each([false, true])(
  "interrupted transfer resumes without duplicates (ignored ranges: %s)",
  async (ignoreRanges) => {
    const f = await fixture(largeCsv());
    f.drops = 1;
    f.ignoreRanges = ignoreRanges;
    const manager = await OpendataManager.create({ ...f.options, retries: 1 });
    const data = await manager.toJSON();
    expect(data).toHaveLength(30000);
    expect(data[0]?.id).toBe("0");
    expect(data.at(-1)?.id).toBe("29999");
    expect(new Set(data.map((r) => r.id)).size).toBe(30000);
    expect(f.requests.some((r) => r.range?.startsWith("bytes=65536-"))).toBe(true);
  },
);

test("without validators, recovery redownloads and verifies prefix", async () => {
  const f = await fixture(largeCsv());
  delete f.etag;
  f.drops = 1;
  const manager = await OpendataManager.create({ ...f.options, retries: 1 });
  expect(await manager.toJSON()).toHaveLength(30000);
  expect(f.requests.filter((r) => r.path.endsWith("data.csv"))).toHaveLength(2);
  expect(f.requests.some((r) => r.range)).toBe(false);
});

test("restart uses a persisted import cursor after a transfer failure", async () => {
  const f = await fixture(largeCsv());
  f.drops = 1;
  const manager = await OpendataManager.create(f.options);
  let cursor: string | undefined;
  let committed = 0;
  try {
    for await (const batch of manager.batches({ batchRecords: 100 })) {
      cursor = batch.nextCursor;
      committed += batch.data.length;
    }
  } catch (error) {
    expect(error).toMatchObject({ code: "DOWNLOAD_INTERRUPTED" });
  }
  expect(committed).toBeGreaterThan(0);
  expect(cursor).toBeDefined();
  const restarted = await OpendataManager.create(f.options);
  const remaining = await collect(restarted.batches({ resumeFrom: cursor! }));
  expect(remaining.flatMap((b) => b.data)[0]?.id).toBe(String(committed));
  expect(committed + remaining.reduce((n, b) => n + b.data.length, 0)).toBe(30000);
});

test.each(["etag", "prefix", "range"])("unsafe recovery fails: %s", async (scenario) => {
  const f = await fixture(largeCsv());
  if (scenario === "prefix") delete f.etag;
  f.drops = 1;
  const manager = await OpendataManager.create(f.options);
  await expect(manager.toJSON()).rejects.toMatchObject({ code: "DOWNLOAD_INTERRUPTED" });
  if (scenario === "etag") f.etag = '"changed"';
  if (scenario === "prefix") f.data[10] = 88;
  if (scenario === "range") f.invalidRange = true;
  await expect(manager.toJSON()).rejects.toMatchObject({
    code: scenario === "range" ? "INVALID_RANGE" : "SOURCE_CHANGED",
  });
});

test("freshness reuses unchanged snapshots and notices same-URL ETag corrections", async () => {
  const f = await fixture();
  const manager = await OpendataManager.create(f.options);
  const original = await collect(manager.batches());
  await manager.refresh();
  expect((await collect(manager.batches()))[0]?.snapshot).toBe(original[0]?.snapshot);
  f.etag = '"v2"';
  f.data = Buffer.from("id,name\n004,updated\n");
  await manager.refresh();
  const updated = await collect(manager.batches());
  expect(updated[0]?.snapshot).not.toBe(original[0]?.snapshot);
  expect(updated[0]?.data).toEqual([{ id: "004", name: "updated" }]);
  const old = await collect(manager.batches({ resumeFrom: original[0]!.nextCursor }));
  expect(old).toEqual([]);
  const forced = await collect(manager.batches({ forceDownload: true }));
  expect(forced[0]?.snapshot).not.toBe(updated[0]?.snapshot);
});

test("modified date invalidates snapshots and HEAD fallback supports validators", async () => {
  const f = await fixture();
  const manager = await OpendataManager.create(f.options);
  const original = await collect(manager.batches());
  f.modified = "20260907";
  f.headUnsupported = true;
  await manager.refresh();
  const updated = await collect(manager.batches());
  expect(updated[0]?.snapshot).not.toBe(original[0]?.snapshot);
  expect(f.requests.some((r) => r.range === "bytes=0-0")).toBe(true);
});

test("TTL revalidates on use; outages fail unless stale cache is explicitly enabled", async () => {
  const f = await fixture();
  const diagnostics: Diagnostic[] = [];
  const manager = await OpendataManager.create({
    ...f.options,
    maxAgeMs: 1,
    onDiagnostic: (d) => diagnostics.push(d),
  });
  const data = await manager.toJSON();
  await new Promise((resolve) => setTimeout(resolve, 5));
  f.unavailable = true;
  await expect(manager.toJSON()).rejects.toMatchObject({ code: "HTTP_ERROR" });
  expect(await manager.toJSON({ allowStale: true })).toEqual(data);
  expect(diagnostics.some((d) => d.code === "STALE_CACHE")).toBe(true);
});

test("retention removes inactive snapshots and expired cursors fail explicitly", async () => {
  const f = await fixture();
  const manager = await OpendataManager.create({ ...f.options, retentionMs: 1 });
  const original = await collect(manager.batches());
  await new Promise((resolve) => setTimeout(resolve, 5));
  await manager.toJSON({ forceDownload: true });
  await expect(
    collect(manager.batches({ resumeFrom: original[0]!.nextCursor })),
  ).rejects.toMatchObject({ code: "CURSOR_EXPIRED" });
});

test("corrupt cached chunks are rejected", async () => {
  const f = await fixture();
  const manager = await OpendataManager.create(f.options);
  await manager.toJSON();
  const keys = await collect(f.options.cache!.keys("datasets"));
  const key = keys.find((k) => k.endsWith(".bin"))!;
  await f.options.cache!.write(key, Buffer.from("corrupted"));
  await expect(manager.toJSON()).rejects.toMatchObject({ code: "CACHE_CORRUPT" });
});
