import { readFile } from "node:fs/promises";
import { expect, test } from "vite-plus/test";
import OpendataManager, { type Diagnostic, type ProviderProfile } from "../src/index.ts";
import { collect, fixture, largeCsv } from "./helpers.ts";

test("initialization discovers metadata only, deduplicates references and treats structure as advisory", async () => {
  const f = await fixture();
  const diagnostics: Diagnostic[] = [];
  const manager = await OpendataManager.create({
    ...f.options,
    onDiagnostic: (d) => diagnostics.push(d),
  });
  expect(f.requests.some((r) => r.path.endsWith("/data.csv"))).toBe(false);
  expect(manager.metadata.entries.filter((e) => e.property.startsWith("structure-")).length).toBe(
    2,
  );
  expect(manager.source.key).toBe("data-20260906-structure-20210405");
  expect(Object.isFrozen(manager.metadata)).toBe(true);
  const batches = await collect(manager.batches({ batchRecords: 2 }));
  expect(batches.map((b) => b.data.length)).toEqual([2, 1]);
  expect(batches[0]?.data[0]).toEqual({ id: "001", name: "first\nsecond" });
  expect(batches[0]?.data[1]).toEqual({ id: "002", name: 'a "quote"' });
  expect(diagnostics.some((d) => d.code === "STRUCTURE_MISMATCH")).toBe(true);
});

test("JSONL bulk output, JSON stream and array export agree and cache is reused", async () => {
  const f = await fixture();
  const manager = await OpendataManager.create(f.options);
  const objects = await manager.toJSON();
  const chunks = await collect(manager.batches({ format: "jsonl", batchRecords: 2 }));
  expect(chunks[0]?.data.split("\n").length).toBe(3);
  expect(
    chunks.flatMap((c) =>
      c.data
        .trimEnd()
        .split("\n")
        .map((line) => JSON.parse(line)),
    ),
  ).toEqual(objects);
  const text = Buffer.concat(
    (await collect(manager.stream({ format: "json" }))) as Buffer[],
  ).toString();
  expect(JSON.parse(text)).toEqual(objects);
  expect(f.requests.filter((r) => r.method === "GET" && r.path.endsWith("/data.csv"))).toHaveLength(
    1,
  );
  await expect(manager.toJSON({ maxRecords: 2 })).rejects.toMatchObject({ code: "EXPORT_LIMIT" });
});

test("full JSON is valid for empty data and escapes newlines inside values", async () => {
  const f = await fixture("id,name\n");
  const manager = await OpendataManager.create(f.options);
  expect(await manager.toJSON()).toEqual([]);
  expect(
    JSON.parse(
      Buffer.concat((await collect(manager.stream({ format: "json" }))) as Buffer[]).toString(),
    ),
  ).toEqual([]);
});

test("cursor resumes after committed batches, including multiline and filtered records", async () => {
  const f = await fixture();
  const options = {
    ...f.options,
    hooks: {
      record: {
        id: "filter",
        version: "1",
        run: (record: Readonly<Record<string, string>>) =>
          record.id === "002" ? null : { ...record },
      },
    },
  };
  const manager = await OpendataManager.create(options);
  const initial = await collect(manager.batches({ batchRecords: 2 }));
  expect(initial[0]?.endRecord).toBe(2);
  expect(initial[0]?.data).toHaveLength(1);
  const restarted = await OpendataManager.create(options);
  const remaining = await collect(restarted.batches({ resumeFrom: initial[0]!.nextCursor }));
  expect(remaining.flatMap((b) => b.data)).toEqual([{ id: "003", name: "-" }]);
  expect(await collect(restarted.batches({ resumeFrom: initial.at(-1)!.nextCursor }))).toEqual([]);
  const changed = await OpendataManager.create({
    ...options,
    hooks: { record: { ...options.hooks.record, version: "2" } },
  });
  await expect(
    collect(changed.batches({ resumeFrom: initial[0]!.nextCursor })),
  ).rejects.toMatchObject({ code: "INCOMPATIBLE_CURSOR" });
});

test("all-filtered batches still expose progress cursors", async () => {
  const f = await fixture();
  const manager = await OpendataManager.create({
    ...f.options,
    hooks: { record: { id: "filter-all", version: "1", run: () => null } },
  });
  const result = await collect(manager.batches({ batchRecords: 2 }));
  expect(result.map((b) => [b.data, b.endRecord])).toEqual([
    [[], 2],
    [[], 3],
  ]);
});

test("profiles apply provider, dataset, then explicit settings and reject ambiguous matches", async () => {
  const f = await fixture("wrong,wrong\n001,value\n");
  const provider: ProviderProfile = {
    id: "provider",
    version: "1",
    origin: f.options.baseUrl,
    csv: { headers: ["id", "provider"] },
  };
  const dataset: ProviderProfile = {
    id: "dataset",
    version: "1",
    origin: f.options.baseUrl,
    identifier: "test",
    csv: { headers: ["id", "dataset"] },
  };
  const manager = await OpendataManager.create({
    ...f.options,
    profiles: [dataset, provider],
    csv: { delimiter: ",", headers: ["id", "explicit"] },
  });
  expect(await manager.toJSON()).toEqual([{ id: "001", explicit: "value" }]);
  await expect(
    OpendataManager.create({
      ...f.options,
      profiles: [provider, { ...provider, id: "duplicate" }],
    }),
  ).rejects.toMatchObject({ code: "AMBIGUOUS_PROFILE" });
});

test("metadata repair and selection hooks resolve conflicts; raw repair is replayed from original cache", async () => {
  const f = await fixture("id,name\n001,broken\n");
  f.meta =
    "property,value\nidentifier,wrong\ndata-20260906-structure-20210405,data.csv\ndata-20260906-structure-20210405,other.csv\n";
  const manager = await OpendataManager.create({
    ...f.options,
    hooks: {
      metadata: {
        id: "metadata",
        version: "1",
        run: (entries) =>
          entries.map((e) => (e.property === "identifier" ? { ...e, value: "test" } : e)),
      },
      selectSource: {
        id: "select",
        version: "1",
        run: (meta) => meta.sources.find((s) => s.url.endsWith("/data.csv"))!,
      },
      repairStream: {
        id: "repair",
        version: "1",
        run: async function* (bytes) {
          for await (const part of bytes)
            yield Buffer.from(Buffer.from(part).toString().replace("broken", "fixed"));
        },
      },
    },
  });
  expect(await manager.toJSON()).toEqual([{ id: "001", name: "fixed" }]);
  expect(manager.metadata.originalEntries[0]?.value).toBe("wrong");
  expect(manager.metadata.properties.identifier).toBe("test");
  const keys = await collect(f.options.cache!.keys("datasets"));
  const rawKey = keys.find((key) => key.endsWith(".bin"))!;
  expect(Buffer.from((await f.options.cache!.read(rawKey))!).toString()).toContain("broken");
});

test.each([
  ["id,name\n1,a,b\n", "INVALID_RECORD_WIDTH"],
  ["id,id\n1,a\n", "INVALID_HEADERS"],
  ["id,\n1,a\n", "INVALID_HEADERS"],
  ['id,name\n1,"unterminated\n', "PROCESSING_ERROR"],
  ["", "MISSING_HEADER"],
])("malformed CSV fails with diagnostic location: %s", async (data, code) => {
  const f = await fixture(data);
  const manager = await OpendataManager.create(f.options);
  await expect(manager.toJSON()).rejects.toMatchObject({
    code,
    diagnostic: {
      source: manager.source.url,
      record: expect.any(Number),
      line: expect.any(Number),
    },
  });
});

test("invalid decoding and oversized records fail", async () => {
  const f = await fixture();
  f.data = Buffer.from([0x69, 0x64, 0x2c, 0x6e, 0x0a, 0xff, 0x2c, 0x61]);
  const manager = await OpendataManager.create(f.options);
  await expect(manager.toJSON()).rejects.toMatchObject({ code: "INVALID_ENCODING" });
  f.data = Buffer.from("id,name\n1," + "a".repeat(100));
  const limited = await OpendataManager.create({
    ...f.options,
    csv: { delimiter: ",", maxRecordSize: 16 },
  });
  await expect(limited.toJSON({ forceDownload: true })).rejects.toThrow();
});

test("delimiter detection, UTF-16 BOM and explicit Windows-1251 decoding", async () => {
  const f = await fixture("id;name\n001;Привет\n");
  f.data = Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from(f.data.toString(), "utf16le")]);
  const manager = await OpendataManager.create({ ...f.options, csv: {} });
  expect(await manager.toJSON()).toEqual([{ id: "001", name: "Привет" }]);
  f.data = Buffer.concat([
    Buffer.from("id\tname\n001\t"),
    Buffer.from([0xcf, 0xf0, 0xe8, 0xe2, 0xe5, 0xf2]),
    Buffer.from("\n"),
  ]);
  const windows = await OpendataManager.create({ ...f.options, csv: { encoding: "windows-1251" } });
  expect(await windows.toJSON({ forceDownload: true })).toEqual([{ id: "001", name: "Привет" }]);
});

test("headerless input and repaired header widths", async () => {
  const f = await fixture("001,value\n002,other\n");
  const manager = await OpendataManager.create({
    ...f.options,
    csv: { delimiter: ",", headers: ["id", "name"], hasHeader: false },
  });
  expect(await manager.toJSON()).toHaveLength(2);
  f.data = Buffer.from("wrong\n001,value\n");
  const repaired = await OpendataManager.create({
    ...f.options,
    csv: { delimiter: ",", headers: ["id", "name"] },
  });
  expect(await repaired.toJSON({ forceDownload: true })).toEqual([{ id: "001", name: "value" }]);
});

test("public operations reject overlap and cancellation retains reusable progress", async () => {
  const f = await fixture(largeCsv());
  const manager = await OpendataManager.create(f.options);
  const controller = new AbortController();
  const iterator = manager.batches({ batchRecords: 1, signal: controller.signal });
  const first = await iterator.next();
  await expect(manager.refresh()).rejects.toMatchObject({ code: "MANAGER_BUSY" });
  controller.abort(new Error("cancelled"));
  await expect(iterator.next()).rejects.toThrow("cancelled");
  const resumed = await collect(manager.batches({ resumeFrom: first.value!.nextCursor }));
  expect(resumed.flatMap((b) => b.data)).toHaveLength(29999);
});

test("supplied metadata selects the latest actual URL without hardcoding publisher schema", async () => {
  const f = await fixture();
  const metadata = await readFile("example/meta.csv", "utf8").catch(() => undefined);
  if (!metadata) return;
  f.meta = metadata.replaceAll("1000000012-ReestrProducts", "test");
  const manager = await OpendataManager.create({ ...f.options, structure: "ignore" });
  expect(manager.source.url).toMatch(/data-20260906-structure-20210405\.csv$/);
  expect(manager.metadata.sources).toHaveLength(470);
});
