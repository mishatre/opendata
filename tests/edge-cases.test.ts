import { readFile } from "node:fs/promises";
import { expect, test } from "vite-plus/test";
import OpendataManager, { type DataRecord, type Diagnostic } from "../src/index.ts";
import { csvRows, detectDelimiter } from "../src/csv.ts";
import { collect, fixture, largeCsv } from "./helpers.ts";

test("committed four-column metadata and multiline fixtures work together", async () => {
  const f = await fixture(await readFile("tests/fixtures/data.csv", "utf8"));
  f.meta = await readFile("tests/fixtures/meta.csv", "utf8");
  f.structure = await readFile("tests/fixtures/structure.csv", "utf8");
  const manager = await OpendataManager.create({ ...f.options, csv: {} });
  const data = await manager.toJSON();
  expect(data).toEqual([
    { id: "001", name: "first\nsecond", code: "0001" },
    { id: "002", name: 'a "quote"', code: "-" },
    { id: "003", name: "", code: "  value" },
  ]);
  expect(manager.metadata.entries[0]?.attributes.title).toBe("Identifier");
});

test("metadata and structure encoding/dialect overrides are independent of data", async () => {
  const f = await fixture();
  f.meta =
    "property;value\nidentifier;test\ndata-20260906-structure-20210405;data.csv\nstructure-20210405;structure.csv\n";
  f.structure = "field name;format\nid;string\nname;string\n";
  const manager = await OpendataManager.create({
    ...f.options,
    metadataCsv: { delimiter: ";" },
    structureCsv: { delimiter: ";" },
  });
  expect(manager.structure.fields).toHaveLength(2);
  expect(await manager.toJSON()).toHaveLength(3);
});

test("ambiguous references fail while a uniquely latest unsorted reference is selected", async () => {
  const f = await fixture();
  f.meta =
    "property,value\ndata-20260904-structure-20210405,old.csv\ndata-20260906-structure-20210405,data.csv\ndata-20260905-structure-20210405,middle.csv\n";
  expect((await OpendataManager.create(f.options)).source.url).toMatch(/\/data.csv$/);
  f.meta += "data-20260906-structure-20210405,conflict.csv\n";
  await expect(
    OpendataManager.create({ ...f.options, csv: { delimiter: ",", quote: false } }),
  ).rejects.toMatchObject({ code: "AMBIGUOUS_SOURCE" });
});

test("missing or invalid structures are advisory and can be ignored", async () => {
  const f = await fixture();
  f.structure = "not a structure";
  const warnings: Diagnostic[] = [];
  const manager = await OpendataManager.create({
    ...f.options,
    onDiagnostic: (d) => warnings.push(d),
  });
  expect(await manager.toJSON()).toHaveLength(3);
  expect(warnings.some((d) => d.code === "STRUCTURE_UNAVAILABLE")).toBe(true);
  f.requests.length = 0;
  const ignored = await OpendataManager.create({ ...f.options, structure: "ignore" });
  await ignored.toJSON();
  expect(f.requests.some((r) => r.path.endsWith("structure.csv"))).toBe(false);
});

test("publisher HTML responses fail even when an explicit delimiter is supplied", async () => {
  const f = await fixture("<!DOCTYPE html><html><body>Maintenance</body></html>");
  await expect((await OpendataManager.create(f.options)).toJSON()).rejects.toMatchObject({
    code: "UNEXPECTED_CONTENT",
  });
});

test("a downloaded malformed file is cached as raw-complete but not parse-validated", async () => {
  const f = await fixture("id,name\n1,too,many\n");
  const manager = await OpendataManager.create(f.options);
  await expect(manager.toJSON()).rejects.toMatchObject({ code: "INVALID_RECORD_WIDTH" });
  const keys = await collect(f.options.cache!.keys("datasets"));
  const manifest = JSON.parse(
    Buffer.from(
      (await f.options.cache!.read(keys.find((k) => k.endsWith("manifest.json"))!))!,
    ).toString(),
  );
  expect(manifest.complete).toBe(true);
  expect(manifest.validatedProfiles).toEqual([]);
});

test("decoder and quoted CSV survive every single-byte boundary", async () => {
  const text = '\ufeffid,name\r\n01,"Привет\r\n世界"\r\n02,"a ""quote"""\r\n';
  const bytes = Buffer.from(text);
  const source = async function* () {
    for (const byte of bytes) yield Uint8Array.of(byte);
  };
  const rows = await collect(csvRows(source(), {}, new AbortController().signal));
  expect(rows.map((r) => r.cells)).toEqual([
    ["id", "name"],
    ["01", "Привет\r\n世界"],
    ["02", 'a "quote"'],
  ]);
  expect(rows.map((r) => r.line)).toEqual([1, 3, 4]);
});

test("ambiguous delimiters need an explicit override", () => {
  expect(() => detectDelimiter("a,b;c\n1,2;3\n", {}, true)).toThrow();
  expect(detectDelimiter("a,b;c\n1,2;3\n", { delimiter: ";" }, true)).toBe(";");
});

test.each([NaN, undefined, new Date(), BigInt(1)])(
  "non-JSON hook values are rejected: %s",
  async (value) => {
    const f = await fixture();
    const manager = await OpendataManager.create({
      ...f.options,
      hooks: {
        record: { id: "bad-json", version: "1", run: () => ({ value }) as unknown as DataRecord },
      },
    });
    await expect(manager.toJSON()).rejects.toMatchObject({ code: "INVALID_TRANSFORM" });
  },
);

test("slow consumers do not cause unbounded download-ahead", async () => {
  const f = await fixture(largeCsv(100000));
  let durable = 0;
  const manager = await OpendataManager.create({
    ...f.options,
    onProgress: (p) => {
      if (p.phase === "download") durable = p.bytes;
    },
  });
  const iterator = manager.batches({ batchRecords: 1 });
  await iterator.next();
  await new Promise((resolve) => setTimeout(resolve, 100));
  // The parser/input high-water marks allow bounded prefetch, not the entire response.
  expect(durable).toBeLessThan(256 * 1024);
  await iterator.return(undefined);
});

test("failed replacement preserves the previous completed snapshot", async () => {
  const f = await fixture();
  const warnings: Diagnostic[] = [];
  const manager = await OpendataManager.create({
    ...f.options,
    maxAgeMs: 1,
    onDiagnostic: (d) => warnings.push(d),
  });
  const original = await manager.toJSON();
  f.modified = "20260907";
  f.etag = '"v2"';
  f.data = Buffer.from("id,name\n1,too,many\n");
  await manager.refresh();
  await expect(manager.toJSON()).rejects.toMatchObject({ code: "INVALID_RECORD_WIDTH" });
  f.unavailable = true;
  await new Promise((resolve) => setTimeout(resolve, 5));
  expect(await manager.toJSON({ allowStale: true })).toEqual(original);
});
