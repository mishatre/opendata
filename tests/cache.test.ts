import { randomUUID } from "node:crypto";
import { S3Client } from "@aws-sdk/client-s3";
import { afterEach, expect, test } from "vite-plus/test";
import OpendataManager, { type CacheAdapter } from "../src/index.ts";
import { S3Cache } from "../src/s3.ts";
import { collect, fixture, largeCsv, server, tempCache } from "./helpers.ts";

const clients: S3Client[] = [];
afterEach(() => {
  for (const client of clients.splice(0)) client.destroy();
});
async function localS3(): Promise<S3Cache> {
  const objects = new Map<string, Buffer>();
  const endpoint = await server(async (req, res) => {
    const url = new URL(req.url!, "http://localhost");
    const key = decodeURIComponent(url.pathname).replace(/^\/bucket\//, "");
    if (url.searchParams.get("list-type") === "2") {
      const keys = [...objects.keys()]
        .filter((k) => k.startsWith(url.searchParams.get("prefix") ?? ""))
        .sort();
      const offset = Number(url.searchParams.get("continuation-token") ?? 0);
      const page = keys.slice(offset, offset + 2);
      const more = offset + page.length < keys.length;
      res.setHeader("Content-Type", "application/xml");
      res.end(
        `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>${more}</IsTruncated>${more ? `<NextContinuationToken>${offset + page.length}</NextContinuationToken>` : ""}${page.map((k) => `<Contents><Key>${k}</Key></Contents>`).join("")}</ListBucketResult>`,
      );
      return;
    }
    if (req.method === "PUT") {
      objects.set(key, Buffer.concat((await collect(req)) as Buffer[]));
      res.setHeader("ETag", '"test"');
      res.end();
    } else if (req.method === "DELETE") {
      objects.delete(key);
      res.writeHead(204).end();
    } else if (objects.has(key)) res.end(objects.get(key));
    else
      res
        .writeHead(404, { "Content-Type": "application/xml" })
        .end("<Error><Code>NoSuchKey</Code></Error>");
  });
  const client = new S3Client({
    endpoint,
    region: "us-east-1",
    forcePathStyle: true,
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
  });
  clients.push(client);
  return new S3Cache({ client, bucket: "bucket", prefix: randomUUID() });
}
async function contract(cache: CacheAdapter): Promise<void> {
  expect(await cache.read("contract/missing")).toBeUndefined();
  await cache.write("contract/one", Buffer.from("old"));
  await cache.write("contract/one", Buffer.from("new"));
  expect(Buffer.from((await cache.read("contract/one"))!).toString()).toBe("new");
  const bytes = Buffer.from([0, 255, 10, 128]);
  await cache.write("contract/nested/two", bytes);
  await cache.write("contract/three", bytes);
  expect(Buffer.from((await cache.read("contract/nested/two"))!)).toEqual(bytes);
  expect((await collect(cache.keys("contract"))).sort()).toEqual([
    "contract/nested/two",
    "contract/one",
    "contract/three",
  ]);
  await cache.delete("contract/one");
  await cache.delete("contract/one");
  expect(await cache.read("contract/one")).toBeUndefined();
  await expect(cache.write("../escape", bytes)).rejects.toThrow();
  for await (const key of cache.keys("contract")) await cache.delete(key);
}
test.each(["filesystem", "s3"])("cache adapter contract: %s", async (backend) => {
  await contract(backend === "s3" ? await localS3() : await tempCache());
});

test("S3 manager persists chunks, resumes interrupted download, and replays cursors", async () => {
  const f = await fixture(largeCsv(12000));
  f.drops = 1;
  const options = { ...f.options, cache: await localS3(), retries: 1 };
  const manager = await OpendataManager.create(options);
  const batches = await collect(manager.batches());
  expect(batches.reduce((n, b) => n + b.data.length, 0)).toBe(12000);
  const restarted = await OpendataManager.create(options);
  const rest = await collect(restarted.batches({ resumeFrom: batches[0]!.nextCursor }));
  expect(rest.flatMap((b) => b.data)[0]?.id).toBe("1000");
  expect(rest.reduce((n, b) => n + b.data.length, 0)).toBe(11000);
});

test.skipIf(!process.env.OPENDATA_S3_TEST_ENDPOINT || !process.env.OPENDATA_S3_TEST_BUCKET)(
  "external S3-compatible endpoint contract",
  async () => {
    const client = new S3Client({
      endpoint: process.env.OPENDATA_S3_TEST_ENDPOINT!,
      region: process.env.AWS_REGION ?? "us-east-1",
      forcePathStyle: true,
    });
    clients.push(client);
    const cache = new S3Cache({
      client,
      bucket: process.env.OPENDATA_S3_TEST_BUCKET!,
      prefix: `opendata-test-${randomUUID()}`,
    });
    try {
      await contract(cache);
    } finally {
      for await (const key of cache.keys("contract")) await cache.delete(key);
    }
  },
);
