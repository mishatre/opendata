import { createReadStream } from "node:fs";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { pipeline } from "node:stream/promises";
import { expect, test } from "vite-plus/test";
import OpendataManager from "../src/index.ts";
import { server, tempCache } from "./helpers.ts";

test.skipIf(process.env.OPENDATA_FULL_SAMPLE !== "1")(
  "stream the full Minpromtorg sample with bounded memory",
  async () => {
    const path = "example/data-20260906-structure-20210405.csv";
    const size = (await stat(path)).size;
    const meta = await readFile("example/meta.csv", "utf8");
    const structure = await readFile("example/structure-20210405.csv");
    let baseUrl = "";
    let dataRequests = 0;
    baseUrl = await server(async (req, res) => {
      if (req.url?.endsWith("meta.csv"))
        res.end(meta.replaceAll("https://minpromtorg.gov.ru/", baseUrl));
      else if (req.url?.includes("/data-")) {
        dataRequests++;
        res.setHeader("Content-Length", size);
        res.setHeader("ETag", '"sample"');
        await pipeline(createReadStream(path), res);
      } else res.end(structure);
    });
    let downloaded = 0;
    let peakHeap = process.memoryUsage().heapUsed;
    let firstBatchBytes = 0;
    let count = 0;
    let multiline = 0;
    const manager = await OpendataManager.create({
      baseUrl,
      identifier: "1000000012-ReestrProducts",
      cache: await tempCache(),
      onProgress: (p) => {
        if (p.phase === "download") downloaded = p.bytes;
      },
    });
    for await (const batch of manager.batches()) {
      if (!count) firstBatchBytes = downloaded;
      for (const record of batch.data) {
        if (Object.keys(record).length !== 27) throw new Error("Unexpected field count");
        if (Object.values(record).some((v) => typeof v === "string" && /[\r\n]/.test(v)))
          multiline++;
      }
      count += batch.data.length;
      peakHeap = Math.max(peakHeap, process.memoryUsage().heapUsed);
    }
    expect(count).toBe(513856);
    expect(multiline).toBe(15571);
    expect(downloaded).toBe(size);
    expect(firstBatchBytes).toBeLessThan(size);
    expect(peakHeap).toBeLessThan(256 * 1024 * 1024);
    expect(dataRequests).toBe(1);
    await mkdir(".cache", { recursive: true });
    await writeFile(
      ".cache/full-sample-report.json",
      JSON.stringify(
        {
          records: count,
          multiline,
          bytes: downloaded,
          firstBatchBytes,
          peakHeapMiB: Math.round(peakHeap / 1024 / 1024),
        },
        null,
        2,
      ) + "\n",
    );
  },
  180_000,
);
