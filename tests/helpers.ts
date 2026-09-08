import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach } from "vite-plus/test";
import { FileCache, type ManagerOptions } from "../src/index.ts";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
export async function tempCache(): Promise<FileCache> {
  const directory = await mkdtemp(join(tmpdir(), "opendata-test-"));
  cleanups.push(() => rm(directory, { recursive: true, force: true }));
  return new FileCache({ directory });
}
export async function server(
  handler: (request: IncomingMessage, response: ServerResponse) => void | Promise<void>,
): Promise<string> {
  const instance = createServer((req, res) => {
    void Promise.resolve(handler(req, res)).catch((error) => {
      res.destroy(error as Error);
    });
  });
  instance.listen(0, "127.0.0.1");
  await once(instance, "listening");
  cleanups.push(async () => {
    instance.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      instance.close((error) => (error ? reject(error) : resolve())),
    );
  });
  const address = instance.address();
  if (!address || typeof address === "string") throw new Error("No listener address");
  return `http://127.0.0.1:${address.port}/`;
}
export interface Fixture {
  data: Buffer;
  etag?: string;
  modified: string;
  structure: string;
  meta?: string;
  unavailable: boolean;
  ignoreRanges: boolean;
  invalidRange: boolean;
  headUnsupported: boolean;
  drops: number;
  dropAfter: number;
  interrupt?: () => void;
  requests: { method: string; path: string; range?: string }[];
  options: ManagerOptions;
}
export async function fixture(
  data = 'id,name\r\n001,"first\nsecond"\r\n002,"a ""quote"""\r\n003,-\r\n',
): Promise<Fixture> {
  const f: Fixture = {
    data: Buffer.from(data),
    etag: '"v1"',
    modified: "20260906",
    structure: '"field name",format\nid,string\n',
    unavailable: false,
    ignoreRanges: false,
    invalidRange: false,
    headUnsupported: false,
    drops: 0,
    dropAfter: 65536,
    requests: [],
    options: {
      baseUrl: "",
      identifier: "test",
      cache: await tempCache(),
      chunkBytes: 8192,
      csv: { delimiter: "," },
      retryDelayMs: 1,
      retries: 0,
    },
  };
  f.options.onProgress = (progress) => {
    if (progress.phase === "download" && progress.bytes >= f.dropAfter) {
      f.interrupt?.();
      delete f.interrupt;
    }
  };
  f.options.baseUrl = await server((req, res) => {
    const path = req.url ?? "/";
    f.requests.push({
      method: req.method ?? "GET",
      path,
      ...(req.headers.range ? { range: req.headers.range } : {}),
    });
    if (f.unavailable) {
      res.writeHead(503).end();
      return;
    }
    if (path.endsWith("/meta.csv")) {
      res.end(
        f.meta ??
          `property,value\nidentifier,test\nmodified,${f.modified}\nprovenance,data-${f.modified}T0000-structure-20210405T0000\ndata-${f.modified}-structure-20210405,data.csv\nstructure-20210405,structure.csv\nstructure-20210405,structure.csv\n`,
      );
      return;
    }
    if (path.endsWith("/structure.csv")) {
      res.end(f.structure);
      return;
    }
    if (!path.endsWith("/data.csv")) {
      res.writeHead(404).end();
      return;
    }
    if (f.etag) res.setHeader("ETag", f.etag);
    if (req.method === "HEAD" && f.headUnsupported) {
      res.writeHead(405).end();
      return;
    }
    if (f.etag && req.headers["if-none-match"] === f.etag) {
      res.writeHead(304).end();
      return;
    }
    if (req.method === "HEAD") {
      res.setHeader("Content-Length", f.data.length);
      res.end();
      return;
    }
    let start = 0;
    let end = f.data.length - 1;
    if (req.headers.range && !f.ignoreRanges) {
      const match = /^bytes=(\d+)-(\d*)$/.exec(req.headers.range);
      start = Number(match?.[1]);
      if (match?.[2]) end = Number(match[2]);
      if (start >= f.data.length) {
        res.writeHead(416, { "Content-Range": `bytes */${f.data.length}` }).end();
        return;
      }
      res.statusCode = 206;
      res.setHeader(
        "Content-Range",
        `bytes ${f.invalidRange ? start + 1 : start}-${end}/${f.data.length}`,
      );
    }
    const bytes = f.data.subarray(start, end + 1);
    res.setHeader("Content-Length", bytes.length);
    if (f.drops > 0) {
      f.drops--;
      f.interrupt = () => res.destroy();
      res.write(bytes.subarray(0, f.dropAfter));
    } else res.end(bytes);
  });
  return f;
}
export function largeCsv(rows = 30000): string {
  return "id,name\n" + Array.from({ length: rows }, (_, i) => `${i},value-${i}\n`).join("");
}
export async function collect<T>(source: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const item of source) values.push(item);
  return values;
}
