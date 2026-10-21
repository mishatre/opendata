import { expect, test } from "vite-plus/test";
import type { CacheAdapter } from "../src/index.ts";
import { collect, tempCache } from "./helpers.ts";

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
/** Any adapter must atomically replace keys and never expose partial values. */
test("filesystem cache adapter contract", async () => {
  await contract(await tempCache());
});
