import {
	DeleteObjectCommand,
	GetObjectCommand,
	ListObjectsV2Command,
	PutObjectCommand,
	type S3Client,
} from "@aws-sdk/client-s3";
import type { CacheAdapter } from "./cache.ts";
import { validateKey } from "./cache.ts";

export interface S3CacheOptions {
	client: S3Client;
	bucket: string;
	prefix?: string;
}

/** Separate entry point: importing the core library never loads the AWS SDK. */
export class S3Cache implements CacheAdapter {
	private readonly client: S3Client;
	private readonly bucket: string;
	private readonly prefix: string;

	constructor(options: S3CacheOptions) {
		this.client = options.client;
		this.bucket = options.bucket;
		this.prefix = (options.prefix ?? "opendata").replace(/^\/+|\/+$/g, "");
		if (this.prefix) {
			validateKey(this.prefix);
		}
	}

	private key(key: string): string {
		validateKey(key);
		return this.prefix ? `${this.prefix}/${key}` : key;
	}

	public async read(key: string): Promise<Uint8Array | undefined> {
		try {
			const response = await this.client.send(
				new GetObjectCommand({ Bucket: this.bucket, Key: this.key(key) }),
			);
			return await response.Body?.transformToByteArray();
		} catch (error) {
			if (
				(error as { $metadata?: { httpStatusCode?: number } }).$metadata
					?.httpStatusCode === 404
			) {
				return undefined;
			}
			throw error;
		}
	}

	public async write(key: string, data: Uint8Array): Promise<void> {
		await this.client.send(
			new PutObjectCommand({
				Bucket: this.bucket,
				Key: this.key(key),
				Body: data,
				ContentType: key.endsWith(".json")
					? "application/json"
					: "application/octet-stream",
			}),
		);
	}

	public async delete(key: string): Promise<void> {
		await this.client.send(
			new DeleteObjectCommand({ Bucket: this.bucket, Key: this.key(key) }),
		);
	}

	public async *keys(prefix: string): AsyncGenerator<string> {
		let token: string | undefined;
		do {
			const response = await this.client.send(
				new ListObjectsV2Command({
					Bucket: this.bucket,
					Prefix: `${this.key(prefix)}/`,
					...(token ? { ContinuationToken: token } : {}),
				}),
			);
			for (const item of response.Contents ?? []) {
				if (item.Key) {
					yield this.prefix ? item.Key.slice(this.prefix.length + 1) : item.Key;
				}
			}
			token = response.IsTruncated ? response.NextContinuationToken : undefined;
		} while (token);
	}
}
