import { NotFoundError, QueueServiceError } from './errors.ts';
import { contentDigest, signRequest } from './signing.ts';

export const VERSION = '1.0.0';

/** The Queue Service endpoint. */
export const DEFAULT_ENDPOINT = 'https://queues.thailandhosting.com';
/** The most messages one send, receive or delete takes. */
export const MAX_BATCH = 10;
/** The longest a receive waits for a first message, in seconds. */
export const MAX_WAIT = 20;

const RETRY_STATUSES = new Set([502, 503, 504]);

export interface ClientOptions {
  /** A Project access key from the Console (Queue Service → Access keys). Requests are signed with
   *  the secret (HTTP Message Signatures, RFC 9421); the secret itself is never sent. */
  accessKeyId: string;
  secretAccessKey: string;
  /** The queues' region, e.g. `th-bkk-1`, appended to the endpoint. Without one the service finds
   *  each queue's region itself. */
  region?: string;
  /** Default https://queues.thailandhosting.com; it may already end in a region. */
  endpoint?: string;
  /** Retries after the service was busy or unreachable (default 4; 0 disables). */
  maxRetries?: number;
  /** Appended to the SDK's User-Agent. */
  userAgent?: string;
  /** A fetch implementation (default: the global fetch). */
  fetch?: typeof fetch;
}

/** A queue's settings in whole seconds; left-out fields keep their value (or the default on a new queue). */
export interface QueueSettings {
  /** How long a received message stays hidden before it is handed out again (0–43200, default 30). */
  visibilityTimeout?: number;
  /** How long a message is kept (60–1209600, default 345600: 4 days). */
  retention?: number;
  /** Hides every new message for this long (0–900, default 0). */
  delay?: number;
  /** Moves a message to `deadLetterQueue` (or drops it) after this many receives (0 never, up to 1000). */
  maxReceives?: number;
  /** A queue of the same Project ('' for none). */
  deadLetterQueue?: string;
}

export interface QueueInfo {
  name: string;
  region: string;
  createdAt: Date;
  visibilityTimeout: number;
  retention: number;
  delay: number;
  maxReceives: number;
  deadLetterQueue?: string;
  /** The backlog (getQueue and Queue.info only). */
  messages?: QueueStats;
}

export interface QueueStats {
  /** Ready to be received. */
  visible: number;
  /** Received, not yet deleted. */
  inFlight: number;
  /** Sent with a delay still running. */
  delayed: number;
  bytes: number;
  /** Age of the oldest message, in seconds. */
  oldestAge: number;
}

export interface SendOptions {
  /** Travel with the message (string keys and values, up to 10). */
  attributes?: Record<string, string>;
  /** Hides the message for this many seconds first (at most 900; default: the queue's delay). */
  delay?: number;
  signal?: AbortSignal;
}

export interface OutgoingMessage {
  body: string;
  attributes?: Record<string, string>;
  delay?: number;
}

export interface ReceiveOptions {
  /** How many messages to take, 1 to 10 (default 1). */
  max?: number;
  /** Hide the received messages this many seconds instead of the queue's timeout (at least 1). */
  visibilityTimeout?: number;
  /** Long-poll up to this many seconds (at most 20) for a first message; 0 answers at once. */
  wait?: number;
  signal?: AbortSignal;
}

/** A received message. Delete it with its receipt once handled. */
export interface Message {
  id: string;
  receipt: string;
  body: string;
  attributes: Record<string, string>;
  sentAt: Date;
  /** How many times the message was handed out, this one included. */
  receives: number;
  firstReceivedAt: Date;
  /** The body parsed as JSON. */
  json<T = unknown>(): T;
}

export interface DeleteResult {
  receipt: string;
  deleted: boolean;
  /** `invalid_receipt` or `unavailable` when not deleted. */
  error?: string;
}

export interface ProcessOptions {
  /** Messages handled at once (default 1). */
  concurrency?: number;
  /** Seconds each receive long-polls (default and at most 20). */
  wait?: number;
  /** Hide received messages this many seconds instead of the queue's timeout; make it longer than a handler takes. */
  visibilityTimeout?: number;
  /** Seconds a message whose handler failed stays hidden before it is handed out again
   *  (default: until its visibility timeout ends). */
  retryDelay?: number;
  /** Hears about handler failures, and errors of the service the worker rode out (message undefined). */
  onError?: (err: unknown, message?: Message) => void;
  /** Stops the worker; it resolves once the running handlers finish. */
  signal?: AbortSignal;
}

const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    if (signal?.aborted) return resolve();
    const t = setTimeout(done, ms);
    function done() {
      clearTimeout(t);
      signal?.removeEventListener('abort', done);
      resolve();
    }
    signal?.addEventListener('abort', done, { once: true });
  });

/** The pause before retry n: exponential from 100 ms, capped at 5 s, with jitter. */
const backoff = (n: number) => {
  const d = Math.min(100 * 2 ** Math.min(n, 6), 5000);
  return d / 2 + Math.random() * (d / 2);
};

const toDate = (v: unknown) => (typeof v === 'string' ? new Date(v) : new Date(0));

function queueFromJSON(d: any): QueueInfo {
  return { ...d, createdAt: toDate(d.createdAt) };
}

function messageFromJSON(d: any): Message {
  return {
    id: d.id, receipt: d.receipt, body: d.body ?? '', attributes: d.attributes ?? {}, sentAt: toDate(d.sentAt),
    receives: d.receives ?? 0, firstReceivedAt: toDate(d.firstReceivedAt),
    json<T>() { return JSON.parse(this.body) as T; },
  };
}

/** Talks to Queue Service with one access key. Reuse it: it keeps connections open. */
export class Client {
  readonly baseUrl: string; // up to and including /v1
  private readonly keyId: string;
  private readonly secret: string;
  private readonly maxRetries: number;
  private readonly userAgent: string;
  private readonly fetch: typeof fetch;

  constructor(o: ClientOptions) {
    if (!o?.accessKeyId || !o?.secretAccessKey) throw new Error('queue-service: accessKeyId and secretAccessKey are required');
    const u = new URL((o.endpoint || DEFAULT_ENDPOINT).replace(/\/+$/, ''));
    if (u.protocol !== 'https:' && u.protocol !== 'http:') throw new Error(`queue-service: invalid endpoint ${o.endpoint}`);
    let path = u.pathname.replace(/\/+$/, '').replace(/\/v1$/, '');
    if (o.region) path += '/' + encodeURIComponent(o.region);
    this.baseUrl = `${u.origin}${path}/v1`;
    this.keyId = o.accessKeyId;
    this.secret = o.secretAccessKey;
    this.maxRetries = o.maxRetries ?? 4;
    this.userAgent = `thailandhosting-queue-service-sdk-node/${VERSION}${o.userAgent ? ' ' + o.userAgent : ''}`;
    this.fetch = o.fetch ?? globalThis.fetch.bind(globalThis);
  }

  /** @internal One call: JSON in, JSON (or nothing) out. `path` is after /v1, already escaped. */
  async call<T = any>(method: string, path: string, payload?: unknown, signal?: AbortSignal): Promise<{ status: number; data: T }> {
    const url = this.baseUrl + path;
    const body = payload === undefined ? undefined : JSON.stringify(payload);
    for (let attempt = 0; ; attempt++) {
      const headers: Record<string, string> = { 'User-Agent': this.userAgent, Accept: 'application/json' };
      if (body !== undefined) {
        headers['Content-Type'] = 'application/json';
        headers['Content-Digest'] = contentDigest(body);
      }
      Object.assign(headers, signRequest({ method, url, accessKeyId: this.keyId, secretAccessKey: this.secret, contentDigest: headers['Content-Digest'] }));
      let resp: Response;
      try {
        resp = await this.fetch(url, { method, headers, body, signal, redirect: 'manual' });
      } catch (err) {
        if (signal?.aborted || attempt >= this.maxRetries) throw err;
        await sleep(backoff(attempt), signal);
        continue;
      }
      if (resp.status >= 300) {
        const text = await resp.text().catch(() => '');
        if (RETRY_STATUSES.has(resp.status) && attempt < this.maxRetries) {
          const after = Number(resp.headers.get('retry-after'));
          await sleep(Number.isFinite(after) && after > 0 ? Math.min(after, 60) * 1000 : backoff(attempt), signal);
          if (!signal?.aborted) continue;
        }
        let p: any = {};
        try { p = JSON.parse(text); } catch { /* not Problem Details */ }
        const code = p.code || resp.headers.get('x-error-code') || `http_${resp.status}`;
        const message = p.title || text.trim() || resp.statusText;
        const problem = { type: p.type, instance: p.instance };
        if (resp.status === 404) throw new NotFoundError(code, message, problem);
        throw new QueueServiceError(resp.status, code, message, problem);
      }
      const text = await resp.text();
      return { status: resp.status, data: (text ? JSON.parse(text) : undefined) as T };
    }
  }

  /** The queues the key can reach, by name. */
  async listQueues(opts: { signal?: AbortSignal } = {}): Promise<QueueInfo[]> {
    const { data } = await this.call('GET', '/queues', undefined, opts.signal);
    return (data?.queues ?? []).map(queueFromJSON);
  }

  /** Creates a queue, or changes the given settings of the one by that name (the key must manage queues). */
  async putQueue(name: string, settings: QueueSettings = {}, opts: { signal?: AbortSignal } = {}): Promise<QueueInfo & { created: boolean }> {
    const { status, data } = await this.call('PUT', '/queues/' + encodeURIComponent(name), settings, opts.signal);
    return { ...queueFromJSON(data), created: status === 201 };
  }

  /** A queue's settings and backlog. */
  async getQueue(name: string, opts: { signal?: AbortSignal } = {}): Promise<QueueInfo> {
    const { data } = await this.call('GET', '/queues/' + encodeURIComponent(name), undefined, opts.signal);
    return queueFromJSON(data);
  }

  /** Deletes a queue with every message in it. */
  async deleteQueue(name: string, opts: { signal?: AbortSignal } = {}): Promise<void> {
    await this.call('DELETE', '/queues/' + encodeURIComponent(name), undefined, opts.signal);
  }

  /** A handle for sending and receiving on one queue (it makes no call by itself). */
  queue(name: string): Queue {
    return new Queue(this, name);
  }
}

/** Sends and receives on one queue. */
export class Queue {
  private readonly path: string;
  private readonly client: Client;
  readonly name: string;

  constructor(client: Client, name: string) {
    this.client = client;
    this.name = name;
    this.path = '/queues/' + encodeURIComponent(name);
  }

  /** The queue's settings and backlog. */
  info(opts: { signal?: AbortSignal } = {}): Promise<QueueInfo> {
    return this.client.getQueue(this.name, opts);
  }

  /** Sends one message and returns its ID. */
  async send(body: string, opts: SendOptions = {}): Promise<string> {
    const { data } = await this.client.call('POST', this.path + '/messages', { body, attributes: opts.attributes, delay: opts.delay || undefined }, opts.signal);
    return data.id;
  }

  /** Sends a value encoded as JSON. */
  sendJSON(value: unknown, opts: SendOptions = {}): Promise<string> {
    return this.send(JSON.stringify(value), opts);
  }

  /** Sends up to 10 messages in one request (all or none) and returns their IDs in order. */
  async sendBatch(messages: (OutgoingMessage | string)[], opts: { signal?: AbortSignal } = {}): Promise<string[]> {
    if (messages.length < 1 || messages.length > MAX_BATCH) throw new RangeError(`queue-service: send 1 to ${MAX_BATCH} messages at once`);
    const payload = messages.map((m) => (typeof m === 'string' ? { body: m } : { body: m.body, attributes: m.attributes, delay: m.delay || undefined }));
    const { data } = await this.client.call('POST', this.path + '/messages', { messages: payload }, opts.signal);
    return data.ids;
  }

  /** Takes up to `max` messages, hiding them for the visibility timeout; none when the queue is empty
   *  (after waiting up to `wait` seconds). */
  async receive(opts: ReceiveOptions = {}): Promise<Message[]> {
    const payload: Record<string, number> = {};
    if (opts.max) payload.max = opts.max;
    if (opts.visibilityTimeout) payload.visibilityTimeout = Math.max(1, Math.floor(opts.visibilityTimeout));
    if (opts.wait) payload.wait = Math.min(MAX_WAIT, Math.floor(opts.wait));
    const { data } = await this.client.call('POST', this.path + '/receive', payload, opts.signal);
    return (data?.messages ?? []).map(messageFromJSON);
  }

  /** Deletes a handled message by its receipt (deleting it again is not an error). */
  async delete(receipt: string, opts: { signal?: AbortSignal } = {}): Promise<void> {
    await this.client.call('DELETE', this.path + '/messages/' + encodeURIComponent(receipt), undefined, opts.signal);
  }

  /** Deletes up to 10 handled messages; each receipt gets a result, in the order given. */
  async deleteBatch(receipts: string[], opts: { signal?: AbortSignal } = {}): Promise<DeleteResult[]> {
    if (receipts.length < 1 || receipts.length > MAX_BATCH) throw new RangeError(`queue-service: delete 1 to ${MAX_BATCH} receipts at once`);
    const { data } = await this.client.call('POST', this.path + '/delete', { receipts }, opts.signal);
    const byReceipt = new Map<string, DeleteResult[]>();
    for (const r of (data?.results ?? []) as DeleteResult[]) byReceipt.set(r.receipt, [...(byReceipt.get(r.receipt) ?? []), r]);
    return receipts.map((receipt) => byReceipt.get(receipt)?.shift() ?? { receipt, deleted: false, error: 'unavailable' });
  }

  /** Hides a received message `timeout` seconds from now (to work on it longer), or with 0 hands it out again at once. */
  async changeVisibility(receipt: string, timeout: number, opts: { signal?: AbortSignal } = {}): Promise<void> {
    await this.client.call('POST', this.path + '/visibility', { receipt, timeout: Math.floor(timeout) }, opts.signal);
  }

  /** Removes every message of the queue. */
  async purge(opts: { signal?: AbortSignal } = {}): Promise<void> {
    await this.client.call('POST', this.path + '/purge', {}, opts.signal);
  }

  /**
   * A worker: receives with long polling and runs `handle` on each message, `concurrency` at a time,
   * until `signal` aborts (it then resolves once the running handlers finish). A message whose handler
   * resolves is deleted; one whose handler throws is left to be handed out again (after `retryDelay`
   * when set), and after the queue's maxReceives it goes to its dead-letter queue. Busy or unreachable
   * answers are retried with backoff; errors retrying cannot fix (a missing queue, a key that may not
   * receive) reject.
   */
  async process(handle: (message: Message) => unknown | Promise<unknown>, opts: ProcessOptions = {}): Promise<void> {
    const concurrency = Math.max(1, opts.concurrency ?? 1);
    const wait = Math.min(MAX_WAIT, opts.wait && opts.wait > 0 ? opts.wait : MAX_WAIT);
    const signal = opts.signal;
    const report = (err: unknown, m?: Message) => { try { opts.onError?.(err, m); } catch { /* ignore */ } };
    const running = new Set<Promise<void>>();
    let failures = 0;
    const runOne = async (m: Message) => {
      try {
        await handle(m);
      } catch (err) {
        report(err, m);
        if (opts.retryDelay && opts.retryDelay > 0) {
          await this.changeVisibility(m.receipt, opts.retryDelay).catch((e) => {
            if (!(e instanceof QueueServiceError && e.code === 'invalid_receipt')) report(e, m);
          });
        }
        return;
      }
      await this.delete(m.receipt).catch((e) => report(e, m));
    };
    try {
      while (!signal?.aborted) {
        while (running.size >= concurrency) await Promise.race(running);
        const free = Math.min(concurrency - running.size, MAX_BATCH);
        let messages: Message[];
        try {
          messages = await this.receive({ max: free, wait, visibilityTimeout: opts.visibilityTimeout, signal });
        } catch (err) {
          if (signal?.aborted) break;
          if (err instanceof QueueServiceError && [400, 401, 403, 404, 421].includes(err.status) && err.code !== 'project_suspended') throw err;
          report(err);
          await sleep(backoff(++failures), signal);
          continue;
        }
        failures = 0;
        for (const m of messages) {
          const p: Promise<void> = runOne(m).finally(() => running.delete(p));
          running.add(p);
        }
      }
    } finally {
      await Promise.allSettled(running);
    }
  }
}
