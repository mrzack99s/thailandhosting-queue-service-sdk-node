# ThailandHosting Queue Service — Node.js SDK

TypeScript/JavaScript client for ThailandHosting Queue Service: message
queues that take slow work off your site or app. One side sends jobs,
workers receive them, and nothing is lost when a worker fails.

```
npm install @thailandhosting/queue-service
```

Node.js 18.17+. ESM with TypeScript types. No runtime dependencies: it uses
Node's built-in `fetch`.

## Quick start

```ts
import { Client } from '@thailandhosting/queue-service';

const client = new Client({
  region: 'th-bkk-1',
  accessKeyId: process.env.TH_QUEUE_ACCESS_KEY_ID!,
  secretAccessKey: process.env.TH_QUEUE_SECRET_ACCESS_KEY!,
});
const jobs = client.queue('jobs');

// Send
await jobs.sendJSON({ orderId: 42 });
await jobs.send('plain text', { attributes: { kind: 'email' }, delay: 30 });
await jobs.sendBatch(['a', 'b', { body: 'c', attributes: { x: '1' } }]);

// A worker: long-polls, runs the handler, deletes what it handled.
const stop = new AbortController();
process.on('SIGTERM', () => stop.abort());
await jobs.process(async (m) => {
  const { orderId } = m.json<{ orderId: number }>();
  await ship(orderId); // throwing leaves the message to be retried
}, { concurrency: 8, retryDelay: 30, signal: stop.signal });
```

Create access keys in the Console (Queue Service → Access keys). Reuse one
`Client`: it keeps connections open.

## Receiving by hand

```ts
const messages = await jobs.receive({ max: 10, wait: 20 });
for (const m of messages) {
  try {
    await handle(m.body);
    await jobs.delete(m.receipt);
  } catch {
    // handed out again after the visibility timeout
  }
}
```

- `wait` long-polls up to 20 seconds for a first message, so an idle worker
  makes few requests.
- A received message is hidden for the queue's visibility timeout (or
  `visibilityTimeout`). Delete it with its receipt once handled; otherwise it
  is handed out again. `m.receives` counts the deliveries.
- `changeVisibility(receipt, seconds)` keeps a message hidden longer while
  you work on it, or with 0 releases it at once.
- `deleteBatch(receipts)` deletes up to 10 and reports each one in order.

Delivery is **at least once**: make handlers safe to run twice (check an
order ID before charging it, for example).

## Queues

```ts
const q = await client.putQueue('jobs', {
  visibilityTimeout: 120,
  maxReceives: 5,
  deadLetterQueue: 'jobs-dlq', // create it first
});
console.log(q.created);

await client.listQueues();
const info = await client.getQueue('jobs'); // info.messages: visible, inFlight, delayed, oldestAge
await jobs.purge();
await client.deleteQueue('jobs');
```

`putQueue` creates a queue or changes the given settings of an existing one
(the key must be allowed to manage queues). After `maxReceives` deliveries a
message moves to the dead-letter queue (or is dropped without one).

| Setting (seconds) | Default | Range |
| --- | --- | --- |
| `visibilityTimeout` | 30 | 0 – 43200 |
| `retention` | 345600 (4 days) | 60 – 1209600 |
| `delay` | 0 | 0 – 900 |
| `maxReceives` | 0 (never move) | 0 – 1000 |

Limits: a message is at most 256 KiB (body and up to 10 attributes), a batch
10 messages and 1 MiB.

## Errors

Error answers throw `QueueServiceError` (`NotFoundError` for 404) with the
service's `code`:

```ts
import { NotFoundError, QueueServiceError } from '@thailandhosting/queue-service';

try {
  await client.getQueue('missing');
} catch (e) {
  if (e instanceof NotFoundError) console.log(e.code); // queue_not_found
}
```

Common codes: `queue_not_found`, `forbidden`, `invalid_receipt`,
`queue_full`, `too_many_in_flight`, `too_large`, `project_suspended`,
`wrong_region`, `busy`. Busy or unreachable answers are retried with
exponential backoff (`maxRetries`, default 4). Every call takes an optional
`signal`.

## Endpoint and region

The default endpoint is `https://queues.thailandhosting.com`. Set `region`
to the queues' region; without one the service finds each queue's region.
`endpoint` may also be given with the region already in it:
`https://queues.thailandhosting.com/th-bkk-1`.

## Authentication

Every request is signed with HTTP Message Signatures (RFC 9421,
HMAC-SHA256 with the secret access key) over the method, host, path, query
and the body's Content-Digest (RFC 9530). The secret is never sent.
`signRequest` is exported for calling the API without the client.

## Development

```
npm test        # unit tests; set TH_QUEUE_TEST_ENDPOINT, TH_QUEUE_TEST_REGION,
                # TH_QUEUE_TEST_KEY and TH_QUEUE_TEST_SECRET for the round trip
npm run build
```

## License

MIT
