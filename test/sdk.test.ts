// Unit tests, and (when TH_QUEUE_TEST_ENDPOINT is set) a round trip against a real endpoint.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Client, NotFoundError, QueueServiceError, contentDigest, signRequest } from '../src/index.ts';

test('signature matches the service signer', () => {
  const body = '{"body":"hello"}';
  const digest = contentDigest(body);
  assert.equal(digest, 'sha-256=:HaY64dHGT0VJzs5YVVsj7yU6p7ssXKbEjBKRiGPP9Ro=:');
  const h = signRequest({ method: 'POST', url: 'https://queues.thailandhosting.com/th-bkk-1/v1/queues/jobs/messages?x=1', accessKeyId: 'AKID', secretAccessKey: 'secret', contentDigest: digest, created: 1767225600 });
  assert.equal(h['Signature-Input'], 'th=("@method" "@authority" "@path" "@query" "content-digest");created=1767225600;keyid="AKID";alg="hmac-sha256"');
  assert.equal(h.Signature, 'th=:eOsMVDCzdaff3N9m/aWz1ijBsJ9BgdiTtotBm4vfDdo=:');
  const g = signRequest({ method: 'GET', url: 'https://queues.thailandhosting.com/th-bkk-1/v1/queues', accessKeyId: 'AKID', secretAccessKey: 'secret', created: 1767225600 });
  assert.equal(g.Signature, 'th=:OjjuqbpOgI6K6AMMgIsPsySRmAEGuQPAwO/wXEyBfb4=:');
});

test('endpoint and region', () => {
  const k = { accessKeyId: 'k', secretAccessKey: 's' };
  assert.equal(new Client(k).baseUrl, 'https://queues.thailandhosting.com/v1');
  assert.equal(new Client({ ...k, region: 'th-bkk-1' }).baseUrl, 'https://queues.thailandhosting.com/th-bkk-1/v1');
  assert.equal(new Client({ ...k, endpoint: 'https://queues.thailandhosting.com/th-bkk-1/' }).baseUrl, 'https://queues.thailandhosting.com/th-bkk-1/v1');
  assert.equal(new Client({ ...k, endpoint: 'http://127.0.0.1:7811/v1', region: 'th-bkk-1' }).baseUrl, 'http://127.0.0.1:7811/th-bkk-1/v1');
  assert.throws(() => new Client({ accessKeyId: 'k' } as any));
});

test('errors and retries', async () => {
  let calls = 0;
  const srv = createServer((req, res) => {
    assert.match(String(req.headers['signature-input']), /^th=\(/);
    if (req.url === '/th-bkk-1/v1/queues/busy') {
      if (++calls < 3) {
        res.writeHead(503, { 'Retry-After': '0', 'Content-Type': 'application/problem+json' });
        return res.end('{"type":"tag:x","title":"Busy","status":503,"code":"busy"}');
      }
      return res.end('{"name":"busy","createdAt":"2026-10-14T00:00:00Z","messages":{"visible":3}}');
    }
    res.writeHead(404, { 'Content-Type': 'application/problem+json', 'X-Error-Code': 'queue_not_found' });
    res.end(JSON.stringify({ type: 'tag:x', title: 'The queue does not exist.', status: 404, code: 'queue_not_found', instance: req.url }));
  });
  await new Promise<void>((r) => srv.listen(0, '127.0.0.1', r));
  try {
    const c = new Client({ endpoint: `http://127.0.0.1:${(srv.address() as AddressInfo).port}`, region: 'th-bkk-1', accessKeyId: 'k', secretAccessKey: 's' });
    const info = await c.getQueue('busy');
    assert.equal(info.messages?.visible, 3);
    assert.equal(calls, 3);
    assert.ok(info.createdAt instanceof Date);
    await assert.rejects(c.getQueue('missing'), (e: unknown) => e instanceof NotFoundError && e.code === 'queue_not_found' && e.instance === '/th-bkk-1/v1/queues/missing');
  } finally {
    srv.close();
  }
});

const endpoint = process.env.TH_QUEUE_TEST_ENDPOINT;

test('round trip against a real endpoint', { skip: !endpoint && 'TH_QUEUE_TEST_ENDPOINT is not set' }, async () => {
  const c = new Client({ endpoint, region: process.env.TH_QUEUE_TEST_REGION, accessKeyId: process.env.TH_QUEUE_TEST_KEY!, secretAccessKey: process.env.TH_QUEUE_TEST_SECRET! });
  const name = `sdk-node-${Date.now()}`;
  const dlq = name + '-dlq';
  assert.equal((await c.putQueue(dlq)).created, true);
  const created = await c.putQueue(name, { visibilityTimeout: 30, maxReceives: 2, deadLetterQueue: dlq });
  assert.equal(created.created, true);
  assert.equal(created.maxReceives, 2);
  try {
    assert.equal((await c.putQueue(name, { visibilityTimeout: 20 })).created, false);
    assert.ok((await c.listQueues()).some((q) => q.name === name));

    const q = c.queue(name);
    assert.ok(await q.sendJSON({ order: 1, name: 'สวัสดี' }, { attributes: { kind: 'order' } }));
    assert.equal((await q.sendBatch(['a', 'b', { body: 'c', attributes: { x: '1' } }])).length, 3);
    assert.equal((await q.info()).messages?.visible, 4);
    const got = await q.receive({ max: 10, wait: 1 });
    assert.equal(got.length, 4);
    assert.deepEqual(got[0]!.json(), { order: 1, name: 'สวัสดี' });
    assert.equal(got[0]!.attributes.kind, 'order');
    assert.equal(got[0]!.receives, 1);
    await q.delete(got[0]!.receipt);
    await q.delete(got[0]!.receipt); // again: harmless
    const res = await q.deleteBatch([got[1]!.receipt, got[2]!.receipt, 'nonsense']);
    assert.deepEqual(res.map((r) => r.deleted), [true, true, false]);
    assert.equal(res[2]!.error, 'invalid_receipt');

    await q.changeVisibility(got[3]!.receipt, 0);
    const again = await q.receive({ wait: 2 });
    assert.equal(again.length, 1);
    assert.equal(again[0]!.receives, 2);
    await q.changeVisibility(again[0]!.receipt, 0);
    assert.equal((await q.receive({ wait: 1 })).length, 0, 'a message past maxReceives was handed out');
    const dead = await c.queue(dlq).receive({ wait: 2 });
    assert.equal(dead[0]?.body, 'c');

    for (let i = 0; i < 25; i++) await q.send(String(i));
    const seen = new Set<string>();
    let failed = false;
    const stop = new AbortController();
    const timer = setTimeout(() => stop.abort(), 30_000);
    await q.process(async (m) => {
      if (m.body === '7' && !failed) {
        failed = true;
        throw new Error('try again');
      }
      seen.add(m.body);
      if (seen.size === 25) stop.abort();
    }, { concurrency: 4, wait: 1, retryDelay: 1, signal: stop.signal });
    clearTimeout(timer);
    assert.equal(seen.size, 25);
    assert.ok(failed);
    const left = (await q.info()).messages!;
    assert.equal(left.visible + left.inFlight, 0);

    await q.send('x');
    await q.purge();
    await assert.rejects(c.getQueue('no-such-queue-' + name), NotFoundError);
    await assert.rejects(c.queue('no-such-queue-' + name).process(() => {}), (e: unknown) => e instanceof QueueServiceError && e.code === 'queue_not_found');
  } finally {
    await c.deleteQueue(name).catch(() => {});
    await c.deleteQueue(dlq).catch(() => {});
  }
});
