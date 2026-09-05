import test from 'node:test';
import assert from 'node:assert/strict';
import { initializeScript, parseHeader } from './server.js';

const script = `/**\n * @name 测试源\n * @version 1.0.0\n */
const { EVENT_NAMES, on, send } = globalThis.lx
on(EVENT_NAMES.request, ({ source, action, info }) => Promise.resolve('https://example.test/' + source + '/' + info.type))
send(EVENT_NAMES.inited, { sources: { tx: { name: 'QQ', type: 'music', actions: ['musicUrl'], qualitys: ['320k'] } } })`;

test('parses metadata and resolves an LX action', async () => {
  assert.equal(parseHeader(script).name, '测试源');
  const runtime = await initializeScript(script, 'test.js');
  assert.deepEqual(runtime.capabilities.tx.actions, ['musicUrl']);
  assert.equal(await runtime.requestHandler({ source: 'tx', action: 'musicUrl', info: { type: '320k', musicInfo: {} } }), 'https://example.test/tx/320k');
});

test('rejects scripts without registration', async () => {
  await assert.rejects(() => initializeScript('/** @name bad */', 'bad.js'));
});
