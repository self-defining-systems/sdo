const { test } = require('node:test');
const assert = require('node:assert/strict');
const { restoreAll } = require('./restore.cjs');

test('all disjoint archives start before either completes, including misses', async () => {
  const pending = [];
  const inputs = ['normal', 'race'].map(key => ({path: `/tmp/${key}\n~/go/pkg/mod`, key, 'restore-keys': `${key}-fallback\n${key}-older`}));
  const result = restoreAll(inputs, (paths, key, keys) => new Promise(resolve => {
    assert.equal(paths[0], `/tmp/${key}`);
    assert.equal(paths[1], '~/go/pkg/mod');
    assert.deepEqual(keys, [`${key}-fallback`, `${key}-older`]);
    pending.push(resolve);
  }));
  assert.equal(pending.length, 2);
  pending[1](undefined);
  pending[0]('normal-hit');
  assert.deepEqual(await result, ['normal-hit', undefined]);
});

test('waits for the other restore before reporting an archive failure', async () => {
  let finish;
  let completed = false;
  const result = restoreAll(['normal', 'race'].map(key => ({path: key, key, 'restore-keys': key})),
    (paths, key) => key === 'normal' ? Promise.reject(new Error('archive failure')) : new Promise(resolve => { finish = resolve; }));
  result.catch(() => { completed = true; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(completed, false);
  finish('race-hit');
  await assert.rejects(result, /Go cache restore failed/);
});
