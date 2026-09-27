import { test } from 'node:test';
import assert from 'node:assert/strict';
import { freshKernel, putBlob } from './helpers.js';

test('冒烟：导入并按摘要取回', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const digest = await putBlob(kernel, 'hello');
    const { bytes } = await kernel.readContent(digest);
    assert.equal(bytes.toString(), 'hello');
    assert.equal(kernel.stateInfo().liveObjects, 1);
  } finally {
    await cleanup();
  }
});
