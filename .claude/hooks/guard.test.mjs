import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decide } from './guard.mjs';

const tracked = ['src/main/resources/db/migration/V1__schema.sql'];

test('editing a committed migration is blocked', () => {
  const result = decide(
    { tool_name: 'Edit', tool_input: { file_path: 'C:\\repo\\src\\main\\resources\\db\\migration\\V1__schema.sql' } },
    tracked,
  );
  assert.equal(result.block, true);
  assert.match(result.reason, /new migration/);
});

test('writing a new migration is allowed', () => {
  const result = decide(
    { tool_name: 'Write', tool_input: { file_path: '/repo/src/main/resources/db/migration/V2__add_index.sql' } },
    tracked,
  );
  assert.equal(result.block, false);
});

test('editing normal code is allowed', () => {
  const result = decide({ tool_name: 'Edit', tool_input: { file_path: '/repo/src/main/java/App.java' } }, tracked);
  assert.equal(result.block, false);
});

test('force push is blocked in all its forms', () => {
  for (const command of ['git push --force', 'git push -f origin main', 'git push origin main --force-with-lease']) {
    assert.equal(decide({ tool_name: 'Bash', tool_input: { command } }, tracked).block, true, command);
  }
});

test('a normal push is allowed', () => {
  assert.equal(decide({ tool_name: 'Bash', tool_input: { command: 'git push -u origin feat/x' } }, tracked).block, false);
});

test('skipping git hooks is blocked', () => {
  assert.equal(decide({ tool_name: 'Bash', tool_input: { command: 'git commit --no-verify -m "x"' } }, tracked).block, true);
});

test('the words alone in another command are allowed', () => {
  assert.equal(decide({ tool_name: 'Bash', tool_input: { command: 'grep -r "--force" docs' } }, tracked).block, false);
});
