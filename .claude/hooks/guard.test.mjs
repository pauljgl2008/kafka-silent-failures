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
  for (const command of [
    'git push --force',
    'git push -f origin main',
    'git push origin main --force-with-lease',
    'git -C . push --force',
    'git push origin +main',
    'git push -fu origin main',
    'cd repo && git push --force',
  ]) {
    assert.equal(decide({ tool_name: 'Bash', tool_input: { command } }, tracked).block, true, command);
  }
});

test('a normal push is allowed', () => {
  assert.equal(decide({ tool_name: 'Bash', tool_input: { command: 'git push -u origin feat/x' } }, tracked).block, false);
});

test('a push with options but no force is allowed', () => {
  for (const command of ['git push -u origin feat/x', 'git push --set-upstream origin main', 'git push origin feat/a+b']) {
    assert.equal(decide({ tool_name: 'Bash', tool_input: { command } }, tracked).block, false, command);
  }
});

test('skipping git hooks is blocked', () => {
  for (const command of ['git commit --no-verify -m "x"', 'git commit -n -m "x"', 'git commit -anm "x"']) {
    assert.equal(decide({ tool_name: 'Bash', tool_input: { command } }, tracked).block, true, command);
  }
});

test('a normal commit is allowed', () => {
  for (const command of ['git commit -m "x"', 'git commit -am "x"', 'git commit -m "add -n flag"']) {
    assert.equal(decide({ tool_name: 'Bash', tool_input: { command } }, tracked).block, false, command);
  }
});

test('changing a committed migration from the shell is blocked', () => {
  const file = 'src/main/resources/db/migration/V1__schema.sql';
  for (const command of [`sed -i s/a/b/ ${file}`, `echo x >> ${file}`, `perl -pi -e s/a/b/ ${file}`, `rm ${file}`, `git mv ${file} x.sql`]) {
    assert.equal(decide({ tool_name: 'Bash', tool_input: { command } }, tracked).block, true, command);
  }
});

test('reading a committed migration from the shell is allowed', () => {
  const file = 'src/main/resources/db/migration/V1__schema.sql';
  for (const command of [`cat ${file}`, `grep -n TABLE ${file}`, `git diff ${file}`]) {
    assert.equal(decide({ tool_name: 'Bash', tool_input: { command } }, tracked).block, false, command);
  }
});

test('the words alone in another command are allowed', () => {
  assert.equal(decide({ tool_name: 'Bash', tool_input: { command: 'grep -r "--force" docs' } }, tracked).block, false);
});
