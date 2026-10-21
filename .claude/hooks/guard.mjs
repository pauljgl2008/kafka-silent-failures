#!/usr/bin/env node
// PreToolUse hook for Claude Code. It blocks two things an AI agent (or a tired human)
// should never do in this repo:
//   1. Edit a Flyway migration that is already committed. Flyway checks a checksum
//      of every applied migration, so changing one breaks every database that ran it.
//      The fix is always a new V<n>__*.sql file.
//   2. Force-push or skip the git hooks.
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

const MIGRATION_RE = /src\/main\/resources\/db\/migration\/V[^/]*\.sql$/;
const FORCE_PUSH_RE = /\bgit\s+push\b[^;&|]*\s(--force(-with-lease)?|-f)\b/;
const NO_VERIFY_RE = /\bgit\b[^;&|]*\s--no-verify\b/;

export function decide(input, trackedFiles) {
  const tool = input.tool_name;
  const args = input.tool_input ?? {};

  if (tool === 'Edit' || tool === 'Write' || tool === 'MultiEdit') {
    const path = String(args.file_path ?? '').replaceAll('\\', '/');
    const tracked = trackedFiles.find((f) => path.endsWith(f));
    if (MIGRATION_RE.test(path) && tracked) {
      return {
        block: true,
        reason: `${tracked} is already committed. Flyway would reject the new checksum. Add a new migration instead.`,
      };
    }
  }

  if (tool === 'Bash') {
    const command = String(args.command ?? '');
    if (FORCE_PUSH_RE.test(command)) {
      return { block: true, reason: 'Force push is not allowed. Push a new commit instead.' };
    }
    if (NO_VERIFY_RE.test(command)) {
      return { block: true, reason: 'Skipping git hooks is not allowed. Fix what the hook reports.' };
    }
  }

  return { block: false };
}

function trackedMigrations() {
  try {
    return execFileSync('git', ['ls-files', 'src/main/resources/db/migration'], { encoding: 'utf8' })
      .split('\n')
      .filter(Boolean);
  } catch {
    return [];
  }
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  let raw = '';
  process.stdin.on('data', (chunk) => (raw += chunk));
  process.stdin.on('end', () => {
    const result = decide(JSON.parse(raw || '{}'), trackedMigrations());
    if (result.block) {
      // Exit code 2 tells Claude Code to block the call and show the reason to the agent.
      process.stderr.write(result.reason + '\n');
      process.exit(2);
    }
  });
}
