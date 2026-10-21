#!/usr/bin/env node
// PreToolUse hook for Claude Code. It blocks two things an AI agent (or a tired human)
// should never do in this repo:
//   1. Change a Flyway migration that is already committed. Flyway checks a checksum
//      of every applied migration, so changing one breaks every database that ran it.
//      The fix is always a new V<n>__*.sql file.
//   2. Force-push or skip the git hooks.
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

const MIGRATION_RE = /src\/main\/resources\/db\/migration\/V[^/]*\.sql$/;
// Git options that come before the subcommand and take a value: `git -C dir push`.
const GIT_OPTIONS_WITH_VALUE = new Set(['-C', '-c', '--git-dir', '--work-tree', '--namespace']);
const WRITE_COMMANDS = new Set(['rm', 'mv', 'cp', 'tee', 'truncate', 'dd']);
const IN_PLACE_EDITORS = new Set(['sed', 'perl']);

const MIGRATION_REASON = (file) =>
  `${file} is already committed. Flyway would reject the new checksum. Add a new migration instead.`;

// "a && b | c; d" -> one entry per command.
const COMMAND_SEPARATOR = /&&|\|\||[;|\n]/;

// Splits one command into words. Quoted text stays one word and is marked,
// so a commit message like "add -n" is not read as a flag.
function wordsOf(command) {
  return [...command.matchAll(/"[^"]*"|'[^']*'|[^\s"']+/g)].map(([text]) => ({
    text,
    quoted: text.startsWith('"') || text.startsWith("'"),
  }));
}

function gitSubcommand(words) {
  let i = 1;
  while (i < words.length && words[i].text.startsWith('-')) {
    i += GIT_OPTIONS_WITH_VALUE.has(words[i].text) ? 2 : 1;
  }
  return { name: words[i]?.text, args: words.slice(i + 1) };
}

// True for "-f" and for short flags grouped together, like "-fu".
const hasShortFlag = (word, letter) => !word.quoted && /^-[a-zA-Z]+$/.test(word.text) && word.text.includes(letter);

function checkGit(words) {
  if (words.some((w) => !w.quoted && w.text === '--no-verify')) {
    return 'Skipping git hooks is not allowed. Fix what the hook reports.';
  }
  const { name, args } = gitSubcommand(words);
  if (name === 'push') {
    const force = args.some((w) =>
      (!w.quoted && /^--force(-with-lease|-if-includes)?(=.*)?$/.test(w.text))
      || hasShortFlag(w, 'f')
      || (!w.quoted && w.text.startsWith('+')));
    if (force) {
      return 'Force push is not allowed. Push a new commit instead.';
    }
  }
  if (name === 'commit' && args.some((w) => hasShortFlag(w, 'n'))) {
    return 'Skipping git hooks is not allowed. Fix what the hook reports.';
  }
  return null;
}

function checkMigrationFromShell(words, raw, trackedFiles) {
  const touched = trackedFiles.find((file) => words.some((w) => w.text.replaceAll('\\', '/').endsWith(file)));
  if (!touched) {
    return null;
  }
  const program = words[0].text;
  const writes =
    WRITE_COMMANDS.has(program)
    || (IN_PLACE_EDITORS.has(program) && words.some((w) => /^-\w*i/.test(w.text)))
    || (program === 'git' && ['mv', 'rm', 'checkout', 'restore'].includes(gitSubcommand(words).name))
    || />/.test(raw);
  return writes ? MIGRATION_REASON(touched) : null;
}

export function decide(input, trackedFiles) {
  const tool = input.tool_name;
  const args = input.tool_input ?? {};

  if (tool === 'Edit' || tool === 'Write' || tool === 'MultiEdit') {
    const path = String(args.file_path ?? '').replaceAll('\\', '/');
    const tracked = trackedFiles.find((f) => path.endsWith(f));
    if (MIGRATION_RE.test(path) && tracked) {
      return { block: true, reason: MIGRATION_REASON(tracked) };
    }
  }

  if (tool === 'Bash') {
    const line = String(args.command ?? '');
    for (const part of line.split(COMMAND_SEPARATOR)) {
      const words = wordsOf(part);
      if (words.length === 0) {
        continue;
      }
      const reason = (words[0].text === 'git' ? checkGit(words) : null)
        ?? checkMigrationFromShell(words, part, trackedFiles);
      if (reason) {
        return { block: true, reason };
      }
    }
  }

  return { block: false };
}

function trackedMigrations() {
  try {
    return execFileSync('git', ['ls-files', '--full-name', 'src/main/resources/db/migration'], {
      encoding: 'utf8',
      cwd: process.env.CLAUDE_PROJECT_DIR || process.cwd(),
    })
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
