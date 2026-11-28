#!/usr/bin/env python3
"""Core pack checks that no off-the-shelf linter does. Pack: core. Standard library only.

  core-check.py [ROOT]

1. size:  no source file over CORE_MAX_LINES lines (default 750).
2. name:  no file or folder named utils, helpers, misc, common, manager or the like (a grab-bag, not a domain).
3. agents: AGENTS.md and CLAUDE.md stay under CORE_MAX_AGENTS_LINES lines (default 200).
4. skill: each SKILL.md stays under CORE_MAX_SKILL_LINES lines (default 500), also under .claude/skills and .agents/skills.

A waiver goes in ROOT/.core-allow, one per line: `<check> <path> <reason>`. The reason is required.
Hidden folders, vendored folders and git submodule paths (.gitmodules) are skipped. Exit 1 when anything fails.
"""
import fnmatch
import os
import re
import sys
from pathlib import Path

SKIP_DIRS = {"node_modules", "vendor", "dist", "build", "target", "__pycache__", "venv", ".venv", "site-packages"}
SOURCE_EXT = {".py", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".go", ".java", ".kt", ".rs", ".rb", ".cs",
              ".c", ".h", ".cc", ".cpp", ".hpp", ".swift", ".php", ".sh", ".scala", ".sql"}
GRAB_BAG = re.compile(r"^(utils?|helpers?|misc|common|manager|managers|stuff|things)$|"
                      r"(_|-)(utils?|helpers?|manager)$|^[A-Za-z]+(Utils?|Helpers?|Manager)$", re.I)
AGENT_FILES = ("AGENTS.md", "CLAUDE.md")
# Hidden folders are skipped by walk(), but agents load skills from these, so they are read on purpose.
HIDDEN_SKILL_DIRS = (".claude/skills", ".agents/skills")


def load_waivers(root):
    waivers, problems = set(), []
    path = root / ".core-allow"
    if not path.is_file():
        return waivers, problems
    for n, line in enumerate(path.read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            problems.append(f".core-allow:{n}: needs `<check> <path> <reason>`, the reason is required")
        else:
            waivers.add((parts[0], parts[1]))
    return waivers, problems


def submodule_paths(root):
    """Paths named in .gitmodules: third-party code the repo pins, not code it owns."""
    try:
        text = (Path(root) / ".gitmodules").read_text()
    except OSError:
        return set()
    return {m.group(1).strip().strip("/") for m in re.finditer(r"^\s*path\s*=\s*(.+)$", text, re.M)}


def walk(root):
    subs = submodule_paths(root)
    for dirpath, dirs, files in os.walk(root):
        rel = os.path.relpath(dirpath, root)
        dirs[:] = sorted(d for d in dirs if not d.startswith(".") and d not in SKIP_DIRS
                         and os.path.normpath(os.path.join(rel, d)) not in subs)
        for name in sorted(files):
            yield Path(dirpath) / name


def excluded(rel, globs):
    """CORE_EXCLUDE globs, matched the way lizard matches its -x globs (against ./<path>), so every step skips the same files."""
    return any(fnmatch.fnmatch(f"./{rel}", g) or fnmatch.fnmatch(rel, g) for g in globs)


def stem_is_grab_bag(name):
    return bool(GRAB_BAG.search(Path(name).stem)) if "." in name else bool(GRAB_BAG.search(name))


def _folder_problems(path, root, seen_dirs, waivers):
    found = []
    for part in path.relative_to(root).parts[:-1]:
        folder = path.relative_to(root).as_posix().split(part)[0] + part
        if folder not in seen_dirs:
            seen_dirs.add(folder)
            if GRAB_BAG.search(part) and ("name", folder) not in waivers:
                found.append(f"{folder}: grab-bag folder name '{part}'. Name it for the domain it serves, or waive it in .core-allow")
    return found


def _source_problems(path, rel, max_lines, waivers):
    found = []
    if stem_is_grab_bag(path.name) and ("name", rel) not in waivers:
        found.append(f"{rel}: grab-bag file name. Name it for the domain it serves, or waive it in .core-allow")
    try:
        count = len(path.read_text(errors="ignore").splitlines())
    except OSError:
        return found, True
    if count > max_lines and ("size", rel) not in waivers:
        found.append(f"{rel}: {count} lines, over the {max_lines} line cap. Split it by domain, or waive it in .core-allow")
    return found, False


def _agent_problems(path, rel, max_agents, waivers):
    count = len(path.read_text(errors="ignore").splitlines())
    if count > max_agents and ("agents", rel) not in waivers:
        return [f"{rel}: {count} lines, over the {max_agents} line cap for agent instructions. Keep only what the agent cannot learn from the code"]
    return []


def _skill_problems(path, rel, max_skill, waivers):
    count = len(path.read_text(errors="ignore").splitlines())
    if count > max_skill and ("skill", rel) not in waivers:
        return [f"{rel}: {count} lines, over the {max_skill} line cap for a skill. Move detail into files the skill links to"]
    return []


def hidden_skill_files(root):
    for base in HIDDEN_SKILL_DIRS:
        for path in sorted((root / base).rglob("SKILL.md")):
            if not SKIP_DIRS.intersection(path.relative_to(root).parts):
                yield path


def _file_problems(path, root, limits, seen_dirs):
    max_lines, max_agents, max_skill, waivers = limits
    rel = path.relative_to(root).as_posix()
    found = _folder_problems(path, root, seen_dirs, waivers)
    if path.suffix in SOURCE_EXT:
        more, unreadable = _source_problems(path, rel, max_lines, waivers)
        found += more
        if unreadable:
            return found
    if path.name in AGENT_FILES and path.parent == root:
        found += _agent_problems(path, rel, max_agents, waivers)
    if path.name == "SKILL.md":
        found += _skill_problems(path, rel, max_skill, waivers)
    return found


def main(argv):
    root = Path(argv[1] if len(argv) > 1 else ".").resolve()
    max_lines = int(os.environ.get("CORE_MAX_LINES", "750"))
    max_agents = int(os.environ.get("CORE_MAX_AGENTS_LINES", "200"))
    max_skill = int(os.environ.get("CORE_MAX_SKILL_LINES", "500"))
    globs = os.environ.get("CORE_EXCLUDE", "").split()
    waivers, problems = load_waivers(root)
    seen_dirs = set()
    limits = (max_lines, max_agents, max_skill, waivers)
    for path in [*walk(root), *hidden_skill_files(root)]:
        if excluded(path.relative_to(root).as_posix(), globs):
            continue
        problems += _file_problems(path, root, limits, seen_dirs)
    for p in problems:
        print(f"core-check: {p}")
    if not problems:
        print("core-check: ok")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
