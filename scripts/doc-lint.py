"""Idempotent doc lint: verify relative markdown links resolve and report stale facts."""
import pathlib
import re
import sys
import urllib.parse

ROOT = pathlib.Path(__file__).resolve().parent.parent
SKIP = (".git", "node_modules", "java/target", "next/.next", "next/out", ".playwright-mcp", ".qoder")
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")

files = [p for p in ROOT.rglob("*.md") if not any(part in str(p.relative_to(ROOT)) for part in SKIP)]
broken = []
for f in files:
    for m in LINK.finditer(f.read_text(errors="ignore")):
        target = m.group(1)
        if target.startswith(("http://", "https://", "mailto:", "#", "<")):
            continue
        path = urllib.parse.unquote(target.split("#")[0])
        if path and not (f.parent / path).resolve().exists():
            broken.append(f"{f.relative_to(ROOT)} -> {target}")

print(f"markdown files: {len(files)}")
for item in broken:
    print("BROKEN", item)
print(f"broken links: {len(broken)}")
sys.exit(1 if broken else 0)
