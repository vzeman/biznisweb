#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_DIR = ROOT / "templates" / "reporting-client"
PROJECTS_DIR = ROOT / "projects"


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
    if not slug:
        raise ValueError("Client slug must contain at least one alphanumeric character.")
    return slug


def render_text(text: str, slug: str, display_name: str) -> str:
    return text.replace("__CLIENT_SLUG__", slug).replace("__CLIENT_DISPLAY_NAME__", display_name)


def main() -> int:
    parser = argparse.ArgumentParser(description="Scaffold a new reporting client bundle from templates.")
    parser.add_argument("client", help="Client slug or display name.")
    parser.add_argument("--display-name", help="Human-readable client name.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing project directory if it already exists.",
    )
    args = parser.parse_args()

    slug = slugify(args.client)
    display_name = args.display_name.strip() if args.display_name else slug[:1].upper() + slug[1:]
    target_dir = PROJECTS_DIR / slug
    if target_dir.exists():
        if not args.force:
            raise SystemExit(f"Target directory already exists: {target_dir}")
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    for template_path in TEMPLATE_DIR.iterdir():
        if template_path.name == "README_CLIENT_SETUP.md":
            destination = target_dir / template_path.name
        elif template_path.name == "settings.template.json":
            destination = target_dir / "settings.json"
        else:
            destination = target_dir / template_path.name

        if template_path.suffix == ".json":
            payload = json.loads(render_text(template_path.read_text(encoding="utf-8"), slug, display_name))
            destination.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            continue

        destination.write_text(
            render_text(template_path.read_text(encoding="utf-8"), slug, display_name),
            encoding="utf-8",
        )

    print(f"Scaffolded reporting client template at {target_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
