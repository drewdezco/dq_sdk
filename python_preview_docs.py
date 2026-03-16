import os
from pathlib import Path
from data_quality import (
    get_readme_html,
    get_usage_html,
    get_architecture_html,
    get_getting_started_html,
    get_pipeline_html,
)
OUTPUT_DIR = Path("./_html_preview")
def ensure_output_dir() -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR
def preview_doc(name: str, html: str, write_file: bool = True) -> None:
    print(f"\n=== {name} ===")
    print(f"Length: {len(html)} characters")
    print("Snippet (first 400 chars):")
    print(html[:400], "...\n")
    if write_file:
        out_dir = ensure_output_dir()
        out_path = out_dir / f"{name.lower()}.html"
        out_path.write_text(html, encoding="utf-8")
        print(f"Full HTML written to: {out_path.resolve()}")
def main() -> None:
    # Generate all HTML docs
    readme_html = get_readme_html()
    usage_html = get_usage_html()
    arch_html = get_architecture_html()
    getting_started_html = get_getting_started_html()
    pipeline_html = get_pipeline_html()
    # Preview and optionally write them to disk
    preview_doc("README", readme_html, True)
    preview_doc("USAGE", usage_html, True)
    preview_doc("ARCHITECTURE", arch_html, True)
    preview_doc("GETTING_STARTED", getting_started_html, True)
    preview_doc("PIPELINE", pipeline_html, True)
if __name__ == "__main__":
    main()