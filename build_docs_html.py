"""
One-off script: convert README.md, ARCHITECTURE.md, VALIDATIONS_AND_DIMENSIONS.md
to standalone HTML files with a shared executive-friendly theme.
Output: README.html, ARCHITECTURE.html, VALIDATIONS_AND_DIMENSIONS.html at project root.
"""
import re
from pathlib import Path

try:
    import markdown
except ImportError:
    raise SystemExit("Run: pip install markdown  # then re-run this script")

ROOT = Path(__file__).resolve().parent

# Same theme for all three pages: clean, executive-friendly
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f8f9fa;
      --card: #fff;
      --text: #1a1a1a;
      --text-muted: #555;
      --accent: #0d6efd;
      --border: #dee2e6;
      --code-bg: #e9ecef;
      --table-stripe: #f1f3f5;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
      line-height: 1.6;
      color: var(--text);
      background: var(--bg);
      margin: 0;
      padding: 2rem 1rem;
    }}
    .container {{
      max-width: 800px;
      margin: 0 auto;
      background: var(--card);
      padding: 2.5rem 3rem;
      border-radius: 8px;
      box-shadow: 0 1px 3px rgba(0,0,0,.08);
    }}
    h1 {{
      font-size: 1.75rem;
      font-weight: 600;
      margin-top: 0;
      margin-bottom: 1.5rem;
      padding-bottom: 0.75rem;
      border-bottom: 2px solid var(--accent);
      color: var(--text);
    }}
    h2 {{
      font-size: 1.35rem;
      font-weight: 600;
      margin-top: 2rem;
      margin-bottom: 0.75rem;
      color: var(--text);
    }}
    h3 {{
      font-size: 1.15rem;
      font-weight: 600;
      margin-top: 1.5rem;
      margin-bottom: 0.5rem;
      color: var(--text);
    }}
    h4, h5, h6 {{
      font-size: 1rem;
      font-weight: 600;
      margin-top: 1.25rem;
      margin-bottom: 0.5rem;
    }}
    p {{
      margin: 0 0 1rem;
      color: var(--text);
    }}
    a {{
      color: var(--accent);
      text-decoration: none;
    }}
    a:hover {{
      text-decoration: underline;
    }}
    ul, ol {{
      margin: 0 0 1rem;
      padding-left: 1.5rem;
    }}
    li {{
      margin-bottom: 0.35rem;
    }}
    code {{
      font-family: "Consolas", "Monaco", monospace;
      font-size: 0.9em;
      background: var(--code-bg);
      padding: 0.15em 0.4em;
      border-radius: 4px;
    }}
    pre {{
      background: var(--code-bg);
      padding: 1rem 1.25rem;
      border-radius: 6px;
      overflow-x: auto;
      margin: 1rem 0;
      border: 1px solid var(--border);
    }}
    pre code {{
      background: none;
      padding: 0;
      font-size: 0.85rem;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 1rem 0;
      font-size: 0.95rem;
    }}
    th, td {{
      border: 1px solid var(--border);
      padding: 0.6rem 0.75rem;
      text-align: left;
    }}
    th {{
      background: var(--table-stripe);
      font-weight: 600;
    }}
    tr:nth-child(even) {{ background: var(--table-stripe); }}
    blockquote {{
      margin: 1rem 0;
      padding: 0.5rem 0 0.5rem 1rem;
      border-left: 4px solid var(--accent);
      background: var(--table-stripe);
      color: var(--text-muted);
    }}
    .mermaid {{
      margin: 1rem 0;
      min-height: 120px;
    }}
    hr {{
      border: none;
      border-top: 1px solid var(--border);
      margin: 2rem 0;
    }}
  </style>
  {mermaid_script}
</head>
<body>
  <div class="container">
    {body}
  </div>
  {mermaid_init}
</body>
</html>
"""


def md_to_html(path: Path, title: str, enable_mermaid: bool = False) -> str:
    raw = path.read_text(encoding="utf-8")
    html = markdown.markdown(
        raw,
        extensions=["extra", "sane_lists", "nl2br"],
        extension_configs={"extra": {"enable_attributes": True}},
    )
    if enable_mermaid:
        # Replace mermaid code block with div.mermaid so mermaid.js can render it
        html = re.sub(
            r'<pre><code[^>]*>(.*?)</code></pre>',
            _replace_mermaid_block,
            html,
            flags=re.DOTALL,
        )
    return HTML_TEMPLATE.format(
        title=title,
        body=html,
        mermaid_script='<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>' if enable_mermaid else "",
        mermaid_init='<script>mermaid.initialize({ startOnLoad: true });</script>' if enable_mermaid else "",
    )


def _replace_mermaid_block(match: re.Match) -> str:
    import html as html_lib
    code = html_lib.unescape(match.group(1).strip())
    if "flowchart" in code or "graph " in code:
        return f'<div class="mermaid">\n{code}\n</div>'
    return match.group(0)


def main() -> None:
    configs = [
        (ROOT / "README.md", "README.html", "Modularized Data Quality — Overview"),
        (ROOT / "docs" / "ARCHITECTURE.md", "ARCHITECTURE.html", "Data Quality Package — Architecture", True),
        (ROOT / "demos" / "VALIDATIONS_AND_DIMENSIONS.md", "VALIDATIONS_AND_DIMENSIONS.html", "Validations and Dimensions Reference"),
    ]
    for item in configs:
        md_path = item[0]
        html_name = item[1]
        title = item[2]
        enable_mermaid = item[3] if len(item) > 3 else False
        if not md_path.exists():
            print(f"Skip (not found): {md_path}")
            continue
        html = md_to_html(md_path, title, enable_mermaid=enable_mermaid)
        out = ROOT / html_name
        out.write_text(html, encoding="utf-8")
        print(f"Wrote {out.name}")


if __name__ == "__main__":
    main()
