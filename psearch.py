#!/usr/bin/env python3
"""
psearch.py - recursive search for .txt/.csv with table output.

CSV output:
- One CSV file => one MySQL-style ASCII table.
- Columns: Line | <matched column(s) only>
- Matched columns are UNION across:
    * header-name matches (pattern matches column name)
    * value matches (pattern matches a cell) in any matching row
- Rows shown are rows whose raw CSV line matched the pattern.

TXT output:
- One TXT file => one MySQL-style ASCII table.
- Columns: Line | Matched Text
- Matched Text is the full line with matches highlighted (colors only if interactive TTY).

Windows piping safety:
- Avoid Unicode symbols in output by default (no '▶', no em dashes).
- Disable color automatically when stdout is not a TTY.
- Configure stdout to UTF-8 where possible to avoid pipe/clip failures.
"""

from __future__ import annotations

import argparse
import csv
import fnmatch
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

# ----------------- stdout safety (Windows piping) -----------------
def _configure_stdout_safely() -> None:
    # Python 3.7+: helps when piping to tools like clip
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

_configure_stdout_safely()

# -------- TOML loading (py3.11+) ----------
try:
    import tomllib  # Python 3.11+
except Exception:
    tomllib = None  # type: ignore

# -------- Optional colorama for Windows ----------
try:
    import colorama  # type: ignore
    # convert/strip only when TTY; prevents weird issues when piping
    _isatty = bool(getattr(sys.stdout, "isatty", lambda: False)())
    colorama.init(strip=not _isatty, convert=_isatty)
except Exception:
    colorama = None  # type: ignore


# ----------------- Colors -----------------
class C:
    RESET = "\x1b[0m"
    DIM = "\x1b[2m"
    BOLD = "\x1b[1m"

    GREEN = "\x1b[32m"
    YELLOW = "\x1b[33m"
    CYAN = "\x1b[36m"

    HL = "\x1b[30;43m"  # black on yellow highlight


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s)


def supports_color() -> bool:
    # Critical: colors OFF when piped (e.g., | clip)
    if not sys.stdout.isatty():
        return False
    if os.environ.get("NO_COLOR"):
        return False
    return True


def colorize(s: str, code: str, enable: bool) -> str:
    return f"{code}{s}{C.RESET}" if enable else s


# ----------------- Config -----------------
@dataclass
class SearchConfig:
    roots: List[str]
    extensions: List[str]
    exclude_dirs: List[str]
    exclude_files: List[str]
    case_sensitive: bool
    regex: bool
    max_matches: int
    max_matches_per_file: int
    max_file_mb: int
    follow_symlinks: bool


@dataclass
class OutputConfig:
    show_file_banner: bool
    context_separator: str
    color: bool

    csv_delimiter: str
    csv_max_cols: int
    table_truncate_cell: int  # max printable chars per cell


@dataclass
class AppConfig:
    search: SearchConfig
    output: OutputConfig


DEFAULT_TOML = """\
[search]
roots = ["."]
extensions = [".txt", ".csv"]
exclude_dirs = [".git", "__pycache__", "node_modules", ".venv", "venv", "dist", "build"]
exclude_files = []
case_sensitive = false
regex = false
max_matches = 5000
max_matches_per_file = 200
max_file_mb = 50
follow_symlinks = false

[output]
show_file_banner = true
context_separator = "-"
color = true

csv_delimiter = ","
csv_max_cols = 200
table_truncate_cell = 200
"""


def load_toml_config(path: Path) -> Dict:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    if tomllib is None:
        raise RuntimeError("tomllib not available. Use Python 3.11+.")
    with path.open("rb") as f:
        return tomllib.load(f)


def dict_get(d: Dict, path: str, default):
    cur = d
    parts = path.split(".")
    for p in parts[:-1]:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur.get(parts[-1], default) if isinstance(cur, dict) else default


def build_config(toml_dict: Dict) -> AppConfig:
    search = SearchConfig(
        roots=list(dict_get(toml_dict, "search.roots", ["." ])),
        extensions=list(dict_get(toml_dict, "search.extensions", [".txt", ".csv"])),
        exclude_dirs=list(dict_get(toml_dict, "search.exclude_dirs", [])),
        exclude_files=list(dict_get(toml_dict, "search.exclude_files", [])),
        case_sensitive=bool(dict_get(toml_dict, "search.case_sensitive", False)),
        regex=bool(dict_get(toml_dict, "search.regex", False)),
        max_matches=int(dict_get(toml_dict, "search.max_matches", 5000)),
        max_matches_per_file=int(dict_get(toml_dict, "search.max_matches_per_file", 200)),
        max_file_mb=int(dict_get(toml_dict, "search.max_file_mb", 50)),
        follow_symlinks=bool(dict_get(toml_dict, "search.follow_symlinks", False)),
    )
    output = OutputConfig(
        show_file_banner=bool(dict_get(toml_dict, "output.show_file_banner", True)),
        context_separator=str(dict_get(toml_dict, "output.context_separator", "-")),
        color=bool(dict_get(toml_dict, "output.color", True)),
        csv_delimiter=str(dict_get(toml_dict, "output.csv_delimiter", ",")),
        csv_max_cols=int(dict_get(toml_dict, "output.csv_max_cols", 200)),
        table_truncate_cell=int(dict_get(toml_dict, "output.table_truncate_cell", 200)),
    )
    return AppConfig(search=search, output=output)


def write_default_config(path: Path) -> None:
    if path.exists():
        print(f"Config already exists: {path}")
        return
    path.write_text(DEFAULT_TOML, encoding="utf-8")
    print(f"Wrote default config: {path}")


# ----------------- File walking -----------------
def should_exclude_dir(name: str, exclude_dirs: List[str]) -> bool:
    for pat in exclude_dirs:
        if name == pat or fnmatch.fnmatch(name, pat):
            return True
    return False


def should_exclude_file(name: str, exclude_files: List[str]) -> bool:
    for pat in exclude_files:
        if name == pat or fnmatch.fnmatch(name, pat):
            return True
    return False


def iter_files(cfg: SearchConfig) -> Iterable[Path]:
    exts = {e.lower() for e in cfg.extensions}
    for root in cfg.roots:
        root_path = Path(root).expanduser().resolve()
        if not root_path.exists():
            continue

        for dirpath, dirnames, filenames in os.walk(root_path, followlinks=cfg.follow_symlinks):
            dirnames[:] = [d for d in dirnames if not should_exclude_dir(d, cfg.exclude_dirs)]
            for fn in filenames:
                if should_exclude_file(fn, cfg.exclude_files):
                    continue
                low = fn.lower()
                if not any(low.endswith(ext) for ext in exts):
                    continue
                yield Path(dirpath) / fn


# ----------------- Search helpers -----------------
def compile_pattern(term: str, regex: bool, case_sensitive: bool) -> re.Pattern:
    flags = 0 if case_sensitive else re.IGNORECASE
    return re.compile(term, flags) if regex else re.compile(re.escape(term), flags)


def highlight_matches(text: str, pat: re.Pattern, enable_color: bool) -> str:
    if not enable_color:
        return text

    def repl(m: re.Match) -> str:
        return f"{C.HL}{m.group(0)}{C.RESET}"

    return pat.sub(repl, text)


def is_too_big(path: Path, max_file_mb: int) -> bool:
    try:
        size = path.stat().st_size
    except Exception:
        return True
    return size > (max_file_mb * 1024 * 1024)


def read_lines_stream(path: Path) -> Iterable[str]:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                yield line.rstrip("\n").rstrip("\r")
    except Exception:
        with path.open("r", encoding="latin-1", errors="replace") as f:
            for line in f:
                yield line.rstrip("\n").rstrip("\r")


def parse_csv_line(line: str, delimiter: str) -> List[str]:
    try:
        r = csv.reader([line], delimiter=delimiter)
        return next(r, [])
    except Exception:
        return line.split(delimiter)


def truncate_plain(s: str, max_len: int) -> str:
    if max_len <= 0:
        return s
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def mysql_table(headers: List[str], rows: List[List[str]], truncate_to: int) -> str:
    def prep(cell: str) -> str:
        plain = strip_ansi(cell)
        if len(plain) > truncate_to:
            return truncate_plain(plain, truncate_to)
        return cell

    h_cells = [prep(h) for h in headers]
    r_cells = [[prep(c) for c in row] for row in rows]

    widths: List[int] = []
    for ci in range(len(h_cells)):
        w = len(strip_ansi(h_cells[ci]))
        for row in r_cells:
            if ci < len(row):
                w = max(w, len(strip_ansi(row[ci])))
        widths.append(w)

    def sep() -> str:
        parts = ["+"]
        for w in widths:
            parts.append("-" * (w + 2))
            parts.append("+")
        return "".join(parts)

    def fmt_row(cells: List[str]) -> str:
        out = ["|"]
        for i, w in enumerate(widths):
            c = cells[i] if i < len(cells) else ""
            pad = w - len(strip_ansi(c))
            out.append(" " + c + (" " * pad) + " ")
            out.append("|")
        return "".join(out)

    lines: List[str] = []
    lines.append(sep())
    lines.append(fmt_row(h_cells))
    lines.append(sep())
    for row in r_cells:
        lines.append(fmt_row(row))
    lines.append(sep())
    return "\n".join(lines)


# ----------------- TXT aggregated per file -----------------
@dataclass
class TxtFileResult:
    rows: List[Tuple[int, str]]  # (line_no, matched_line)


def search_txt_file_table(
    path: Path,
    pat: re.Pattern,
    cfg: SearchConfig,
    enable_color: bool
) -> Optional[TxtFileResult]:
    if is_too_big(path, cfg.max_file_mb):
        return None

    rows: List[Tuple[int, str]] = []
    line_no = 0
    per_file = 0

    for line in read_lines_stream(path):
        line_no += 1
        if pat.search(line) is None:
            continue
        rows.append((line_no, highlight_matches(line, pat, enable_color)))
        per_file += 1
        if per_file >= cfg.max_matches_per_file:
            break

    if not rows:
        return None
    return TxtFileResult(rows=rows)


def print_txt_file_table(result: TxtFileResult, out: OutputConfig, enable_color: bool) -> None:
    headers = [
        colorize("Line", C.CYAN + C.BOLD, enable_color),
        colorize("Matched Text", C.CYAN + C.BOLD, enable_color),
    ]
    rows = [[colorize(str(ln), C.DIM, enable_color), txt] for ln, txt in result.rows]
    print(colorize(">> TXT (matched lines)", C.GREEN + C.BOLD, enable_color) +
          colorize(f" rows={len(result.rows)}", C.DIM, enable_color))
    print(mysql_table(headers, rows, truncate_to=out.table_truncate_cell))


# ----------------- CSV aggregated per file -----------------
@dataclass
class CsvFileResult:
    headers: List[str]
    matched_cols: List[int]                 # union of matched columns to display
    rows: List[Tuple[int, List[str]]]       # (line_no, parsed_row)


def search_csv_file_matched_cols_only(
    path: Path,
    pat: re.Pattern,
    cfg: SearchConfig,
    out: OutputConfig
) -> Optional[CsvFileResult]:
    if is_too_big(path, cfg.max_file_mb):
        return None

    header_seen = False
    headers: List[str] = []
    matched_set: Set[int] = set()
    rows: List[Tuple[int, List[str]]] = []

    line_no = 0
    per_file_hits = 0

    for line in read_lines_stream(path):
        line_no += 1

        if not header_seen:
            header_seen = True
            headers = parse_csv_line(line, out.csv_delimiter)[: out.csv_max_cols]
            for i, h in enumerate(headers):
                if pat.search(h):
                    matched_set.add(i)
            continue

        if pat.search(line) is None:
            continue

        row = parse_csv_line(line, out.csv_delimiter)[: out.csv_max_cols]
        for i, h in enumerate(headers):
            v = row[i] if i < len(row) else ""
            if pat.search(h) or pat.search(v):
                matched_set.add(i)

        rows.append((line_no, row))
        per_file_hits += 1
        if per_file_hits >= cfg.max_matches_per_file:
            break

    if not header_seen:
        return None

    if not matched_set and not rows:
        return None

    matched_cols = sorted(matched_set)
    return CsvFileResult(headers=headers, matched_cols=matched_cols, rows=rows)


def print_csv_file_table_matched_cols_only(
    result: CsvFileResult,
    pat: re.Pattern,
    out: OutputConfig,
    enable_color: bool
) -> None:
    cols = result.matched_cols

    table_headers: List[str] = [colorize("Line", C.CYAN + C.BOLD, enable_color)]
    for i in cols:
        h = result.headers[i] if i < len(result.headers) else ""
        table_headers.append(highlight_matches(h, pat, enable_color))

    table_rows: List[List[str]] = []
    for ln, row in result.rows:
        r: List[str] = [colorize(str(ln), C.DIM, enable_color)]
        for i in cols:
            v = row[i] if i < len(row) else ""
            r.append(highlight_matches(v, pat, enable_color))
        table_rows.append(r)

    print(colorize(">> CSV (matched columns)", C.GREEN + C.BOLD, enable_color) +
          colorize(f" cols={len(cols)} rows={len(result.rows)}", C.DIM, enable_color))
    print(mysql_table(table_headers, table_rows, truncate_to=out.table_truncate_cell))


# ----------------- CLI -----------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="psearch",
        description="Search .txt/.csv recursively. Outputs tables for both TXT and CSV.",
    )
    p.add_argument("term", nargs="?", help="Search term (substring by default, or regex if --regex).")

    p.add_argument("-c", "--config", default="psearch.toml", help="Path to TOML config file (default: psearch.toml)")
    p.add_argument("--init-config", action="store_true", help="Create a default psearch.toml in the current directory.")

    p.add_argument("-r", "--root", action="append", help="Add a root folder to search (repeatable). Overrides config roots if provided.")
    p.add_argument("-e", "--ext", action="append", help="Add extension to include (repeatable). Overrides config extensions if provided.")
    p.add_argument("--exclude-dir", action="append", help="Exclude directory name/glob (repeatable). Adds to config exclude_dirs.")
    p.add_argument("--exclude-file", action="append", help="Exclude file name/glob (repeatable). Adds to config exclude_files.")

    p.add_argument("--regex", action="store_true", help="Treat term as regex (overrides config).")
    p.add_argument("--case", action="store_true", help="Case-sensitive search (overrides config).")
    p.add_argument("--max", type=int, help="Max total matches (overrides config).")
    p.add_argument("--max-per-file", type=int, help="Max matches per file (overrides config).")
    p.add_argument("--max-mb", type=int, help="Skip files larger than this MB (overrides config).")
    p.add_argument("--follow-symlinks", action="store_true", help="Follow symlinks (overrides config).")
    p.add_argument("--file-name-only", action="store_true", help="Search only file names (very fast).")

    p.add_argument("--no-color", action="store_true", help="Disable color output.")
    p.add_argument("--no-banner", action="store_true", help="Do not print per-file banner lines.")
    p.add_argument("--no-sep", action="store_true", help="Do not print separators between files.")

    return p.parse_args()


def apply_overrides(cfg: AppConfig, args: argparse.Namespace) -> Tuple[AppConfig, bool, bool]:
    enable_color = cfg.output.color and supports_color() and (not args.no_color)

    if args.root:
        cfg.search.roots = args.root
    if args.ext:
        cfg.search.extensions = args.ext

    if args.exclude_dir:
        cfg.search.exclude_dirs += args.exclude_dir
    if args.exclude_file:
        cfg.search.exclude_files += args.exclude_file

    if args.regex:
        cfg.search.regex = True
    if args.case:
        cfg.search.case_sensitive = True
    if args.max is not None:
        cfg.search.max_matches = max(1, args.max)
    if args.max_per_file is not None:
        cfg.search.max_matches_per_file = max(1, args.max_per_file)
    if args.max_mb is not None:
        cfg.search.max_file_mb = max(1, args.max_mb)
    if args.follow_symlinks:
        cfg.search.follow_symlinks = True

    if args.no_banner:
        cfg.output.show_file_banner = False
    if args.no_sep:
        cfg.output.context_separator = ""

    return cfg, enable_color, bool(args.file_name_only)


# ----------------- Main -----------------
def main() -> int:
    args = parse_args()

    if args.init_config:
        write_default_config(Path(args.config))
        return 0

    cfg_path = Path(args.config)
    if not cfg_path.exists():
        print(f"Config not found: {cfg_path}")
        print("Create it with: python psearch.py --init-config")
        return 2

    try:
        cfg = build_config(load_toml_config(cfg_path))
    except Exception as e:
        print(f"Failed to load config: {e}")
        return 2

    cfg, enable_color, file_name_only = apply_overrides(cfg, args)

    term = args.term
    if not term:
        print("Missing search term.")
        print("Usage: python psearch.py <term> [-r PATH] [--regex] [--case]")
        return 2

    try:
        pat = compile_pattern(term, cfg.search.regex, cfg.search.case_sensitive)
    except re.error as e:
        print(f"Invalid regex: {e}")
        return 2

    total_matches = 0
    files_scanned = 0

    for fpath in iter_files(cfg.search):
        files_scanned += 1

        if file_name_only:
            if pat.search(str(fpath)) is not None:
                if cfg.output.show_file_banner:
                    print(colorize(str(fpath), C.CYAN + C.BOLD, enable_color))
                print(">> " + str(fpath))
                total_matches += 1
                if total_matches >= cfg.search.max_matches:
                    break
            continue

        if fpath.name.lower().endswith(".csv"):
            res = search_csv_file_matched_cols_only(fpath, pat, cfg.search, cfg.output)
            if res is None:
                continue

            if cfg.output.show_file_banner:
                print(colorize(str(fpath), C.CYAN + C.BOLD, enable_color))
            print_csv_file_table_matched_cols_only(res, pat, cfg.output, enable_color)

            total_matches += len(res.rows) if res.rows else 1
        else:
            res_txt = search_txt_file_table(fpath, pat, cfg.search, enable_color)
            if res_txt is None:
                continue

            if cfg.output.show_file_banner:
                print(colorize(str(fpath), C.CYAN + C.BOLD, enable_color))
            print_txt_file_table(res_txt, cfg.output, enable_color)

            total_matches += len(res_txt.rows)

        if total_matches >= cfg.search.max_matches:
            break

        if cfg.output.context_separator:
            print(cfg.output.context_separator * 40)

    summary = f"Scanned {files_scanned} file(s). Found {total_matches} match(es)."
    if total_matches == 0:
        print(colorize(summary, C.YELLOW, enable_color))
        return 1
    print(colorize(summary, C.GREEN, enable_color))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
