"""``overture-datasets``: CLI over dataset config files and the license policy.

Subcommands:

* ``validate``  strict schema validation of ``<provider>.json`` files
  (optionally also the license policy file).
* ``parse``     dump validated dataset(s) as JSON or a one-line-per-resource table.
* ``license``   render a ``.txt``/``.md`` attribution file for a set of providers.
* ``policy``    validate a license policy file and check datasets against a theme.

Datasets are selected by *spec*: ``provider`` (every resource in the file) or
``provider:resource`` (one resource). With no specs, every ``*.json`` in the
datasets directory is selected.

Nothing here assumes where the config files live; ``--datasets-dir`` is
always explicit.
"""

from __future__ import annotations

import argparse
import difflib
import enum
import glob
import json
import sys
from pathlib import Path
from typing import Sequence

from rich.console import Console
from rich.table import Table
from rich.text import Text

from overture_core.dataset import banner
from overture_core.dataset.attribution import RENDERERS, entries_from_file
from overture_core.dataset.license_policy import load_policy
from overture_core.dataset.schema import DatasetFile, validate_all, validate_file

PROG = "overture-datasets"


class ExitCode(enum.IntEnum):
    """Process exit status; distinct codes let CI tell failure kinds apart."""

    OK = 0
    INVALID = 1  # a dataset config or policy file failed validation
    USAGE = 2  # bad arguments (also what argparse itself exits with)
    POLICY_VIOLATION = 3  # a license is not allowed (or unlisted with --strict)
    IO_ERROR = 4  # missing/unreadable/unwritable file, or nothing matched


EPILOG = """\
examples:
  overture-datasets validate 'path/to/datasets/*.json'
  overture-datasets parse -d path/to/datasets acme:planet
  overture-datasets license -d path/to/datasets acme globex -o ATTRIBUTION.md

exit status:
  0  success
  1  a dataset config or policy file failed validation
  2  invalid arguments
  3  license policy violation
  4  missing, unreadable or unwritable file, or nothing matched
"""


class CliError(Exception):
    """User-facing failure: message to stderr, process exits with ``code``."""

    def __init__(
        self, message: str, code: ExitCode = ExitCode.INVALID, hint: str | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.hint = hint


def _did_you_mean(word: str, options: Sequence[str]) -> str | None:
    """Closest match for a mistyped *word*, formatted as a hint (or ``None``)."""
    close = difflib.get_close_matches(word, options, n=1)
    return f"did you mean '{close[0]}'?" if close else None


# ── output ───────────────────────────────────────────────────────────────────
# Rich handles TTY detection, NO_COLOR/FORCE_COLOR, Windows consoles and
# color-depth downgrade; --color overrides it through _configure().

_OPTS = {"highlight": True, "markup": False, "soft_wrap": True}
_out = Console(**_OPTS)
_err = Console(stderr=True, **_OPTS)


def _configure(mode: str = "auto") -> None:
    global _out, _err
    opts = dict(_OPTS)
    if mode == "always":
        opts.update(force_terminal=True, no_color=False)
    elif mode == "never":
        opts["color_system"] = None
    _out = Console(**opts)
    _err = Console(stderr=True, **opts)


def _say(text: str, style: str = "", *, err: bool = False) -> None:
    (_err if err else _out).print(text, style=style)


def _ok(text: str, *, err: bool = False) -> None:
    _say(text, "green", err=err)


def _fail(text: str, *, err: bool = True) -> None:
    _say(text, "red", err=err)


def _warn(text: str) -> None:
    _say(text, "yellow", err=True)


def _hint(text: str) -> None:
    _say(f"hint: {text}", "cyan", err=True)


def _print_table(rows: Sequence[Sequence[str | Text]]) -> None:
    """Print rows (first row is the header) as a table."""
    header, *body = rows
    table = Table()
    for name in header:
        table.add_column(name)
    for row in body:
        table.add_row(*row)
    _out.print(table, soft_wrap=False)


# ── selection


def parse_spec(spec: str) -> tuple[str, str | None]:
    """``"acme"`` -> ``("acme", None)``; ``"acme:planet"`` -> ``("acme", "planet")``."""
    provider, sep, resource = spec.partition(":")
    if not provider or (sep and not resource) or resource.count(":"):
        raise CliError(
            f"invalid dataset spec '{spec}': expected provider[:resource]",
            ExitCode.USAGE,
            hint="use a provider name ('acme') or provider:resource ('acme:planet')",
        )
    return provider, resource or None


def resolve_selection(
    datasets_dir: Path, specs: Sequence[str]
) -> list[tuple[DatasetFile, set[str] | None]]:
    """Validate and load the files named by *specs* (or all files when empty).

    Returns ``(dataset, resource_labels)`` pairs; ``None`` labels means every
    resource in that file. Multiple specs for one provider are merged.
    """
    if not datasets_dir.is_dir():
        raise CliError(
            f"datasets directory not found: {datasets_dir}",
            ExitCode.IO_ERROR,
            hint="check the '-d/--datasets-dir' path",
        )

    wanted: dict[str, set[str] | None] = {}
    if specs:
        for spec in specs:
            provider, resource = parse_spec(spec)
            current = wanted.get(provider, set())
            if resource is None or current is None:
                wanted[provider] = None
            else:
                wanted[provider] = current | {resource}
    else:
        wanted = {p.stem: None for p in sorted(datasets_dir.glob("*.json"))}
        if not wanted:
            raise CliError(
                f"no *.json dataset files in {datasets_dir}",
                ExitCode.IO_ERROR,
                hint="point '-d' at the directory that holds the '<provider>.json' files",
            )

    selection = []
    for provider in sorted(wanted):
        path = datasets_dir / f"{provider}.json"
        if not path.is_file():
            known = [p.stem for p in datasets_dir.glob("*.json")]
            raise CliError(
                f"no dataset file for provider '{provider}': {path}",
                ExitCode.IO_ERROR,
                hint=_did_you_mean(provider, known)
                or "provider names are the config file names without .json",
            )
        try:
            dataset = validate_file(path)
        except ValueError as exc:
            raise CliError(
                f"{path.name} failed validation:\n{exc}",
                hint=f"fix the fields listed above, then re-run: 'validate {path}'",
            ) from exc
        resources = wanted[provider]
        if resources is not None:
            available = {r.label for r in dataset.resources}
            missing = sorted(resources - available)
            if missing:
                raise CliError(
                    f"resource(s) {missing} not found in provider '{provider}'; "
                    f"available: {sorted(available)}",
                    ExitCode.IO_ERROR,
                    hint=_did_you_mean(missing[0], sorted(available)),
                )
        selection.append((dataset, resources))
    return selection


def _expand_paths(raw: Sequence[str]) -> list[Path]:
    """Expand ``*``/``?`` globs ourselves: PowerShell and cmd.exe pass them through verbatim."""
    paths: list[Path] = []
    for item in raw:
        if not any(ch in item for ch in "*?["):
            if not Path(item).is_file():
                raise CliError(
                    f"file not found: {item}",
                    ExitCode.IO_ERROR,
                    hint="check the path; globs such as 'dir/*.json' are supported",
                )
            paths.append(Path(item))
            continue
        matches = sorted(glob.glob(item))
        if not matches:
            raise CliError(
                f"no files match {item}",
                ExitCode.IO_ERROR,
                hint="check the directory and extension in the pattern",
            )
        paths.extend(Path(m) for m in matches)
    return paths


def _write_output(text: str, output: Path | None) -> None:
    if output is None:
        sys.stdout.write(text)
    else:
        try:
            output.write_text(text, encoding="utf-8")
        except OSError as exc:
            raise CliError(
                f"cannot write {output}: {exc}",
                ExitCode.IO_ERROR,
                hint="check that the directory exists and is writable",
            ) from exc
        _ok(f"wrote {output}", err=True)


# ── subcommands ──────────────────────────────────────────────────────────────


def cmd_validate(args: argparse.Namespace) -> int:
    paths = _expand_paths(args.paths)
    policy_failed = False
    if args.policy:
        try:
            policy = load_policy(args.policy)
        except ValueError as exc:
            policy_failed = True
            _fail(f"FAIL {args.policy}:\n{exc}\n")
        else:
            _ok(f"{args.policy} valid ({len(policy.themes)} themes)")

    errors = validate_all(paths)
    for path, exc in errors:
        _fail(f"FAIL {path}:\n{exc}\n")
    summary = f"{len(paths) - len(errors)}/{len(paths)} dataset config files valid"
    if errors:
        _fail(summary, err=False)
        _hint("fix the fields named above; each FAIL block lists the file and field")
    else:
        _ok(summary)
    return ExitCode.INVALID if errors or policy_failed else ExitCode.OK


def cmd_parse(args: argparse.Namespace) -> int:
    selection = resolve_selection(Path(args.datasets_dir), args.specs)
    if args.format == "json":
        docs = []
        for dataset, resources in selection:
            doc = dataset.model_dump(exclude_none=True)
            if resources is not None:
                doc["resources"] = [
                    r for r in doc["resources"] if r["label"] in resources
                ]
            docs.append(doc)
        payload = docs[0] if len(docs) == 1 and args.specs else docs
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return ExitCode.OK

    rows = [("dataset_id", "license", "attribution", "coverage", "download")]
    for dataset, resources in selection:
        for res in dataset.resources:
            if resources is not None and res.label not in resources:
                continue
            lic = res.collection.license
            dd = res.collection.data_download
            rows.append(
                (
                    f"{dataset.provider.label}_{res.label}",
                    (lic.type if lic and lic.type else "") or "-",
                    "yes" if lic and lic.requires_attribution else "no",
                    res.collection.coverage.description
                    or ",".join(a.iso_3166_1 for a in res.collection.coverage.areas),
                    f"{dd.url}{dd.endpoint}" if dd else "-",
                )
            )
    _print_table(rows)
    _out.print(f"\n{len(rows) - 1} resource(s)")
    return ExitCode.OK


def cmd_license(args: argparse.Namespace) -> int:
    selection = resolve_selection(Path(args.datasets_dir), args.specs)
    entries = [
        e
        for dataset, resources in selection
        for e in entries_from_file(dataset, resources)
    ]
    if args.only_required_attribution:
        entries = [e for e in entries if e["requires_attribution"]]
    if not entries:
        raise CliError(
            "no license entries selected",
            ExitCode.IO_ERROR,
            hint="'--only-required-attribution' excluded every resource; "
            "drop the flag or select other datasets"
            if args.only_required_attribution
            else None,
        )

    output = Path(args.output) if args.output else None
    fmt = args.format or (output.suffix.lstrip(".").lower() if output else "md")
    if fmt not in RENDERERS:
        raise CliError(
            f"unsupported format '{fmt}'; choose from {sorted(RENDERERS)}",
            ExitCode.USAGE,
        )
    _write_output(RENDERERS[fmt](entries, title=args.title), output)
    return ExitCode.OK


def cmd_policy_validate(args: argparse.Namespace) -> int:
    try:
        policy = load_policy(args.policy)
    except ValueError as exc:
        _fail(f"FAIL {args.policy}:\n{exc}")
        _hint("expected a JSON object of 'theme' -> ['license ids']")
        return ExitCode.INVALID
    themes = list(policy.themes)
    licenses = sorted({lic for theme in themes for lic in policy.root[theme]})
    rows = [("license", *themes)]
    rows += [
        (
            lic,
            *(
                Text("✓", style="green")
                if lic in policy.root[theme]
                else Text("-", style="dim")
                for theme in themes
            ),
        )
        for lic in licenses
    ]
    _print_table(rows)
    return ExitCode.OK


def cmd_policy_check(args: argparse.Namespace) -> int:
    try:
        policy = load_policy(args.policy)
    except ValueError as exc:
        raise CliError(
            f"{args.policy} failed validation:\n{exc}",
            hint=f"fix the policy file, then re-run: 'policy validate {args.policy}'",
        ) from exc
    selection = resolve_selection(Path(args.datasets_dir), args.specs)
    entries = [
        e
        for dataset, resources in selection
        for e in entries_from_file(dataset, resources)
    ]

    rc = ExitCode.OK
    if args.theme:
        try:
            violations = policy.violations(args.theme, entries)
        except KeyError as exc:
            raise CliError(
                str(exc.args[0]),
                ExitCode.USAGE,
                hint=_did_you_mean(args.theme, policy.themes),
            ) from exc
        for dataset_id, lic in violations:
            _fail(
                f"FAIL {dataset_id}: license '{lic}' not allowed for theme '{args.theme}'"
            )
        summary = f"{len(entries) - len(violations)}/{len(entries)} resources allowed for theme '{args.theme}'"
        if violations:
            rc = ExitCode.POLICY_VIOLATION
            _fail(summary, err=False)
            _hint(
                f"either add the license to '{args.theme}' in the policy file, "
                "or drop those resources from the theme"
            )
        else:
            _ok(summary)

    unlisted = policy.unlisted_licenses(entries)
    for lic, ids in sorted(unlisted.items()):
        _warn(
            f"WARN license '{lic}' is not listed under any theme (used by {', '.join(ids)})"
        )
    if unlisted:
        _hint("add each unlisted license to a theme in the policy file")
    else:
        _ok(f"all licenses used by {len(entries)} resources are listed in policy")
    if unlisted and args.strict:
        rc = ExitCode.POLICY_VIOLATION
    return rc


# ── parser ───────────────────────────────────────────────────────────────────


def _add_selection_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "-d",
        "--datasets-dir",
        required=True,
        metavar="DIR",
        help="directory containing the <provider>.json dataset files",
    )
    p.add_argument(
        "specs",
        nargs="*",
        metavar="PROVIDER[:RESOURCE]",
        help="PROVIDER selects every resource in <provider>.json; PROVIDER:RESOURCE "
        "selects one resource (default: every file in DIR)",
    )


def _command(
    sub,
    screens: dict[tuple[str, ...], argparse.ArgumentParser],
    path: tuple[str, ...],
    summary: str,
    examples: Sequence[str],
) -> argparse.ArgumentParser:
    """Add a subcommand whose --help (and bare-invocation screen) lists examples."""
    p = sub.add_parser(
        path[-1],
        help=summary,
        description=summary[0].upper() + summary[1:] + ".",
        epilog="examples:\n" + "\n".join(f"  {PROG} {e}" for e in examples),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    screens[path] = p
    return p


def build_parser() -> argparse.ArgumentParser:
    screens: dict[tuple[str, ...], argparse.ArgumentParser] = {}
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="Validate dataset config files, inspect them, and render license\nand attribution files from them.",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--color",
        choices=["auto", "always", "never"],
        default="auto",
        help="colorize output; auto honors NO_COLOR/FORCE_COLOR and disables color "
        "when output is not a terminal (default: %(default)s)",
    )
    sub = parser.add_subparsers(dest="command")
    parser.set_defaults(func=cmd_menu, screens=screens)

    p = _command(
        sub,
        screens,
        ("validate",),
        "validate dataset config files (and optionally the license policy)",
        [
            "validate 'path/to/datasets/*.json'",
            "validate 'path/to/datasets/*.json' --policy path/to/license_policy.json",
        ],
    )
    p.add_argument(
        "paths",
        nargs="+",
        metavar="FILE",
        help="<provider>.json files to validate; globs are expanded by the CLI",
    )
    p.add_argument(
        "--policy",
        metavar="FILE",
        help="also check this license policy file: a JSON object mapping each "
        "theme to its list of allowed license IDs, with no duplicates",
    )
    p.set_defaults(func=cmd_validate)

    p = _command(
        sub,
        screens,
        ("parse",),
        "print validated dataset(s) as a table or JSON",
        [
            "parse -d path/to/datasets",
            "parse -d path/to/datasets acme:planet --format json",
        ],
    )
    _add_selection_args(p)
    p.add_argument(
        "--format",
        choices=["json", "table"],
        default="table",
        help="output format (default: %(default)s)",
    )
    p.set_defaults(func=cmd_parse)

    p = _command(
        sub,
        screens,
        ("license",),
        "render a .txt/.md attribution file for the selected datasets",
        [
            "license -d path/to/datasets acme globex -o ATTRIBUTION.md",
            "license -d path/to/datasets --format txt --only-required-attribution",
        ],
    )
    _add_selection_args(p)
    p.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        help="write to FILE instead of stdout; a .md or .txt extension selects the format",
    )
    p.add_argument(
        "--format",
        choices=sorted(RENDERERS),
        help="output format; overrides the --output extension (default: md)",
    )
    p.add_argument(
        "--title",
        default="Data Attribution",
        help="document title (default: %(default)s)",
    )
    p.add_argument(
        "--only-required-attribution",
        action="store_true",
        help="only include resources whose license requires attribution",
    )
    p.set_defaults(func=cmd_license)

    policy = _command(
        sub,
        screens,
        ("policy",),
        "validate a license policy and check datasets against it",
        ["policy validate path/to/license_policy.json"],
    )
    policy_sub = policy.add_subparsers(dest="policy_command", title="commands")

    p = _command(
        policy_sub,
        screens,
        ("policy", "validate"),
        "validate a license policy file and print it",
        ["policy validate path/to/license_policy.json"],
    )
    p.add_argument("policy", metavar="FILE", help="license policy file to validate")
    p.set_defaults(func=cmd_policy_validate)

    p = _command(
        policy_sub,
        screens,
        ("policy", "check"),
        "check selected datasets' licenses against the policy",
        [
            "policy check --policy path/to/license_policy.json -d path/to/datasets",
            "policy check --policy path/to/license_policy.json --theme places "
            "-d path/to/datasets foursquare",
        ],
    )
    p.add_argument(
        "--policy", required=True, metavar="FILE", help="license policy file"
    )
    p.add_argument(
        "--theme",
        help="exit 3 if any selected resource has a license not allowed for this theme",
    )
    p.add_argument(
        "--strict",
        action="store_true",
        help="also exit 3 for licenses that appear under no theme (otherwise a warning)",
    )
    _add_selection_args(p)
    p.set_defaults(func=cmd_policy_check)

    return parser


MENU = [
    ("validate", "check dataset config files (and optionally a license policy)"),
    ("parse", "show validated datasets as a table or JSON"),
    ("license", "write a .md or .txt attribution file for chosen datasets"),
    ("policy validate", "check a license policy file and show what each theme allows"),
    ("policy check", "check datasets' licenses against the policy"),
]

MENU_EXAMPLES = [
    "validate 'path/to/datasets/*.json' --policy path/to/license_policy.json",
    "parse -d path/to/datasets acme:planet",
    "license -d path/to/datasets acme globex -o ATTRIBUTION.md",
    "policy check --policy path/to/license_policy.json --theme places -d path/to/datasets",
]


def cmd_menu(_: argparse.Namespace) -> int:
    """Default screen when no command is given: banner, commands, examples."""
    if _out.is_terminal:
        _out.print(banner.render_banner(), "")
    _out.print(
        "Validate Overture dataset configs and license policy, and generate "
        "license files.\n"
    )
    _out.print("Commands", style="bold")
    width = max(len(name) for name, _summary in MENU)
    for name, summary in MENU:
        _out.print(f"  {name.ljust(width)}  {summary}")
    _out.print("\nExamples", style="bold")
    for example in MENU_EXAMPLES:
        _out.print(f"  {PROG} {example}")
    _out.print(
        f"\nRun '{PROG} <command> --help' for options, or '{PROG} --help' "
        "for exit codes and global flags."
    )
    return ExitCode.OK


def _bare_command_args(
    parser: argparse.ArgumentParser, argv: Sequence[str] | None
) -> list[str]:
    """Turn a command typed with no arguments into ``<command> --help``.

    ``validate`` alone would otherwise fail with a terse "arguments are
    required"; showing the command's help (with examples) is more useful.
    """
    tokens = list(sys.argv[1:] if argv is None else argv)
    words, skip = [], False
    for tok in tokens:
        if skip:
            skip = False
        elif tok == "--color":
            skip = True
        elif not tok.startswith("--color="):
            words.append(tok)
    if tuple(words) in parser.get_default("screens"):
        return [*tokens, "--help"]
    return tokens


def main(argv: Sequence[str] | None = None) -> int:
    # Attribution strings carry "©" and non-ASCII names; Windows consoles
    # default to a legacy code page that would mangle them.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = build_parser()
    args = parser.parse_args(_bare_command_args(parser, argv))
    _configure(args.color)
    try:
        return int(args.func(args))
    except CliError as exc:
        _fail(f"{PROG}: error: {exc}")
        if exc.hint:
            _hint(exc.hint)
        return int(exc.code)


if __name__ == "__main__":
    sys.exit(main())
