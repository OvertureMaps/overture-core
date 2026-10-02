# overture_core.dataset

Parsing, validating and rendering provider/resource dataset config files, plus the `overture-datasets` CLI. Nothing here assumes where the config files live; callers always pass paths or directories explicitly.

`dataset/__init__.py` is intentionally empty. Import the submodule you need so importing one doesn't pull in the others.

## Modules

| Module | Scope |
| --- | --- |
| `dataset` | Parsing a provider/resource JSON config into its collection/ingestion/matching sections. |
| `schema` | Validating provider/resource JSON configs, including as a standalone CI check. |
| `attribution` | Flattening provider/resource configs into license entries and rendering them as Markdown or plain text. |
| `license_policy` | Validating and querying the per-theme license allowlist. |
| `cli` | The `overture-datasets` command-line interface over the modules above. |

## CLI

`overture-datasets` validates, inspects and renders dataset configs. Commands that select datasets (`parse`, `license`, `policy check`) take the datasets directory explicitly with `-d`; `validate` takes file paths or globs, and `policy validate` takes a policy file. A dataset *spec* is `provider` (every resource in that file) or `provider:resource`; with no specs, every `*.json` in the directory is used.

```sh
# from packages/overture_core, after `uv sync`
uv run overture-datasets validate 'path/to/datasets/*.json' --policy path/to/license_policy.json
uv run overture-datasets parse -d path/to/datasets acme:planet --format table
uv run overture-datasets license -d path/to/datasets acme globex -o ATTRIBUTION.md
uv run overture-datasets license -d path/to/datasets acme --format txt --only-required-attribution
uv run overture-datasets policy validate path/to/license_policy.json
uv run overture-datasets policy check --policy path/to/license_policy.json --theme base -d path/to/datasets
```

- `license` infers `--format` (`md` or `txt`) from the `-o` extension and writes to stdout without `-o`.
- `policy check` fails when a selected resource's license isn't allowed for `--theme`. Licenses listed under no theme are warnings, or failures with `--strict`.
- Running `overture-datasets` with no command prints a banner and a menu of commands and examples. The banner shows only on a terminal, or with `--color always`.
- Success lines are green, failures red, warnings yellow and `hint:` lines cyan. Control this with `--color auto|always|never`; `auto` honors `NO_COLOR` and `FORCE_COLOR` and is off when output is piped. Output is rendered with [Rich](https://github.com/Textualize/rich). JSON and rendered license files are never colored.
- Failures that have an obvious next step print a `hint:` line, such as a "did you mean" for a mistyped provider, resource or theme.
- Exit status:

  | Code | Meaning |
  | --- | --- |
  | 0 | Success |
  | 1 | A dataset config or policy file failed validation |
  | 2 | Invalid arguments, spec, format or theme |
  | 3 | License policy violation, or unlisted licenses with `--strict` |
  | 4 | Missing, unreadable or unwritable file, or nothing matched |

Run `uv run overture-datasets <command> --help` for every option.

## License entries

`attribution.license_entry` is the single definition of the flat license-entry dict that `artifacts.LicenseArtifact` writes to `license.json` and that the CLI renders. `render_markdown` and `render_text` are the CLI's renderers; they group by provider and collapse identical bullets. They are separate from `artifacts.render_attribution_mdx`, which groups by theme.
