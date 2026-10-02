"""Tests for the ``overture-datasets`` CLI."""

import json

import pytest

from overture_core.dataset import banner, cli
from overture_core.dataset.cli import (
    CliError,
    ExitCode,
    main,
    parse_spec,
    resolve_selection,
)


@pytest.fixture(autouse=True)
def fake_spdx(monkeypatch):
    monkeypatch.setattr(
        cli, "license_texts", lambda ids: {i: f"TEXT {i}" for i in sorted(ids)}
    )


def run(capsys, *argv) -> tuple[int, str, str]:
    rc = main([str(a) for a in argv])
    captured = capsys.readouterr()
    return rc, captured.out, captured.err


class TestSpecs:
    @pytest.mark.parametrize(
        "spec, expected",
        [("acme", ("acme", None)), ("acme:planet", ("acme", "planet"))],
    )
    def test_parse(self, spec, expected):
        assert parse_spec(spec) == expected

    @pytest.mark.parametrize(
        "spec",
        ["", ":planet", "acme:", "a:b:c", "../acme", "/tmp/acme", "Acme", "acme:../x"],
    )
    def test_invalid(self, spec):
        with pytest.raises(CliError, match="invalid dataset spec"):
            parse_spec(spec)

    def test_merges_specs_per_provider(self, datasets_dir):
        sel = resolve_selection(
            datasets_dir, ["acme:planet", "acme:coastlines", "globex"]
        )
        assert [(d.provider.label, r) for d, r in sel] == [
            ("acme", {"planet", "coastlines"}),
            ("globex", None),
        ]

    def test_whole_provider_wins_over_resource(self, datasets_dir):
        sel = resolve_selection(datasets_dir, ["acme:planet", "acme"])
        assert sel[0][1] is None
        sel = resolve_selection(datasets_dir, ["acme", "acme:planet"])
        assert sel[0][1] is None

    def test_no_specs_selects_all(self, datasets_dir):
        sel = resolve_selection(datasets_dir, [])
        assert [d.provider.label for d, _ in sel] == ["acme", "globex", "nolic"]

    def test_errors(self, datasets_dir, tmp_path):
        with pytest.raises(CliError, match="directory not found"):
            resolve_selection(tmp_path / "missing", [])
        empty = tmp_path / "empty"
        empty.mkdir()
        with pytest.raises(CliError, match="no \\*.json"):
            resolve_selection(empty, [])
        with pytest.raises(CliError, match="no dataset file for provider 'zzz'"):
            resolve_selection(datasets_dir, ["zzz"])
        with pytest.raises(CliError, match="not found in provider 'acme'"):
            resolve_selection(datasets_dir, ["acme:nope"])

    def test_invalid_file_surfaces_as_cli_error(self, datasets_dir):
        (datasets_dir / "bad.json").write_text("{", encoding="utf-8")
        with pytest.raises(CliError, match="bad.json failed validation"):
            resolve_selection(datasets_dir, ["bad"])


class TestValidate:
    def test_passes_with_glob_and_policy(self, capsys, datasets_dir, policy_path):
        rc, out, _ = run(
            capsys, "validate", f"{datasets_dir}/*.json", "--policy", policy_path
        )
        assert rc == 0
        assert "3/3 dataset config files valid" in out
        assert "valid (2 themes)" in out

    def test_fails_on_bad_file(self, capsys, datasets_dir):
        bad = datasets_dir / "bad.json"
        bad.write_text("{", encoding="utf-8")
        rc, out, err = run(capsys, "validate", datasets_dir / "acme.json", bad)
        assert rc == ExitCode.INVALID
        assert "1/2 dataset config files valid" in out
        assert "FAIL" in err and "bad.json" in err

    def test_fails_on_bad_policy(self, capsys, datasets_dir, tmp_path):
        p = tmp_path / "p.json"
        p.write_text("[]", encoding="utf-8")
        rc, _, err = run(capsys, "validate", datasets_dir / "acme.json", "--policy", p)
        assert rc == ExitCode.INVALID
        assert "FAIL" in err

    def test_glob_with_no_matches(self, capsys, tmp_path):
        rc, _, err = run(capsys, "validate", f"{tmp_path}/*.json")
        assert rc == ExitCode.IO_ERROR
        assert "no files match" in err


def table_rows(out: str) -> list[list[str]]:
    """Cell values of each data/header row of a boxed Rich table."""
    return [
        # Rich draws header cells with "┃" on UTF-8 terminals and "│" elsewhere.
        [c.strip() for c in line.strip("│┃ ").replace("┃", "│").split("│")]
        for line in out.splitlines()
        if line.startswith(("│", "┃"))
    ]


class TestParse:
    def test_table(self, capsys, datasets_dir, monkeypatch):
        monkeypatch.setenv("COLUMNS", "200")  # piped output defaults to 80
        rc, out, _ = run(capsys, "parse", "-d", datasets_dir, "acme", "globex")
        assert rc == 0
        rows = table_rows(out)
        assert rows[0] == [
            "dataset_id",
            "license",
            "attribution",
            "coverage",
            "download",
        ]
        assert any(r[0] == "acme_planet" and r[-1] == "s3://acme/planet/" for r in rows)
        # No description -> falls back to ISO codes; no download -> "-".
        assert any(
            r[0] == "globex_places" and r[3] == "US,CA" and r[-1] == "-" for r in rows
        )

    def test_json_single_resource(self, capsys, datasets_dir):
        rc, out, _ = run(
            capsys, "parse", "-d", datasets_dir, "--format", "json", "acme:planet"
        )
        assert rc == 0
        doc = json.loads(out)
        assert doc["provider"]["label"] == "acme"
        assert [r["label"] for r in doc["resources"]] == ["planet"]

    def test_json_all_is_list(self, capsys, datasets_dir):
        rc, out, _ = run(capsys, "parse", "-d", datasets_dir, "--format", "json")
        assert rc == 0
        assert [d["provider"]["label"] for d in json.loads(out)] == [
            "acme",
            "globex",
            "nolic",
        ]


class TestLicense:
    def test_markdown_to_stdout(self, capsys, datasets_dir):
        rc, out, _ = run(capsys, "license", "-d", datasets_dir, "acme", "globex")
        assert rc == 0
        assert out.startswith("# Data Attribution\n")
        assert "## Acme Maps" in out and "## Globex" in out

    def test_format_from_output_extension(self, capsys, datasets_dir, tmp_path):
        target = tmp_path / "LICENSE.txt"
        rc, out, err = run(
            capsys,
            "license",
            "-d",
            datasets_dir,
            "-o",
            target,
            "--title",
            "T",
            "acme:planet",
        )
        assert rc == 0 and out == ""
        assert f"wrote {target}" in err
        assert target.read_text(encoding="utf-8").startswith("T\n=\n")

    def test_explicit_format_overrides_extension(self, capsys, datasets_dir, tmp_path):
        target = tmp_path / "out.dat"
        rc, _, _ = run(
            capsys,
            "license",
            "-d",
            datasets_dir,
            "-o",
            target,
            "--format",
            "md",
            "acme",
        )
        assert rc == 0
        assert target.read_text(encoding="utf-8").startswith("# ")

    def test_unknown_extension(self, capsys, datasets_dir, tmp_path):
        rc, _, err = run(
            capsys, "license", "-d", datasets_dir, "-o", tmp_path / "out.pdf", "acme"
        )
        assert rc == ExitCode.USAGE
        assert "unsupported format 'pdf'" in err

    def test_only_required_attribution(self, capsys, datasets_dir):
        rc, out, _ = run(
            capsys, "license", "-d", datasets_dir, "--only-required-attribution"
        )
        assert rc == 0
        assert "## Acme Maps" in out
        assert "Globex" not in out and "Stuff" not in out

    def test_default_includes_license_texts(self, capsys, datasets_dir):
        rc, out, _ = run(capsys, "license", "-d", datasets_dir)
        assert rc == 0
        assert "## License Texts" in out
        assert out.count("### ODbL-1.0") == 1 and "TEXT ODbL-1.0" in out

    def test_opt_outs(self, capsys, datasets_dir):
        rc, out, _ = run(capsys, "license", "-d", datasets_dir, "--no-license-texts")
        assert rc == 0 and "License Texts" not in out and "## Acme Maps" in out
        rc, out, _ = run(capsys, "license", "-d", datasets_dir, "--no-attribution")
        assert rc == 0 and "Available under" not in out and "TEXT ODbL-1.0" in out

    def test_all_parts_off(self, capsys, datasets_dir):
        rc, _, err = run(
            capsys,
            "license",
            "-d",
            datasets_dir,
            "--no-attribution",
            "--no-notices",
            "--no-license-texts",
        )
        assert rc == ExitCode.USAGE and "nothing to render" in err

    def test_missing_license_id_fails(self, capsys, datasets_dir, monkeypatch):
        def boom(ids):
            raise cli.LicenseTextError("license id Nope-1.0 not found")

        monkeypatch.setattr(cli, "license_texts", boom)
        rc, _, err = run(capsys, "license", "-d", datasets_dir)
        assert rc == ExitCode.IO_ERROR and "Nope-1.0" in err

    def test_nothing_selected(self, capsys, datasets_dir):
        rc, _, err = run(
            capsys,
            "license",
            "-d",
            datasets_dir,
            "--only-required-attribution",
            "globex",
        )
        assert rc == ExitCode.IO_ERROR
        assert "no license entries selected" in err


class TestValidateLicenseUrlWarning:
    def test_warns_when_url_missing(self, capsys, datasets_dir, tmp_path):
        doc = json.loads((datasets_dir / "acme.json").read_text(encoding="utf-8"))
        for res in doc["resources"]:
            res["collection"]["license"]["url"]["primary"] = ""
        target = tmp_path / "acme.json"
        target.write_text(json.dumps(doc), encoding="utf-8")
        rc, out, err = run(capsys, "validate", target)
        assert rc == 0
        assert "https://spdx.org/licenses/ODbL-1.0.html" in out + err

    def test_no_warning_when_url_present(self, capsys, datasets_dir):
        rc, out, err = run(capsys, "validate", datasets_dir / "acme.json")
        assert rc == 0 and "no license URL" not in out + err


class TestPolicy:
    def test_validate_prints_themes(self, capsys, policy_path):
        rc, out, _ = run(capsys, "policy", "validate", policy_path)
        assert rc == 0
        rows = table_rows(out)
        assert rows[0] == ["license", "base", "places"]
        assert rows[1:] == [
            ["CC-BY-4.0", "✓", "-"],
            ["CC0-1.0", "-", "✓"],
            ["CDLA-Permissive-2.0", "-", "✓"],
            ["ODbL-1.0", "✓", "-"],
        ]

    def test_validate_bad(self, capsys, tmp_path):
        p = tmp_path / "p.json"
        p.write_text("[]", encoding="utf-8")
        rc, _, err = run(capsys, "policy", "validate", p)
        assert rc == ExitCode.INVALID
        assert "FAIL" in err

    def test_validate_unreadable_is_io_error(self, capsys, tmp_path):
        rc, _, err = run(capsys, "policy", "validate", tmp_path / "nope.json")
        assert rc == ExitCode.IO_ERROR
        assert "cannot read" in err

    def test_check_theme_passes(self, capsys, datasets_dir, policy_path):
        rc, out, err = run(
            capsys,
            "policy",
            "check",
            "--policy",
            policy_path,
            "--theme",
            "base",
            "-d",
            datasets_dir,
            "acme",
        )
        assert rc == 0
        assert "2/2 resources allowed for theme 'base'" in out
        assert err == ""

    def test_check_theme_fails(self, capsys, datasets_dir, policy_path):
        rc, out, err = run(
            capsys,
            "policy",
            "check",
            "--policy",
            policy_path,
            "--theme",
            "places",
            "-d",
            datasets_dir,
        )
        assert rc == ExitCode.POLICY_VIOLATION
        assert "1/4 resources allowed for theme 'places'" in out
        assert (
            "FAIL acme_planet: license 'ODbL-1.0' not allowed for theme 'places'" in err
        )
        assert "FAIL nolic_stuff: license '' not allowed" in err

    def test_check_unknown_theme(self, capsys, datasets_dir, policy_path):
        rc, _, err = run(
            capsys,
            "policy",
            "check",
            "--policy",
            policy_path,
            "--theme",
            "nope",
            "-d",
            datasets_dir,
        )
        assert rc == ExitCode.USAGE
        assert "unknown theme 'nope'" in err

    def test_check_unlisted_warns_and_strict_fails(
        self, capsys, datasets_dir, tmp_path
    ):
        p = tmp_path / "p.json"
        p.write_text(json.dumps({"base": ["ODbL-1.0"]}), encoding="utf-8")
        rc, _, err = run(capsys, "policy", "check", "--policy", p, "-d", datasets_dir)
        assert rc == 0
        assert (
            "WARN license 'CDLA-Permissive-2.0' is not listed under any theme (used by globex_places)"
            in err
        )
        rc, _, _ = run(
            capsys, "policy", "check", "--policy", p, "--strict", "-d", datasets_dir
        )
        assert rc == ExitCode.POLICY_VIOLATION

    def test_check_bad_policy(self, capsys, datasets_dir, tmp_path):
        rc, _, err = run(
            capsys,
            "policy",
            "check",
            "--policy",
            tmp_path / "nope.json",
            "-d",
            datasets_dir,
        )
        assert rc == ExitCode.IO_ERROR
        assert "cannot read" in err

    def test_check_invalid_policy(self, capsys, datasets_dir, tmp_path):
        p = tmp_path / "p.json"
        p.write_text("[]", encoding="utf-8")
        rc, _, err = run(capsys, "policy", "check", "--policy", p, "-d", datasets_dir)
        assert rc == ExitCode.INVALID
        assert "failed validation" in err


class TestExitCodes:
    def test_codes_are_distinct_and_stable(self):
        assert {c.name: int(c) for c in ExitCode} == {
            "OK": 0,
            "INVALID": 1,
            "USAGE": 2,
            "POLICY_VIOLATION": 3,
            "IO_ERROR": 4,
        }

    def test_argparse_usage_error_exits_2(self, capsys):
        with pytest.raises(SystemExit) as exc:
            main(["validate", "--policy"])
        assert exc.value.code == ExitCode.USAGE

    def test_invalid_spec_is_usage_error(self, capsys, datasets_dir):
        rc, _, err = run(capsys, "parse", "-d", datasets_dir, "a:b:c")
        assert rc == ExitCode.USAGE
        assert "invalid dataset spec" in err

    def test_missing_file_is_io_error(self, capsys, tmp_path):
        rc, _, err = run(capsys, "validate", tmp_path / "nope.json")
        assert rc == ExitCode.IO_ERROR
        assert "file not found" in err

    def test_missing_datasets_dir_is_io_error(self, capsys, tmp_path):
        rc, _, _ = run(capsys, "parse", "-d", tmp_path / "nope")
        assert rc == ExitCode.IO_ERROR

    def test_unwritable_output_is_io_error(self, capsys, datasets_dir, tmp_path):
        rc, _, err = run(
            capsys, "license", "-d", datasets_dir, "-o", tmp_path / "no" / "x.md"
        )
        assert rc == ExitCode.IO_ERROR
        assert "cannot write" in err

    def test_invalid_dataset_file_is_invalid(self, capsys, datasets_dir):
        (datasets_dir / "bad.json").write_text("{", encoding="utf-8")
        rc, _, _ = run(capsys, "parse", "-d", datasets_dir, "bad")
        assert rc == ExitCode.INVALID


class TestColor:
    GREEN = "\033[32m"

    def test_always_colors_success_green(self, capsys, datasets_dir):
        rc, out, _ = run(
            capsys, "--color", "always", "validate", datasets_dir / "acme.json"
        )
        assert rc == ExitCode.OK
        assert f"{self.GREEN}" in out and "dataset config files valid" in out

    def test_always_colors_failures_red_and_warnings_yellow(
        self, capsys, datasets_dir, tmp_path
    ):
        bad = tmp_path / "bad.json"
        bad.write_text("{", encoding="utf-8")
        _, out, err = run(capsys, "--color", "always", "validate", bad)
        assert "\033[31m" in out
        assert "\033[31mFAIL" in err

        policy = tmp_path / "p.json"
        policy.write_text(json.dumps({"base": ["ODbL-1.0"]}), encoding="utf-8")
        _, _, err = run(
            capsys, "--color", "always", "policy", "check", "--policy", policy,
            "-d", datasets_dir,
        )  # fmt: skip
        assert "\033[33mWARN" in err

    def test_never_emits_no_escapes(self, capsys, datasets_dir):
        _, out, _ = run(
            capsys, "--color", "never", "validate", datasets_dir / "acme.json"
        )
        assert "\033" not in out


class TestHints:
    def test_typo_provider_suggests_match(self, capsys, datasets_dir):
        rc, _, err = run(capsys, "parse", "-d", datasets_dir, "acne")
        assert rc == ExitCode.IO_ERROR
        assert "hint: did you mean 'acme'?" in err

    def test_typo_resource_suggests_match(self, capsys, datasets_dir):
        rc, _, err = run(capsys, "parse", "-d", datasets_dir, "acme:plnet")
        assert rc == ExitCode.IO_ERROR
        assert "hint: did you mean 'planet'?" in err

    def test_typo_theme_suggests_match(self, capsys, datasets_dir, policy_path):
        rc, _, err = run(
            capsys,
            "policy",
            "check",
            "--policy",
            policy_path,
            "--theme",
            "plac",
            "-d",
            datasets_dir,
        )
        assert rc == ExitCode.USAGE
        assert "hint: did you mean 'places'?" in err

    def test_violation_hint(self, capsys, datasets_dir, policy_path):
        rc, _, err = run(
            capsys,
            "policy",
            "check",
            "--policy",
            policy_path,
            "--theme",
            "places",
            "-d",
            datasets_dir,
            "acme",
        )
        assert rc == ExitCode.POLICY_VIOLATION
        assert "hint: either add the license to 'places'" in err

    def test_missing_dir_hint(self, capsys, tmp_path):
        rc, _, err = run(capsys, "parse", "-d", tmp_path / "nope")
        assert rc == ExitCode.IO_ERROR
        assert "hint: check the '-d/--datasets-dir' path" in err


class TestMenu:
    def test_no_command_shows_menu_and_succeeds(self, capsys):
        rc, out, _ = run(capsys)
        assert rc == ExitCode.OK
        for name, _summary in cli.MENU:
            assert name in out
        assert "Examples" in out
        assert "\033" not in out

    def test_menu_covers_every_command(self):
        parser = cli.build_parser()
        top = {a.dest for a in parser._subparsers._group_actions[0]._choices_actions}
        assert top <= {name.split()[0] for name, _summary in cli.MENU}

    def test_banner_shown_with_color_always(self, capsys):
        rc, out, _ = run(capsys, "--color", "always")
        assert rc == ExitCode.OK
        assert "\033[" in out

    def test_banner_renders_gradient(self):
        text = banner.render_banner()
        assert "█" in text.plain
        colors = {s.style.color.triplet for s in text.spans if s.style.color}
        assert len(colors) > 10  # a gradient, not a flat color
