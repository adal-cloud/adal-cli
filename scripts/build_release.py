"""Build the public release archives using Go and the Python standard library."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import zipfile


ROOT = Path(__file__).resolve().parent.parent
TARGETS = [(system, arch) for system in ("linux", "windows", "darwin") for arch in ("amd64", "arm64")]
DOCUMENTS = ("README.md", "LICENSE", "SECURITY.md")


def validate_version(version):
    number = r"(?:0|[1-9][0-9]*)"
    if not re.fullmatch(rf"v{number}\.{number}\.{number}(?:-(?:alpha|beta|rc)\.{number})?", version):
        raise ValueError("Use vMAJOR.MINOR.PATCH, optionally followed by -alpha.N, -beta.N, or -rc.N")
    return version


def pack(package, binary, archive):
    files = [binary, *(package / name for name in DOCUMENTS), package / "build-info.json"]
    if archive.suffix == ".zip":
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as output:
            for path in files:
                output.write(path, path.name)
    else:
        with tarfile.open(archive, "w:gz") as output:
            for path in files:
                info = output.gettarinfo(path, path.name)
                # A Windows checkout does not carry Unix executable permissions.
                info.mode = 0o755 if path == binary else 0o644
                with path.open("rb") as source:
                    output.addfile(info, source)


def write_checksums(output):
    archives = sorted([*output.glob("*.tar.gz"), *output.glob("*.zip")])
    lines = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in archives]
    (output / "checksums.txt").write_text("".join(lines), encoding="utf-8", newline="\n")


def build(version, output):
    validate_version(version)
    # Refuse stale assets instead of publishing a mixture of two builds.
    output.mkdir(parents=True, exist_ok=False)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    with tempfile.TemporaryDirectory(prefix="adal-cli-release-") as directory:
        package = Path(directory)
        for name in DOCUMENTS:
            shutil.copyfile(ROOT / name, package / name)
        for system, arch in TARGETS:
            binary = package / ("adal-cli.exe" if system == "windows" else "adal-cli")
            env = dict(os.environ, GOOS=system, GOARCH=arch, CGO_ENABLED="0")
            subprocess.run(
                ["go", "build", "-trimpath", "-ldflags=-s -w", "-o", str(binary), "./cmd/cli"],
                cwd=ROOT, env=env, check=True,
            )
            metadata = {"version": version, "commit": commit, "os": system, "arch": arch}
            (package / "build-info.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
            extension = "zip" if system == "windows" else "tar.gz"
            archive = output / f"adal-cli-{version}-{system}-{arch}.{extension}"
            pack(package, binary, archive)
            print(archive.name, flush=True)
    write_checksums(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", type=validate_version)
    parser.add_argument("--output", type=Path, default=ROOT / "dist")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    if not args.validate_only:
        build(args.version, args.output.resolve())


if __name__ == "__main__":
    main()
