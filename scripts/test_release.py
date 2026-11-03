import hashlib
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
import zipfile

import build_release


class ReleaseTests(unittest.TestCase):
    def test_versions(self):
        for version in ("v0.0.0", "v1.2.3", "v12.3.4-rc.1", "v1.0.0-beta.0", "v1.0.0-alpha.2"):
            with self.subTest(version=version):
                self.assertEqual(build_release.validate_version(version), version)
        for version in ("", "main", "1.2.3", "v01.2.3", "v1.2", "v1.2.3-rc.01", "../v1.2.3", "v1.2.3\n", "v1.2.3;echo bad"):
            with self.subTest(version=version), self.assertRaises(ValueError):
                build_release.validate_version(version)

    def test_archives_and_checksums(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "package"
            package.mkdir()
            for name in ("README.md", "LICENSE", "SECURITY.md"):
                (package / name).write_text(name, encoding="utf-8")
            (package / "build-info.json").write_text(json.dumps({"version": "v1.2.3", "commit": "a" * 40}), encoding="utf-8")
            binary = package / "adal-cli"
            binary.write_bytes(b"example binary")
            binary.chmod(0o644)
            archive = root / "linux.tar.gz"
            build_release.pack(package, binary, archive)
            with tarfile.open(archive) as contents:
                self.assertEqual(set(contents.getnames()), {"adal-cli", "README.md", "LICENSE", "SECURITY.md", "build-info.json"})
                self.assertEqual(contents.getmember("adal-cli").mode & 0o777, 0o755)
                self.assertEqual(contents.extractfile("adal-cli").read(), b"example binary")
                self.assertEqual(json.load(contents.extractfile("build-info.json"))["version"], "v1.2.3")
            binary.rename(package / "adal-cli.exe")
            archive = root / "windows.zip"
            build_release.pack(package, package / "adal-cli.exe", archive)
            with zipfile.ZipFile(archive) as contents:
                self.assertEqual(set(contents.namelist()), {"adal-cli.exe", "README.md", "LICENSE", "SECURITY.md", "build-info.json"})
                self.assertEqual(contents.read("adal-cli.exe"), b"example binary")
            build_release.write_checksums(root)
            lines = (root / "checksums.txt").read_text().splitlines()
            self.assertEqual(len(lines), 2)
            for line in lines:
                digest, name = line.split("  ")
                self.assertEqual(digest, hashlib.sha256((root / name).read_bytes()).hexdigest())

    def test_output_must_be_new(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "dist"
            output.mkdir()
            marker = output / "keep.txt"
            marker.write_text("keep", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                build_release.build("v1.2.3", output)
            self.assertEqual(marker.read_text(), "keep")


if __name__ == "__main__":
    unittest.main()
