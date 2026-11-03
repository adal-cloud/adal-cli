# Manual builds and releases

The workflow is `.github/workflows/release.yml`. It only handles `workflow_dispatch`.
Pushes, tag creation, and pull requests do not start builds or publication.
GitHub must have the workflow on the default branch before the Run workflow button is available.

## Run a build

1. Open this repository on GitHub. Select **Actions → Release → Run workflow**.
2. Select the branch or ref to build. The workflow uses the exact commit selected at dispatch, even if the branch moves later.
3. Enter a version such as `v1.0.0`. Supported prereleases are `v1.0.0-alpha.1`, `v1.0.0-beta.1`, and `v1.0.0-rc.1`. Numeric components cannot have leading zeroes.
4. Leave **Publish a GitHub Release** unchecked for a build-only run. This is the default.
5. Run the workflow. Download `adal-cli-VERSION` from the run's **Artifacts** section after success. Build-only runs do not create a tag or release. Artifacts are retained for 14 days.

The workflow checks formatting, runs `go test ./...`, `go vet ./...`, and `go build ./...` on Linux, Windows, and macOS. It then tests the packaging script and cross-compiles with `CGO_ENABLED=0` on Linux.
Go is selected from `go.mod`. The websocket dependency is unchanged.

| System  | Architectures | Archive                               | Executable     |
|---------|---------------|---------------------------------------|----------------|
| Linux   | amd64, arm64  | `adal-cli-VERSION-linux-ARCH.tar.gz`  | `adal-cli`     |
| Windows | amd64, arm64  | `adal-cli-VERSION-windows-ARCH.zip`   | `adal-cli.exe` |
| macOS   | amd64, arm64  | `adal-cli-VERSION-darwin-ARCH.tar.gz` | `adal-cli`     |

Every archive includes `README.md`, `LICENSE`, `SECURITY.md`, and `build-info.json`.
The metadata records the version, complete source commit, OS, and architecture.
`checksums.txt` contains the SHA-256 hash of all six archives.
Unix archives preserve executable permissions.
Native CI tests cover one runner architecture per OS. They do not execute all six target binaries.
The binaries are not code-signed or notarized.

## Publish a release

Run the same workflow with a new version and **Publish a GitHub Release** checked.
Do not create the version tag beforehand.
Use a new version for the current protocol; the old releases remain historical downloads.

Publication runs only after all checks, builds, and artifact upload succeed.
It verifies checksums, creates a tag for the exact built commit, and creates a draft release with the seven files.
It publishes the draft only after all files are uploaded.
Versions with an alpha, beta, or rc suffix are marked as prereleases.
The workflow does not overwrite an existing tag or release.
Runs for the same version are serialized.

Only the publish job has `contents: write`. All other jobs have `contents: read`.
The built-in `GITHUB_TOKEN` is sufficient. No personal access token or repository secret is required.
Repository or organization policies must allow GitHub Actions, the referenced actions, and release/tag creation by that token.
Tag rulesets may need an appropriate exception for the release workflow.

If publication fails, inspect the run and any draft release before retrying.
A failure after tag creation can leave a tag. A failure during upload can leave a draft with partial assets.
The workflow deliberately does not delete, replace, or resume those objects.
Choose a new version, or remove only the failed draft and its unused tag after checking them.
Deleting a draft alone does not delete its tag.
Build-only runs can reuse a version because they do not change releases.

## Verify a download

On Linux, download the archive and `checksums.txt` into the same directory:

```sh
sha256sum --check --ignore-missing checksums.txt
```

On macOS, compare `shasum -a 256 ARCHIVE` with its line in `checksums.txt`.
In PowerShell, compare `(Get-FileHash .\ARCHIVE -Algorithm SHA256).Hash` with the matching line.

## Build locally

Go 1.27, Git, and Python 3.10 or newer are required. No third-party Python packages are needed.
From the repository root:

```sh
python3 -m unittest discover -s scripts -v
python3 scripts/build_release.py v1.0.0
```

On Windows, use `python` if the interpreter is named `python`.
The script builds all six targets into `dist`. This directory must not exist before the build.
Use `--output PATH` to select another new directory.
The script never deletes an existing output directory.
It does not run the Go tests or publish anything.
Local `build-info.json` records HEAD; uncommitted edits are not represented by that commit.
Only clean CI builds should be published as official releases.
