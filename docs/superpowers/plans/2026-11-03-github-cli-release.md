# GitHub CLI migration implementation plan

> Execute inline with Superpowers and ub-mode. No commits, pushes, or remote releases.

**Goal:** replace the legacy CLI with the current client and provide manually triggered multi-platform builds and optional release publication.

**Architecture:** GitHub is the primary repository. Copy the existing client and tests, changing only the Go module and internal imports. GitHub Actions tests on three operating systems, then cross-compiles six CGO-free binaries. Publication creates a draft with all assets before making it public.

**Constraints:** keep Go 1.27 and websocket v1.8.15; preserve runtime behavior; keep the original client untouched; use only workflow_dispatch; default to build-only; never overwrite an existing release or tag.

## Tasks

- [x] Copy cmd, internal, module files, and public contract documents. Remove legacy implementation. Change module to github.com/adal-cloud/adal-cli. Run existing tests, vet, formatting, and build checks.
- [x] Add tested release packaging: validate versions, build Linux/Windows/macOS for amd64/arm64, include public documentation and MIT license, retain Unix executable permissions, and generate SHA-256 checksums.
- [x] Replace release workflow: validate inputs, test natively on three OSes, build archives, upload artifacts, optionally create and publish a release for the exact workflow commit. Document usage and failure recovery.
- [x] Verify actual six-target packaging, native Windows/Linux checks, workflow lint, and final read-only review. Record limits of local verification.

## Review focus

Invalid versions and shell metacharacters; duplicate release/tag; an existing output directory; executable permissions after extraction; build/upload failures leaving no public partial release; a selected branch moving after dispatch.

## Evidence and decisions

- Both repositories were clean before work. The original client passed go test ./....
- The local clone used jmelnikov/adal-cli, with a different history. Corrected origin to adal-cloud/adal-cli and aligned the clean local branch with origin/main (3a2e736). Previous local HEAD was 2c46a5e4de408f5c786f6c622210a923e6a6add0 and remains recoverable through the reflog.
- The legacy workflow built the root package, ran on tag pushes, and lacked an explicit tag for manual branch releases. The current client entry point did not exist in the legacy checkout.
- Reuse the destination repository's MIT license and security policy. Keep public CLI contract documentation in its existing English language.
- Migration equivalence: all 33 Go files match the original after module/import replacement and gofmt; go.sum is byte-identical. The original repository remains clean. No application behavior or websocket dependency changed.
- Formatting: normalized existing unformatted source files in the destination so the new CI formatting gate can pass. Added LF attributes for Windows checkouts.
- Packaging TDD: first observed unittest failure because build_release did not exist; implemented packaging and observed all three tests pass. Tests exercise version boundaries, tar executable permission, ZIP contents, hashes, and preservation of an existing output directory.
- Windows (Go 1.27.0) and Linux/WSL (Go 1.27.3): go test ./..., go vet ./..., go build ./... passed. gofmt -l cmd internal returned no files.
- go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.7 .github/workflows/release.yml passed. git diff --check passed.
- Actual local packaging with v1.0.0-rc.1 produced all six archives and checksums. Independently verified exact archive members, each SHA-256, commit/version/target metadata, Unix mode 0755, and ELF/PE/Mach-O architecture formats. Extracted Windows amd64 and Linux amd64 binaries passed --help smoke checks.
- Independent read-only review found no substantive issues. It compared the original client and reviewed workflow and packaging semantics; its tool execution was limited, so runtime verification is provided by the runs above.
- Publication creates the tag atomically through the GitHub API before using gh release create --verify-tag --draft. A competing tag creation fails rather than releasing another commit. A failed publication may leave an unused tag or draft; recovery is documented.
- Limits: no remote workflow, upload, tag creation, or release publication was run. Native macOS execution and ARM64 execution were not available locally. Native OS CI jobs and repository policy compatibility require the first manual GitHub run after push. Local archives contain uncommitted migration code and are verification outputs, not official releases.
- No commits or pushes were made. The Git index remains empty.
