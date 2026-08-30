const releaseRules = [
  { type: "feat", release: "minor" },
  { type: "fix", release: "patch" },
  { type: "perf", release: "patch" },
  { type: "refactor", release: "patch" },
  { type: "revert", release: "patch" },
  { type: "docs", release: false },
  { type: "style", release: false },
  { type: "test", release: false },
  { type: "build", release: false },
  { type: "ci", release: false },
  { type: "chore", release: false },
  { breaking: true, release: "major" },
];

module.exports = {
  branches: ["main"],
  tagFormat: "v${version}",
  plugins: [
    [
      "@semantic-release/commit-analyzer",
      { preset: "conventionalcommits", releaseRules },
    ],
    [
      "@semantic-release/release-notes-generator",
      {
        preset: "conventionalcommits",
        presetConfig: {
          types: [
            { type: "feat", section: "Features" },
            { type: "fix", section: "Bug Fixes" },
            { type: "perf", section: "Performance Improvements" },
            { type: "refactor", section: "Code Refactoring" },
            { type: "revert", section: "Reverts" },
            { type: "docs", section: "Documentation" },
            { type: "style", section: "Styles" },
            { type: "test", section: "Tests" },
            { type: "build", section: "Build System" },
            { type: "ci", section: "Continuous Integration" },
            { type: "chore", section: "Chores" },
          ],
        },
      },
    ],
    ["@semantic-release/changelog", { changelogFile: "CHANGELOG.md" }],
    [
      "@semantic-release/exec",
      { prepareCmd: "scripts/prepare-release.sh ${nextRelease.version}" },
    ],
    [
      "@semantic-release/git",
      {
        assets: ["CHANGELOG.md", "VERSION", "Cargo.toml", "Cargo.lock"],
        message:
          "chore(release): ${nextRelease.version} [skip ci]\n\n${nextRelease.notes}",
      },
    ],
    [
      "@semantic-release/github",
      {
        assets: [
          { path: "dist/*-x86_64-unknown-linux-gnu.tar.gz", label: "Linux x86-64 GNU" },
          { path: "dist/*-x86_64-unknown-linux-musl.tar.gz", label: "Linux x86-64 musl (static)" },
          { path: "dist/SHA256SUMS", label: "SHA-256 checksums" },
        ],
      },
    ],
  ],
};
