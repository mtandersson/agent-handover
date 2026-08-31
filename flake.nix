{
  description = "Reproducible development and release builds for agent-handover";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane/v0.24.0";
  };

  outputs = { self, nixpkgs, crane }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      version = pkgs.lib.removeSuffix "\n" (builtins.readFile ./VERSION);
      craneLib = crane.mkLib pkgs;
      cargoSource = craneLib.cleanCargoSource ./.;
      commonArgs = {
        pname = "agent-handover";
        inherit version;
        src = cargoSource;
        strictDeps = true;
      };
      cargoArtifacts = craneLib.buildDepsOnly (commonArgs // {
        cargoExtraArgs = "--all-features";
      });
      gnu = craneLib.buildPackage (commonArgs // {
        inherit cargoArtifacts;
        doCheck = false;
        nativeBuildInputs = [ pkgs.patchelf ];
        postFixup = ''
          patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 \
            --remove-rpath "$out/bin/agent-handover"
        '';
      });
      staticCraneLib = crane.mkLib pkgs.pkgsStatic;
      staticArgs = {
        pname = "agent-handover";
        inherit version;
        src = staticCraneLib.cleanCargoSource ./.;
        strictDeps = true;
      };
      staticCargoArtifacts = staticCraneLib.buildDepsOnly (staticArgs // {
        cargoExtraArgs = "--all-features";
      });
      musl = staticCraneLib.buildPackage (staticArgs // {
        cargoArtifacts = staticCargoArtifacts;
        doCheck = false;
      });
      releaseTooling = pkgs.buildNpmPackage {
        pname = "agent-handover-release-tooling";
        version = "25.0.9";
        src = ./release-tooling;
        npmDepsHash = "sha256-SDCeTetjPUX1MzgViIvfeBo8R4izU1u7qp1PpzhbfTg=";
        dontNpmBuild = true;
        nativeBuildInputs = [ pkgs.makeWrapper ];
        postInstall = ''
          mkdir -p "$out/bin"
          makeWrapper \
            "$out/lib/node_modules/agent-handover-release-tooling/node_modules/.bin/semantic-release" \
            "$out/bin/semantic-release" \
            --set NODE_PATH "$out/lib/node_modules/agent-handover-release-tooling/node_modules" \
            --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.git pkgs.nodejs_24 ]}
        '';
      };
      format = craneLib.cargoFmt { src = cargoSource; };
      clippy = craneLib.cargoClippy (commonArgs // {
        inherit cargoArtifacts;
        cargoClippyExtraArgs = "--all-targets --all-features -- -D warnings";
      });
      tests = craneLib.cargoTest (commonArgs // {
        inherit cargoArtifacts;
        cargoTestExtraArgs = "--all-targets --all-features";
      });
      quality = pkgs.linkFarm "agent-handover-quality" [
        { name = "format"; path = format; }
        { name = "clippy"; path = clippy; }
        { name = "tests"; path = tests; }
      ];
      scripts = pkgs.runCommand "agent-handover-script-checks" {
        nativeBuildInputs = [ pkgs.actionlint pkgs.shellcheck ];
      } ''
        shellcheck ${./scripts/prepare-release.sh}
        actionlint ${./.github/workflows/ci.yml} ${./.github/workflows/release.yml}
        touch "$out"
      '';
    in
    {
      packages.${system} = {
        default = gnu;
        inherit gnu musl releaseTooling;
      };

      checks.${system} = {
        inherit gnu musl quality scripts;
      };

      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pkgs.actionlint
          pkgs.cargo
          pkgs.cargo-edit
          pkgs.clippy
          pkgs.cocogitto
          pkgs.coreutils
          pkgs.file
          pkgs.git
          pkgs.gnutar
          pkgs.gzip
          pkgs.nodejs_24
          pkgs.patchelf
          pkgs.rustc
          pkgs.rustfmt
          pkgs.shellcheck
          releaseTooling
        ];
      };
    };
}
