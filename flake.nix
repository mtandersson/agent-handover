{
  description = "Reproducible development and release builds for agent-handover";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      version = pkgs.lib.removeSuffix "\n" (builtins.readFile ./VERSION);
      source = pkgs.lib.cleanSourceWith {
        src = ./.;
        filter = path: type:
          let
            name = baseNameOf path;
          in
          !(
            name == ".git"
            || name == ".direnv"
            || name == "target"
            || name == "dist"
            || name == "result"
            || pkgs.lib.hasPrefix "result-" name
          );
      };
      packageArgs = {
        pname = "agent-handover";
        inherit version source;
        src = source;
        cargoLock.lockFile = ./Cargo.lock;
      };
      gnu = (pkgs.rustPlatform.buildRustPackage packageArgs).overrideAttrs (old: {
        nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ pkgs.patchelf ];
        postFixup = (old.postFixup or "") + ''
          patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 \
            --remove-rpath "$out/bin/agent-handover"
        '';
      });
      musl = pkgs.pkgsStatic.rustPlatform.buildRustPackage packageArgs;
      releaseTooling = pkgs.buildNpmPackage {
        pname = "agent-handover-release-tooling";
        version = "25.0.9";
        src = ./release-tooling;
        npmDepsHash = "sha256-pocymPhGmIbKitmbruDcu7FP73JXsckcs9+6aNU+NsM=";
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
      quality = gnu.overrideAttrs (old: {
        pname = "agent-handover-quality";
        nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ pkgs.clippy pkgs.rustfmt ];
        preBuild = ''
          cargo fmt --check
          cargo clippy --all-targets --all-features -- -D warnings
        '';
      });
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
