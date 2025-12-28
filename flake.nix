{
  inputs = {
    nixpkgs.url = "https://channels.nixos.org/nixpkgs-unstable/nixexprs.tar.xz";
    flake-utils.url = "github:numtide/flake-utils";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [
          (import rust-overlay)
        ];

        pkgs = import nixpkgs { inherit overlays system; };
        rust = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            rust
            pkgs.protobuf_33
          ];

          PROTOC = "${pkgs.protobuf_33}/bin/protoc";
          PROTOC_INCLUDE = "${pkgs.protobuf_33}/include";

          shellHook = '''';
        };
      }
    );
}
