# nix/overlays.nix — Expose pkgs.hermes-agent for external NixOS configs
{ inputs, ... }:
{
  flake.overlays.default = final: _: let
    python = (import ./python-version.nix) final;
  in {
    hermes-agent = final.callPackage ./hermes-agent.nix {
      inherit (inputs) uv2nix pyproject-nix pyproject-build-systems;
      inherit python;
      npm-lockfile-fix = inputs.npm-lockfile-fix.packages.${final.stdenv.hostPlatform.system}.default;
      rev = inputs.self.rev or null;
    };
  };
}
