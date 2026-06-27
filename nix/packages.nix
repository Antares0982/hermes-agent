# nix/packages.nix — Antares fork package set (gateway-only)
#
# No TUI, Web, or Desktop packages — stripped for headless gateway builds.
{ inputs, ... }:
{
  perSystem = { pkgs, system, ... }:
    let
      python = (import ./python-version.nix) pkgs;
      hermesAgent = pkgs.callPackage ./hermes-agent.nix {
        inherit (inputs) uv2nix pyproject-nix pyproject-build-systems;
        inherit python;
        rev = inputs.self.rev or null;
      };
    in
    {
      packages = {
        default = hermesAgent;
      };
    };
}
