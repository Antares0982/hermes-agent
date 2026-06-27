# nix/packages.nix — Antares fork package set (gateway-only)
#
# No TUI, Web, or Desktop packages — stripped for headless gateway builds.
{
  perSystem = { pkgs, system, ... }:
    let
      hermesAgent = pkgs.callPackage ./hermes-agent.nix { };
    in
    {
      packages = {
        default = hermesAgent;
      };
    };
}
