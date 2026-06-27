# nix/devShell.nix — Dev shell (gateway-only, no npm infrastructure)
{ ... }:
{
  perSystem =
    { pkgs, self', ... }:
    let
      # Only the gateway package
      devShellHook = self'.packages.default.passthru.devShellHook or "";
    in
    {
      devShells.default = pkgs.mkShell {
        inputsFrom = [ self'.packages.default ];
        packages = with pkgs; [
          uv
        ];
        shellHook = ''
          echo "Hermes Agent dev shell (gateway-only)"
          ${devShellHook}
          echo "Ready. Run 'hermes' to start."
        '';
      };
    };
}
