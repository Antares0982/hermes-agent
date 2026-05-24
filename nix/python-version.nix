# nix/python-version.nix — Centralized Python version for all hermes-agent Nix builds
#
# Import this file with `import ./python-version.nix pkgs` to get the canonical
# Python package (e.g. pkgs.python314).  All Nix expressions that build packages,
# run checks, configure NixOS modules, or generate scripts MUST go through this
# file instead of hardcoding a specific python version (python312, python313, …).
#
# Usage:
#   python = (import ./python-version.nix) pkgs;
#   # then use python.pkgs.X, python.sitePackages, python.withPackages, …
#
# To bump the Python version, change this single line and rebuild.
pkgs: pkgs.python314
