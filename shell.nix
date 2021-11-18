{ pkgs ? import <nixpkgs> {} }:

with pkgs;

mkShell {
  buildInputs = [
    python3
    python3Packages.poetry
  ];
}
