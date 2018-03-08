#!/bin/bash

# http://opam.ocaml.org/doc/Install.html

ARCH=$(uname -m) # x86_64
SYS=$(uname -s) # Linux
OPAM=1.2.2
OCAML=4.05.0
FILE=opam-$OPAM-$ARCH-$SYS

if [ -f $OPAM_DIR/opam ]; then
  echo "Use cache $OPAM_DIR"
  exit
fi

echo "Download file $FILE"
wget -q "https://github.com/ocaml/opam/releases/download/$OPAM/$FILE"

echo "Install Opam $OPAM"
install -m 755 $FILE $OPAM_DIR/opam

echo "Remove temporary file $FILE"
rm -fv $FILE

# echo "Init Ocaml $OCAML"
# $OPAM_DIR/opam init --compiler "$OCAML" --no-setup