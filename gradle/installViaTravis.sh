#!/bin/bash

OPAM_VER=4.05.0
INFER_VER=0.13.0

# https://github.com/facebook/infer/blob/master/INSTALL.md#pre-compiled-versions
echo "Installing Infer dependencies"
sudo apt-get update
sudo apt-get -o Dpkg::Options::="--force-confnew" upgrade -y
sudo apt-get -o Dpkg::Options::="--force-confnew" install -y libffi-dev libmpc-dev libmpfr-dev python-software-properties
# installed on travis: autoconf automake build-essential libgmp-dev m4 pkg-config unzip zlib1g-dev

# http://opam.ocaml.org/doc/Install.html#Binarydistribution
echo "Installing Opam"
wget https://raw.githubusercontent.com/ocaml/opam/master/shell/opam_installer.sh -O - | sh -s /usr/local/bin ${OPAM_VER}

echo "Downloading Infer"
wget -q https://github.com/facebook/infer/releases/download/v${INFER_VER}/infer-linux64-v${INFER_VER}.tar.xz
tar xf infer-linux64-v${INFER_VER}.tar.xz
cd infer-linux64-v${INFER_VER}/

echo "Compiling Infer"
./build-infer.sh java

echo "Installing Infer"
sudo make install

echo "Adding Infer to PATH"
export PATH=`pwd`/infer/bin:$PATH