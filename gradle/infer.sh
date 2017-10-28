#!/bin/bash

# https://github.com/facebook/infer/blob/master/INSTALL.md

PLATFORM=linux64
INFER=0.13.0
FILE=infer-$PLATFORM-v$INFER

if [ -d $INFER_DIR/infer/bin ]; then
  echo "Use cache $INFER_DIR"
  exit
fi

echo "Download file $FILE"
wget -q "https://github.com/facebook/infer/releases/download/v$INFER/$FILE.tar.xz"  -O - | tar -xJf -

echo "Compile Infer $INFER"
cd $FILE
./build-infer.sh java

echo "Install Infer"
make install DESTDIR=$HOME

echo "Remove temporary dir $FILE"
rm -rfv $FILE