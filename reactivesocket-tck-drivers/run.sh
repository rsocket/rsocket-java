#!/bin/bash

LATEST_VERSION=$(find build/libs/ | sort -r | head -1)

echo "running latest version $LATEST_VERSION"

java -cp "build/libs/$LATEST_VERSION" io.reactivesocket.tckdrivers.main.Main "$@"
