#!/bin/bash

LATEST_VERSION=$(ls build/libs/reactivesocket-tck-drivers-*-SNAPSHOT.jar | sort -r | head -1)

echo "running latest version $LATEST_VERSION"

java -cp "$LATEST_VERSION" io.rsocket.tckdrivers.main.Main "$@"
