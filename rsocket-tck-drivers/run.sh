#!/bin/bash

LATEST_VERSION=$(ls rsocket-tck-drivers/build/libs/rsocket-tck-drivers-*-SNAPSHOT.jar | sort -r | head -1)

java -cp "$LATEST_VERSION" io.rsocket.tckdrivers.main.Main "$@"
