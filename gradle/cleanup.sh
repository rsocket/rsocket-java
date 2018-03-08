#!/bin/bash

echo "Remove changes detected in cache"
rm -fv  $HOME/.gradle/caches/*/fileHashes/fileHashes.bin
rm -fv  $HOME/.gradle/caches/*/fileHashes/fileHashes.lock
rm -rfv $HOME/.gradle/caches/*/plugin-resolution/
rm -fv  $HOME/.gradle/caches/modules-2/modules-2.lock
rm -fv  $HOME/.gradle/caches/user-id.txt.lock
rm -fv  $HOME/.gradle/caches/user-id.txt