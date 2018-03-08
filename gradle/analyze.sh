#!/bin/bash

echo "Clean project"
gradle clean

echo "Capture compilation commands"
infer capture -- gradle test

echo "Analyze with default checkers"
infer analyze -a checkers --print-active-checkers --quiet && cat infer-out/bugs.txt

echo "Analyze with experimental checkers"
infer --no-default-checkers --bufferoverrun --compute-analytics --eradicate --repeated-calls --suggest-nullable --tracing --print-active-checkers --quiet && cat infer-out/bugs.txt