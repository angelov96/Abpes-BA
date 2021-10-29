#!/bin/bash

TP_PATH="examples"
FILES_TO_COMPILE="$TP_PATH/*/src/*"
#docker run -v $(pwd):/go/src/github.com/hyperledger/sawtooth-sdk-go/ --network=host -it --entrypoint=/bin/bash sawtooth-build-go-protos

for f in $FILES_TO_COMPILE; do
  cd "$f" && $(which go) get ./... && $(which go) build && cd - || return
done;