#!/bin/bash

PREFIX="start-abpes"
SUFFIX="x-all.sh"

for SYSTEM in graphene corda fabric quorum sawtooth
do
for VARIABLE in 4 8 16
do
echo "bash" $PREFIX"-$SYSTEM-$VARIABLE-"$SUFFIX
$(which bash) $PREFIX"-$SYSTEM-$VARIABLE-"$SUFFIX
done
done