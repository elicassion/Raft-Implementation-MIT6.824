#!/usr/bin/env bash
for i in {1..10}
do
    go test -run TestSnapshotRecoverManyClients3B
done
