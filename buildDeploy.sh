#!/usr/bin/env bash
./build.sh

docker tag sw-perf-flink-bw:1.0 registry.sparkworks.net/sw-perf-flink-bw:1.0
docker push registry.sparkworks.net/sw-perf-flink-bw:1.0

docker tag sw-perf-flink-welt:1.0 registry.sparkworks.net/sw-perf-flink-welt:1.0
docker push registry.sparkworks.net/sw-perf-flink-welt:1.0

