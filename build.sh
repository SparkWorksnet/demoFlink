#!/usr/bin/env bash
docker build --build-arg JAR_FILE=./target/flink.jar -t sw-perf-flink:1.0 .
docker build --build-arg JAR_FILE=./target/flink-s.jar -t sw-perf-flink-s:1.0 .
docker build --build-arg JAR_FILE=./target/flink-w.jar -t sw-perf-flink-w:1.0 .
docker build --build-arg JAR_FILE=./target/flink-welt.jar -t sw-perf-flink-welt:1.0 .
