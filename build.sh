#!/usr/bin/env bash

mkdir -p build
cd go || exit 1
CGO_ENABLED=0 go build -ldflags='-extldflags=-static' -a -o ../build/export-hdfs-directory-usage ./cmd/export_hdfs_directory_usage
