#!/bin/sh

echo "Starting proto to struct..."

# For Grom in proto, run once
# 1. Run this cd $GOPATH/src/github.com/infobloxopen/protoc-gen-gorm && make vendor && make install

protoc -I=. --go_out=.. *.proto

echo "Completed proto to struct..."
