#!/bin/sh

echo "Starting proto to struct..."

protoc -I=. -I=$GOPATH/src/ --go_out=.. --gorm_out=engine=postgres:.. *.proto

# Remove omitempty option
# Credit: https://stackoverflow.com/a/37335452
ls ../models/*.pb.go | xargs -n1 -IX bash -c 'sed s/,omitempty// X > X.tmp && mv X{.tmp,}'

echo "Completed proto to struct..."
