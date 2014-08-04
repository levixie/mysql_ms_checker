#!/bin/sh

gopath=${PWD}/../../
export GOPATH=$gopath
echo "GOPATH set to $GOPATH"

go get github.com/go-sql-driver/mysql

go get github.com/golang/glog

go get code.google.com/p/gcfg

go install
