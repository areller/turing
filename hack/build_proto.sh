protoc -I=./proto kv_store.proto --go_out=plugins=grpc:$GOPATH/src