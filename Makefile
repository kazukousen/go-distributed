.PHONY: compile
compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

.PHONY: test
debug ?= false
test:
	# go test -v -race $(shell go list ./... | grep -v  "internal/server" | sed 's|github.com/kazukousen/go-distributed|.|g')
	# cd ./internal/server && go test -v -debug=true
	go test -v -race ./...
