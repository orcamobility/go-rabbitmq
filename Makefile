all: test vet staticcheck

test:
	go test -v -race -p 1 ./...

vet:
	go vet ./...

staticcheck:
	staticcheck ./...
