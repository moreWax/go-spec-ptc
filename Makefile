.PHONY: build test race clean

build:
	mkdir -p bin
	go build -trimpath -o bin/reasonix-spec-ptc .

test:
	go test ./...

race:
	go test -race ./...

clean:
	rm -rf bin
