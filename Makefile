VERSION ?= $(shell git describe --tags --always | sed "s/^v//")
LDFLAGS  = -s -w -X main.version=$(VERSION)
TARGETS  = darwin/arm64 darwin/amd64 linux/amd64 linux/arm64 windows/amd64

pocketsocket: main.go go.mod go.sum
	CGO_ENABLED=0 go build -ldflags="$(LDFLAGS)" -o $@ .

dist:
	mkdir -p dist
	for t in $(TARGETS); do \
		bin=pocketsocket; [ $${t%/*} = windows ] && bin=pocketsocket.exe; \
		CGO_ENABLED=0 GOOS=$${t%/*} GOARCH=$${t#*/} go build -ldflags="$(LDFLAGS)" -o $$bin . || exit 1; \
		zip -qj dist/pocketsocket_$(VERSION)_$${t%/*}_$${t#*/}.zip $$bin; rm $$bin; \
	done

clean:
	rm -rf pocketsocket dist

.PHONY: pocketsocket dist clean
