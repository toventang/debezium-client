.PHONY: build image clean
GOARCH=amd64
BUILDDIR=build
APPNAME=debeclient
LDFLAGS="-s -w"

build: clean

	@mkdir $(BUILDDIR)
	CGO_ENABLED=0 GO111MODULE=on GOOS=$(OS) GOARCH=$(GOARCH) go build -o $(BUILDDIR)/$(APPNAME) -ldflags $(LDFLAGS)

image: build
	@docker build -q -t $(APPNAME) -f Dockerfile .

clean:
	rm -rf $(BUILDDIR)