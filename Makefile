.PHONY: build image clean

GOARCH=amd64
BUILDDIR=build
APPNAME=debeclient
LDFLAGS="-s -w"

build: clean
	@mkdir $(BUILDDIR)
	GOOS=$(OS) GOARCH=$(GOARCH) go build -o $(BUILDDIR)/$(APPNAME) -ldflags $(LDFLAGS)

image: build
	@docker build -q -t $(APPNAME) -f Dockerfile .

clean:
	rm -rf $(BUILDDIR)