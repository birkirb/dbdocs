VERSION := v1.0.0
NAME := dbdocs

BUILDSTRING := $(shell git log --pretty=format:'%h' -n 1 2>/dev/null || echo "unknown")
VERSIONSTRING := $(VERSION)+$(BUILDSTRING)
BUILDDATE := $(shell date -u -Iseconds)

OUTPUT = $(NAME)
LDFLAGS := -X "main.VERSION=$(VERSIONSTRING)" -X "main.BUILDDATE=$(BUILDDATE)"

default: build

build: $(OUTPUT)

$(OUTPUT): main.go go.mod go.sum
	go build -o $(OUTPUT) -ldflags="$(LDFLAGS)" .

clean:
	rm -f $(OUTPUT)
	rm -rf dist

tag:
	git tag $(VERSION)
	git push origin --tags

build_release: clean
	@mkdir -p dist
	@echo "Building for darwin/amd64..."
	GOOS=darwin GOARCH=amd64 go build -o dist/$(NAME)-darwin-amd64 -ldflags="$(LDFLAGS)" .
	@echo "Building for linux/amd64..."
	GOOS=linux GOARCH=amd64 go build -o dist/$(NAME)-linux-amd64 -ldflags="$(LDFLAGS)" .
	@echo "Building for linux/arm64..."
	GOOS=linux GOARCH=arm64 go build -o dist/$(NAME)-linux-arm64 -ldflags="$(LDFLAGS)" .
	@echo "Building for windows/amd64..."
	GOOS=windows GOARCH=amd64 go build -o dist/$(NAME)-windows-amd64.exe -ldflags="$(LDFLAGS)" .

.PHONY: clean tag build_release
