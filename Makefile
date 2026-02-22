TOOL_DIR := .tool
BINNY := $(TOOL_DIR)/binny

$(BINNY):
	@mkdir -p $(TOOL_DIR)
	@curl -sSfL https://get.anchore.io/binny | sh -s -- -b $(TOOL_DIR)

.PHONY: lint
lint: $(BINNY)
	@$(BINNY) install golangci-lint -q
	@$(TOOL_DIR)/golangci-lint run --fix ./...

.PHONY: test
test:
	go test -short -race -count=1 ./...

.PHONY: test-integration
test-integration:
	go test -v -race -count=1 ./...
