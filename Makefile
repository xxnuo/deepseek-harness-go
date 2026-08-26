UPSTREAM_DIR := deepseek-harness
UPSTREAM_COMMIT := $(shell sed -n 's/^commit=//p' upstream.lock)
UPSTREAM_REPOSITORY := $(shell sed -n 's/^repository=//p' upstream.lock)
DEV_PORT ?= 13080

GO ?= go
GOOS ?= $(shell $(GO) env GOOS)
GOARCH ?= $(shell $(GO) env GOARCH)
DIST_DIR ?= dist
VERSION ?= $(shell sed -n 's/^tag=dsh-v//p' upstream.lock)
GOFLAGS ?= -trimpath -buildvcs=false
LDFLAGS ?= -s -w
RELEASE_COMMANDS ?= dsh dsh-sdk dsh-landlock-run
RELEASE_PLATFORMS ?= linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 windows-amd64 windows-arm64
HOST_GO = GOOS=$(shell $(GO) env GOHOSTOS) GOARCH=$(shell $(GO) env GOHOSTARCH) $(GO)
ASSET_GO = $(HOST_GO) run ./scripts/with_runtime_assets --upstream "$(UPSTREAM_DIR)" --

.PHONY: prepare prepare-upstream check-upstream-clean prepare-runtime-assets verify-upstream sync-runtime-assets verify-runtime-assets generate-pi-ai-catalog verify-pi-ai-catalog generate-dynamic-inspect-catalog verify-dynamic-inspect-catalog generate-upstream-inventory verify-upstream-inventory generate-public-facade verify-public-facade dev test race vet build build-all build-platform $(RELEASE_PLATFORMS) smoke-standalone smoke-clean-archive

prepare:
	@$(MAKE) prepare-upstream
	@$(MAKE) prepare-runtime-assets
	@$(MAKE) verify-upstream

prepare-upstream:
	@test -n "$(UPSTREAM_COMMIT)" && test -n "$(UPSTREAM_REPOSITORY)"
	@if test -e "$(UPSTREAM_DIR)"; then \
		git -C "$(UPSTREAM_DIR)" rev-parse --is-inside-work-tree >/dev/null 2>&1 || { echo "$(UPSTREAM_DIR) exists but is not a git checkout" >&2; exit 1; }; \
		actual=$$(git -C "$(UPSTREAM_DIR)" rev-parse HEAD); \
		test "$$actual" = "$(UPSTREAM_COMMIT)" || { echo "$(UPSTREAM_DIR) is at $$actual; expected $(UPSTREAM_COMMIT). Refusing to overwrite it." >&2; exit 1; }; \
	else \
		git clone --filter=blob:none --no-checkout "$(UPSTREAM_REPOSITORY)" "$(UPSTREAM_DIR)"; \
		git -C "$(UPSTREAM_DIR)" checkout --detach "$(UPSTREAM_COMMIT)"; \
	fi

check-upstream-clean:
	@test -d "$(UPSTREAM_DIR)/.git"
	@git -C "$(UPSTREAM_DIR)" rev-parse HEAD | grep -Fx "$(UPSTREAM_COMMIT)" >/dev/null
	@status=$$(git -C "$(UPSTREAM_DIR)" status --porcelain=v1 --untracked-files=all); \
		test -z "$$status" || { echo "$(UPSTREAM_DIR) has uncommitted changes; restore a clean pinned checkout before syncing or verifying." >&2; printf '%s\n' "$$status" >&2; exit 1; }

prepare-runtime-assets: check-upstream-clean
	@set -e; \
		if $(HOST_GO) run ./scripts/sync_runtime_assets.go -verify-client-build-only -upstream "$(UPSTREAM_DIR)" >/dev/null 2>&1; then \
			echo "official upstream client artifacts are current"; \
		else \
			echo "building official upstream client artifacts"; \
			pnpm --dir "$(UPSTREAM_DIR)" install --frozen-lockfile; \
			pnpm --dir "$(UPSTREAM_DIR)" run build:official; \
			$(HOST_GO) run ./scripts/sync_runtime_assets.go -verify-client-build-only -upstream "$(UPSTREAM_DIR)"; \
		fi

verify-upstream: check-upstream-clean
	@test -d "$(UPSTREAM_DIR)/.git"
	@test -f "$(UPSTREAM_DIR)/packages/core/agent-loop/src/agent.ts"
	@test -f "$(UPSTREAM_DIR)/packages/core/session/src/types.ts"
	@test -f "$(UPSTREAM_DIR)/packages/host/apiproxy/src/api/rpc-map.ts"
	@test -f "$(UPSTREAM_DIR)/apps/cli/src/args.ts"
	@test -f "$(UPSTREAM_DIR)/apps/web/dist/index.html"
	@test -f "$(UPSTREAM_DIR)/packages/client/runtime/lib/client.js"
	@test -f "$(UPSTREAM_DIR)/packages/bundle/base/cordis.patch.yml"
	@test -f "$(UPSTREAM_DIR)/apps/cli/config/agent-presets/standard/agent.cordis.yml"
	@$(MAKE) verify-runtime-assets
	@$(MAKE) verify-pi-ai-catalog
	@$(MAKE) verify-dynamic-inspect-catalog
	@$(MAKE) verify-upstream-inventory
	@$(MAKE) verify-public-facade
	@$(ASSET_GO) test -count=1 -run '^TestUpstreamContract' ./internal/harness
	@$(ASSET_GO) test -count=1 -run '^TestUpstreamCLIContract$$' ./cmd/dsh

sync-runtime-assets: check-upstream-clean
	go run ./scripts/sync_runtime_assets.go -upstream "$(UPSTREAM_DIR)"

verify-runtime-assets: check-upstream-clean
	go run ./scripts/sync_runtime_assets.go -check -upstream "$(UPSTREAM_DIR)"

generate-pi-ai-catalog: check-upstream-clean
	pnpm --dir "$(UPSTREAM_DIR)/packages/llm/llm-pi-ai" exec node ../../../../scripts/generate-pi-ai-catalog.mjs

verify-pi-ai-catalog: check-upstream-clean
	pnpm --dir "$(UPSTREAM_DIR)/packages/llm/llm-pi-ai" exec node ../../../../scripts/generate-pi-ai-catalog.mjs --check

generate-dynamic-inspect-catalog: check-upstream-clean
	pnpm --dir "$(UPSTREAM_DIR)" exec tsx ../scripts/gen_dynamic_inspect_catalog.ts

verify-dynamic-inspect-catalog: check-upstream-clean
	pnpm --dir "$(UPSTREAM_DIR)" exec tsx ../scripts/gen_dynamic_inspect_catalog.ts --check

generate-upstream-inventory: check-upstream-clean
	UPDATE_UPSTREAM_INVENTORY=1 $(ASSET_GO) test -count=1 -run '^TestUpstreamContractWorkspaceInventory$$' ./internal/harness

verify-upstream-inventory: check-upstream-clean
	$(ASSET_GO) test -count=1 -run '^TestUpstreamContractWorkspaceInventory$$' ./internal/harness

generate-public-facade:
	go run ./scripts/gen_public_facade

verify-public-facade:
	go run ./scripts/gen_public_facade -check

dev: prepare-runtime-assets
	$(ASSET_GO) run ./cmd/dsh web --no-open --port "$(DEV_PORT)"

test: prepare-runtime-assets
	$(ASSET_GO) test -count=1 ./...

race: prepare-runtime-assets
	$(ASSET_GO) test -race -count=1 ./...

vet: prepare-runtime-assets
	$(ASSET_GO) vet ./...

build:
	@$(MAKE) build-platform GOOS="$(GOOS)" GOARCH="$(GOARCH)"

build-all: $(RELEASE_PLATFORMS)

$(RELEASE_PLATFORMS):
	@$(MAKE) build-platform GOOS="$(word 1,$(subst -, ,$@))" GOARCH="$(word 2,$(subst -, ,$@))"

build-platform: prepare-runtime-assets
	@test -n "$(GOOS)" && test -n "$(GOARCH)"
	@mkdir -p "$(DIST_DIR)"
	@set -e; \
		suffix=; \
		if test "$(GOOS)" = windows; then suffix=.exe; fi; \
		temporary=$$(mktemp -d "$(DIST_DIR)/.build-$(GOOS)-$(GOARCH)-XXXXXX"); \
		trap 'rm -rf "$$temporary"' EXIT; \
		CGO_ENABLED=0 DSH_TARGET_GOOS="$(GOOS)" DSH_TARGET_GOARCH="$(GOARCH)" $(ASSET_GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o "$$temporary/" $(foreach command,$(RELEASE_COMMANDS),./cmd/$(command)); \
		for command in $(RELEASE_COMMANDS); do \
			output="$(DIST_DIR)/$$command-$(VERSION)-$(GOOS)-$(GOARCH)$$suffix"; \
			echo "building $$output"; \
			mv -f "$$temporary/$$command$$suffix" "$$output"; \
		done

smoke-standalone: prepare-runtime-assets
	$(ASSET_GO) test -count=1 -run TestStandaloneBinaryServesEmbeddedRuntime ./cmd/dsh

smoke-clean-archive: prepare-runtime-assets
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
		git archive --format=tar "$$(git write-tree)" | tar -xf - -C "$$tmp"; \
		cd "$$tmp"; \
		DEEPSEEK_HARNESS_UPSTREAM="$(abspath $(UPSTREAM_DIR))" $(GO) run ./scripts/with_runtime_assets --upstream "$(abspath $(UPSTREAM_DIR))" -- test -count=1 ./...; \
		DEEPSEEK_HARNESS_UPSTREAM="$(abspath $(UPSTREAM_DIR))" $(GO) run ./scripts/with_runtime_assets --upstream "$(abspath $(UPSTREAM_DIR))" -- build ./cmd/dsh ./cmd/dsh-sdk ./cmd/dsh-landlock-run
