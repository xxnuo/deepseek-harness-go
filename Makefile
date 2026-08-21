UPSTREAM_DIR := deepseek-harness
UPSTREAM_COMMIT := $(shell sed -n 's/^commit=//p' upstream.lock)
UPSTREAM_REPOSITORY := $(shell sed -n 's/^repository=//p' upstream.lock)

.PHONY: prepare check-upstream-clean prepare-runtime-assets verify-upstream sync-runtime-assets verify-runtime-assets generate-pi-ai-catalog verify-pi-ai-catalog generate-dynamic-inspect-catalog verify-dynamic-inspect-catalog test smoke-standalone smoke-clean-archive

prepare:
	@test -n "$(UPSTREAM_COMMIT)" && test -n "$(UPSTREAM_REPOSITORY)"
	@if test -e "$(UPSTREAM_DIR)"; then \
		git -C "$(UPSTREAM_DIR)" rev-parse --is-inside-work-tree >/dev/null 2>&1 || { echo "$(UPSTREAM_DIR) exists but is not a git checkout" >&2; exit 1; }; \
		actual=$$(git -C "$(UPSTREAM_DIR)" rev-parse HEAD); \
		test "$$actual" = "$(UPSTREAM_COMMIT)" || { echo "$(UPSTREAM_DIR) is at $$actual; expected $(UPSTREAM_COMMIT). Refusing to overwrite it." >&2; exit 1; }; \
	else \
		git clone --filter=blob:none --no-checkout "$(UPSTREAM_REPOSITORY)" "$(UPSTREAM_DIR)"; \
		git -C "$(UPSTREAM_DIR)" checkout --detach "$(UPSTREAM_COMMIT)"; \
	fi
	@$(MAKE) prepare-runtime-assets
	@$(MAKE) verify-upstream

check-upstream-clean:
	@test -d "$(UPSTREAM_DIR)/.git"
	@git -C "$(UPSTREAM_DIR)" rev-parse HEAD | grep -Fx "$(UPSTREAM_COMMIT)" >/dev/null
	@status=$$(git -C "$(UPSTREAM_DIR)" status --porcelain=v1 --untracked-files=all); \
		test -z "$$status" || { echo "$(UPSTREAM_DIR) has uncommitted changes; restore a clean pinned checkout before syncing or verifying." >&2; printf '%s\n' "$$status" >&2; exit 1; }

prepare-runtime-assets: check-upstream-clean
	@set -e; \
		if go run ./scripts/sync_runtime_assets.go -verify-client-build-only -upstream "$(UPSTREAM_DIR)" >/dev/null 2>&1; then \
			echo "official upstream client artifacts are current"; \
		else \
			echo "building official upstream client artifacts"; \
			pnpm --dir "$(UPSTREAM_DIR)" install --frozen-lockfile; \
			pnpm --dir "$(UPSTREAM_DIR)" run build:official; \
			go run ./scripts/sync_runtime_assets.go -verify-client-build-only -upstream "$(UPSTREAM_DIR)"; \
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
	@go test -count=1 -run '^TestUpstreamContract' .
	@go test -count=1 -run '^TestUpstreamCLIContract$$' ./cmd/dsh

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

test:
	go test -count=1 ./...

smoke-standalone:
	go test -count=1 -run TestStandaloneBinaryServesEmbeddedRuntime ./cmd/dsh

smoke-clean-archive:
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
		git archive --format=tar "$$(git write-tree)" | tar -xf - -C "$$tmp"; \
		cd "$$tmp"; \
		go test -count=1 ./...; \
		go build ./cmd/dsh ./cmd/dsh-sdk ./cmd/dsh-landlock-run
