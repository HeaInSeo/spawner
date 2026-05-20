SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c

LOCALBIN    := $(CURDIR)/bin
REPORT_DIR  := $(CURDIR)/reports
GOCACHE_DIR := /tmp/spawner-gocache
GOTMPDIR    := /tmp/spawner-gotmp

GOLANGCI_LINT         := $(LOCALBIN)/golangci-lint
GOLANGCI_LINT_VERSION := v2.11.3

GOVULNCHECK         := $(LOCALBIN)/govulncheck
GOVULNCHECK_VERSION := v1.1.4

GOENV := GOCACHE="$(GOCACHE_DIR)" GOTMPDIR="$(GOTMPDIR)"

PKGS_ALL        := ./...
PKGS_CORE       := ./cmd/... ./pkg/...
PKGS_SECURITY   := ./cmd/... ./pkg/...
PKGS_LIFECYCLE  := ./pkg/actor ./pkg/dispatcher ./cmd/imp

COVERAGE_THRESHOLD := 70

.PHONY: test coverage coverage-check fmt vet lint lint-depguard lint-fix lint-security vuln vuln-all test-race test-lifecycle golangci-lint govulncheck

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	@mkdir -p "$(GOCACHE_DIR)" "$(GOTMPDIR)"
	$(GOENV) go test -v -race -cover $(PKGS_ALL)

# ── Coverage ──────────────────────────────────────────────────────────────────

coverage:
	@mkdir -p "$(REPORT_DIR)" "$(GOCACHE_DIR)" "$(GOTMPDIR)"
	$(GOENV) go test $(PKGS_CORE) -coverprofile="$(REPORT_DIR)/cover.out" -covermode=atomic
	go tool cover -func="$(REPORT_DIR)/cover.out" | tee "$(REPORT_DIR)/coverage.txt"

# coverage-check: total coverage가 COVERAGE_THRESHOLD 미만이면 exit 1.
# make coverage 실행 후 호출하거나 단독 실행 가능.
coverage-check: coverage
	@total=$$(go tool cover -func="$(REPORT_DIR)/cover.out" \
	    | awk '/^total:/ {gsub(/%/,"",$$NF); print int($$NF)}'); \
	echo "[spawner] coverage: $${total}% (threshold: $(COVERAGE_THRESHOLD)%)"; \
	if [ "$${total}" -lt "$(COVERAGE_THRESHOLD)" ]; then \
	    echo "[spawner] FAIL: coverage $${total}% < threshold $(COVERAGE_THRESHOLD)%"; exit 1; \
	fi

# ── Race / Lifecycle regression ───────────────────────────────────────────────

# test-race: 핵심 lifecycle 패키지에만 -race를 실행 (make test보다 빠른 피드백용).
# make test도 -race를 포함하므로 CI에서는 make test로 충분하다.
test-race:
	@mkdir -p "$(GOCACHE_DIR)" "$(GOTMPDIR)"
	$(GOENV) go test -race $(PKGS_LIFECYCLE)

# test-lifecycle: goroutine leak / semaphore lifecycle 테스트를 -count=10으로 반복.
# 간헐적 race나 leak을 조기에 발견하기 위한 로컬 회귀 타겟.
test-lifecycle:
	@mkdir -p "$(GOCACHE_DIR)" "$(GOTMPDIR)"
	$(GOENV) go test -race -count=10 \
	    -run 'TestIngress_ReleasesSlotAfterActorBecomesIdle|TestDispatcher_SemaphoreLifecycleNoLeak|TestK8sActor_LoopExitsCleanly_NoLeak|TestMailbox_ProducerPool_NoLeak' \
	    $(PKGS_LIFECYCLE)

# ── Format / Vet ──────────────────────────────────────────────────────────────

fmt:
	go fmt $(PKGS_ALL)

vet:
	@mkdir -p "$(GOCACHE_DIR)" "$(GOTMPDIR)"
	$(GOENV) go vet $(PKGS_ALL)

# ── Lint ──────────────────────────────────────────────────────────────────────

lint: golangci-lint lint-depguard
	@mkdir -p "$(REPORT_DIR)" "$(GOCACHE_DIR)" "$(GOTMPDIR)"
	$(GOENV) $(GOLANGCI_LINT) run --config=.golangci.yml $(PKGS_CORE) | tee "$(REPORT_DIR)/lint.txt"

lint-depguard: golangci-lint
	@mkdir -p "$(REPORT_DIR)" "$(GOCACHE_DIR)" "$(GOTMPDIR)"
	$(GOENV) $(GOLANGCI_LINT) run --enable-only depguard $(PKGS_CORE) | tee "$(REPORT_DIR)/lint-depguard.txt"

lint-fix: golangci-lint
	$(GOLANGCI_LINT) run --config=.golangci.yml --fix $(PKGS_CORE)

# Security-focused lint (gosec). Kept separate from main lint gate so that
# observation findings do not block regular development CI.
lint-security: golangci-lint
	@mkdir -p "$(REPORT_DIR)" "$(GOCACHE_DIR)" "$(GOTMPDIR)"
	@echo "[spawner] security scan scope: $(PKGS_SECURITY)" | tee "$(REPORT_DIR)/lint-security-summary.txt"
	@set +e; \
	$(GOENV) $(GOLANGCI_LINT) run --enable-only gosec $(PKGS_SECURITY) \
	| tee "$(REPORT_DIR)/gosec.txt"; \
	echo "gosec_exit=$$?" | tee -a "$(REPORT_DIR)/lint-security-summary.txt"

# ── Vulnerability scan ────────────────────────────────────────────────────────

vuln: govulncheck
	@mkdir -p "$(REPORT_DIR)" "$(GOCACHE_DIR)" "$(GOTMPDIR)"
	@set +e; \
	$(GOENV) $(GOVULNCHECK) $(PKGS_SECURITY) 2>&1 | tee "$(REPORT_DIR)/govulncheck-core.txt"; \
	echo "govulncheck_core_exit=$$?" | tee "$(REPORT_DIR)/govulncheck-core.summary"

vuln-all: govulncheck
	@mkdir -p "$(REPORT_DIR)" "$(GOCACHE_DIR)" "$(GOTMPDIR)"
	@set +e; \
	$(GOENV) $(GOVULNCHECK) ./... 2>&1 | tee "$(REPORT_DIR)/govulncheck-all.txt"; \
	echo "govulncheck_all_exit=$$?" | tee "$(REPORT_DIR)/govulncheck-all.summary"

# ── Tool installation ─────────────────────────────────────────────────────────

# Builds golangci-lint from source using the local Go toolchain.
# This ensures the binary is built with the same Go version as the project,
# avoiding type-checker panics from Go version mismatches in dependencies.
# Skips if the binary is already present — delete bin/golangci-lint to force reinstall.
golangci-lint:
	@mkdir -p "$(LOCALBIN)"
	@test -x "$(GOLANGCI_LINT)" || GOBIN="$(LOCALBIN)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

govulncheck:
	@mkdir -p "$(LOCALBIN)"
	@test -x "$(GOVULNCHECK)" || GOBIN="$(LOCALBIN)" go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
