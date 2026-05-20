# spawner

Job spawner for Kubernetes — dispatches workloads via a driver interface, with the k8s implementation isolated in `cmd/imp/`.

## Package structure

```
pkg/          ← k8s-agnostic core (domain types, driver interface, dispatcher, policy, store)
cmd/imp/      ← k8s adapter (implements pkg/driver.Driver against k8s.io/*)
cmd/server/   ← entry point
```

`pkg/driver` defines the `Driver` / `Prepared` / `Handle` interfaces in pure Go.
`cmd/imp` holds the k8s implementation. Nothing in `pkg/` imports `k8s.io` or `sigs.k8s.io` —
this boundary is enforced by depguard in CI (see below).

## Development

```bash
make test            # unit tests with -race, all packages
make test-race       # -race on lifecycle packages only (pkg/actor, pkg/dispatcher, cmd/imp)
make test-lifecycle  # goroutine leak / semaphore lifecycle regression, -race -count=10
make coverage        # coverage report → reports/cover.out, reports/coverage.txt
make coverage-check  # coverage report + fail if total < 70%
make lint            # golangci-lint + depguard boundary check (builds to bin/ on first run)
make lint-fix        # auto-fix lint issues
make vuln            # govulncheck (builds to bin/ on first run)
```

Security observation (non-blocking, `workflow_dispatch` only):

```bash
make lint-security   # gosec scan → reports/gosec.txt
make vuln-all        # govulncheck (all packages) → reports/govulncheck-all.txt
```

## CI workflows

| Workflow | Trigger | What it does |
|---|---|---|
| `lint.yml` | push / PR | `make lint` (includes depguard boundary check) |
| `test.yml` | push / PR | `make test` + `make coverage-check`, uploads `reports/` artifact |
| `security-observe.yml` | manual | gosec + govulncheck, uploads `reports/` artifact |

## depguard — k8s 경계 강제

`pkg/` 전체는 k8s와 구조적으로 분리되어 있다. k8s 임포트는 `cmd/imp/`에만 존재하며,
이는 포트-어댑터 설계의 의도된 경계다.

```
pkg/**/*.go  →  k8s.io/**, sigs.k8s.io/**  임포트 금지
*_test.go    →  depguard 제외 (테스트 보조 라이브러리 허용)
```

`make lint` 실행 시 `lint-depguard`가 먼저 실행되어 경계 위반을 명확한 에러 메시지로 차단한다.
이 덕분에 `pkg/` 전체가 k8s 클러스터 없이 단위 테스트 가능하다.

## Testing

### Test strategy

모든 테스트는 실제 Kubernetes 클러스터 없이 fake actor / fake driver / in-memory store로 동작한다.
k8s 의존 테스트는 `//go:build integration` 태그로 격리되어 있으며 일반 `make test`에서 제외된다.

- **정상 경로**: ingress boundary, RunStore state machine, actor lifecycle
- **failure-path**: driver 오류(Prepare/Start/Wait/Cancel), semaphore rollback, goroutine 누수 없음
- **lifecycle invariant**: Close/cancel 경합 panic 없음, OnIdle·OnTerminate 중복 호출 시 release 1회
- **goroutine leak**: `go.uber.org/goleak`으로 lifecycle 테스트 전반에 걸쳐 검증

### Coverage (v0.1.0)

| Package | Coverage |
|---|---|
| `pkg/actor` | 90.0% |
| `pkg/api` | 92.2% |
| `pkg/dispatcher` | 76.0% |
| `pkg/driver` | 100.0% |
| `pkg/factory` | 96.8% |
| `pkg/frontdoor` | 91.7% |
| `pkg/policy` | 74.7% |
| `pkg/store` | 79.8% |
| `cmd/imp` | 76.5% |
| `cmd/server` | 29.5% |
| **Total** | **77.8%** |

커버리지 하한선은 70% (`make coverage-check` 강제).

## Placement surface

Current `RunSpec.Placement` semantics are intentionally narrow.

- `NodeSelector`: direct hard constraint passthrough
- `RequiredNodeName`: mapped to `kubernetes.io/hostname` nodeSelector
- `PreferredNodes`: mapped to `nodeAffinity.preferredDuringSchedulingIgnoredDuringExecution`

`RequiredNodeName`과 `PreferredNodes`는 동시에 사용하지 않는다 (v0 제약).
