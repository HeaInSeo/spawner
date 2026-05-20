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
make test          # unit tests with -race
make coverage      # coverage report → reports/
make lint          # golangci-lint + depguard boundary check (downloads to bin/ on first run)
make lint-fix      # auto-fix lint issues
make vuln          # govulncheck (downloads to bin/ on first run)
```

Security observation (non-blocking, `workflow_dispatch` only):

```bash
make lint-security   # gosec scan → reports/gosec.txt
make vuln            # govulncheck → reports/govulncheck-core.txt
```

## CI workflows

| Workflow | Trigger | What it does |
|---|---|---|
| `lint.yml` | push / PR | `make lint` (includes depguard boundary check) |
| `test.yml` | push / PR | `make test` + `make coverage`, uploads `reports/` artifact |
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
