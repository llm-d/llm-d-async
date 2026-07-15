# CLAUDE.md — llm-d-async

Asynchronous dispatch processor for llm-d. Pulls batch requests from a message queue, gates dispatch based on system capacity, and forwards requests to `llm-d-router` (or another inference gateway).

## General Principles

- **Think before coding**: State assumptions explicitly. If uncertain, ask. Don't guess silently.
- **Simplicity first**: No features beyond what was asked. No abstractions for single-use code.
- **Surgical changes**: Only touch what you must. Don't "improve" adjacent code, comments, or formatting. Match existing style.

## Code Conventions

- **Logging**: Use `logr.Logger` — stdlib `log` is banned in non-test code (enforced by `depguard`).
- **Interfaces**: Verify compliance at compile time: `var _ Interface = (*Impl)(nil)`.
- **Errors**: Wrap with `fmt.Errorf("context: %w", err)` when returning errors from external packages. Handle errors once — return OR log, never both.
- **No mutable globals**: Avoid package-level mutable variables. Use unexported state with constructors and accessors. Exceptions: Prometheus metric registrations and build-time version vars.
- **No panic**: Never use `panic()` in production code. Return errors instead.
- **YAGNI**: Don't add exported functions, methods, or types without a caller in this repo.
- **Porting rule**: When adapting code from EPP or another repo, strip it to what is actually called and re-fit it to local idioms before opening the PR.

## Build & Verify

- `make build` — compile binary
- `make ci` — run all CI checks (fmt, vet, lint, test)
- `make lint` — run golangci-lint
- `make lint-fix` — run golangci-lint with auto-fix

## Testing

- Unit tests: `make test`
- Integration tests: `make test-integration`
- All tests: `make test-all`
- E2E tests: `make test-e2e` (requires Kind cluster)

## Multi-Module Repo

This repo has four Go modules: root, `api/`, `pipeline/`, `producer/`. Run `make fmt` and `make vet` to cover all modules. See `CONTRIBUTING.md` for release tagging.
