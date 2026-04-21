module github.com/llm-d-incubation/llm-d-async/producer

go 1.25.0

require (
	github.com/alicebob/miniredis/v2 v2.37.0
	// TODO(#107): After the first tagged api release (e.g. api/v0.1.0), bump this require from v0.0.0 to that semver (e.g. v0.1.0).
	github.com/llm-d-incubation/llm-d-async/api v0.0.0
	github.com/redis/go-redis/v9 v9.18.0
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// TODO(#107): Keep for monorepo builds; omitted when consumers require published versions only.
replace github.com/llm-d-incubation/llm-d-async/api => ../api
