module github.com/llm-d/llm-d-async/producer

go 1.26.0

require (
	github.com/alicebob/miniredis/v2 v2.39.0
	github.com/llm-d/llm-d-async/api v0.9.1
	github.com/redis/go-redis/v9 v9.22.0
	github.com/stretchr/testify v1.12.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/llm-d/llm-d-async/api => ../api
