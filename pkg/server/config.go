package server

import (
	"fmt"
	"os"
)

// loadTransportConfigBytes returns the transport configuration bytes, from the
// inline --transport-config value or the --transport-config-file path.
func loadTransportConfigBytes(t TransportOptions) ([]byte, error) {
	if t.Config != "" {
		return []byte(t.Config), nil
	}
	data, err := os.ReadFile(t.ConfigFile) // #nosec G304 -- path from trusted CLI flag
	if err != nil {
		return nil, fmt.Errorf("failed to read transport config file %q: %w", t.ConfigFile, err)
	}
	return data, nil
}
