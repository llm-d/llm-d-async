package util

import (
	"fmt"
	"os"
)

// Make sure it starts with '/'
func NormalizeURLPath(path string) string {
	if len(path) == 0 {
		return "/"
	}
	if path[0] != '/' {
		path = "/" + path
	}
	return path
}

func NormalizeBaseURL(baseURL string) string {
	if len(baseURL) == 0 {
		return ""
	}

	//convert to loop
	for baseURL[len(baseURL)-1] == '/' {
		baseURL = baseURL[:len(baseURL)-1]
	}
	return baseURL

}

// ExpandEnvMapValues expands environment variables in map values.
func ExpandEnvMapValues(values map[string]string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}

	expanded := make(map[string]string, len(values))
	for key, value := range values {
		expandedValue, err := expandEnvValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to expand value for %q: %w", key, err)
		}
		expanded[key] = expandedValue
	}
	return expanded, nil
}

func expandEnvValue(value string) (string, error) {
	var missingEnvVar string
	expanded := os.Expand(value, func(name string) string {
		if missingEnvVar != "" {
			return ""
		}
		envValue, ok := os.LookupEnv(name)
		if !ok {
			missingEnvVar = name
			return ""
		}
		return envValue
	})
	if missingEnvVar != "" {
		return "", fmt.Errorf("environment variable %q is not set", missingEnvVar)
	}
	return expanded, nil
}
