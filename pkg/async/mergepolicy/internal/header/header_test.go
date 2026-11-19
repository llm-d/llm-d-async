package header

import (
	"maps"
	"testing"
)

func TestSet_ReplacesEveryCasingOfTheHeader(t *testing.T) {
	headers := map[string]string{
		"Content-Type":  "application/json",
		"content-type":  "text/plain",
		"CONTENT-TYPE":  "application/octet-stream",
		"Authorization": "Bearer t",
	}
	Set(headers, "content-type", "multipart/form-data")
	want := map[string]string{"content-type": "multipart/form-data", "Authorization": "Bearer t"}
	if !maps.Equal(headers, want) {
		t.Fatalf("headers = %v, want %v", headers, want)
	}
}

func TestSet_KeepsTheRequestedSpellingAndOtherHeaders(t *testing.T) {
	headers := map[string]string{"x-custom": "1"}
	Set(headers, "X-Gateway-Priority", "3")
	Set(headers, "X-Gateway-Priority", "4")
	want := map[string]string{"x-custom": "1", "X-Gateway-Priority": "4"}
	if !maps.Equal(headers, want) {
		t.Fatalf("headers = %v, want %v", headers, want)
	}
}
