package main

import (
	"testing"
)

func TestParseApprovalLimit(t *testing.T) {
	limit, err := parseApprovalLimit("100000")
	if err != nil || limit != 100000 {
		t.Fatalf("parseApprovalLimit() = %d, %v", limit, err)
	}

	for _, raw := range []string{"", "abc", "0", "-1"} {
		t.Run(raw, func(t *testing.T) {
			if _, err := parseApprovalLimit(raw); err == nil {
				t.Fatalf("parseApprovalLimit(%q) unexpectedly succeeded", raw)
			}
		})
	}
}
