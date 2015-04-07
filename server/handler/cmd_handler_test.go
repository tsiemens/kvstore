package handler

import (
	"testing"
)

func TestMinOps(t *testing.T) {
	if minSuccessfulOps(1) != 1 {
		t.Fatal("min successful for 1 failed")
	}
	if minSuccessfulOps(2) != 2 {
		t.Fatal("min successful for 2 failed")
	}
	if minSuccessfulOps(5) != 3 {
		t.Fatal("min successful for 5 failed")
	}
}
