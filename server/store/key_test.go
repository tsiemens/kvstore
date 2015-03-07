package store

import (
	"testing"

	"github.com/tsiemens/kvstore/shared/api"
)

func makeTestKey(bytes []byte) Key {
	keybytes, _ := api.NewKey(bytes)
	return Key(keybytes)
}

func testCompareBiggerSmaller(t *testing.T, bigger Key, smaller Key) {
	// True Evals
	if !bigger.GreaterThan(smaller) {
		t.Fatal("GreaterThan failed")
	}

	if !bigger.GreaterEquals(smaller) {
		t.Fatal("GreaterEquals failed")
	}

	if !smaller.LessThan(bigger) {
		t.Fatal("LessThan failed")
	}

	if !smaller.LessEquals(bigger) {
		t.Fatal("LessEquals failed")
	}

	// False Evals
	if smaller.GreaterThan(bigger) {
		t.Fatal("GreaterThan failed")
	}

	if smaller.GreaterEquals(bigger) {
		t.Fatal("GreaterEquals failed")
	}

	if bigger.LessThan(smaller) {
		t.Fatal("LessThan failed")
	}

	if bigger.LessEquals(smaller) {
		t.Fatal("LessEquals failed")
	}
}

func TestKeyMSBCompare(t *testing.T) {
	k1 := makeTestKey([]byte{0x40, 0x50})
	k2 := makeTestKey([]byte{0x50, 0x50})

	testCompareBiggerSmaller(t, k2, k1)
}

func TestKeyLSBDifferentCompare(t *testing.T) {
	k1 := makeTestKey([]byte{0x50, 0x50})
	k2 := makeTestKey([]byte{0x50, 0x51})

	testCompareBiggerSmaller(t, k2, k1)
}

func TestKeyEquals(t *testing.T) {
	k := makeTestKey([]byte{0x88, 0x77})

	// True Evals
	if k.GreaterThan(k) {
		t.Fatal("GreaterThan failed")
	}

	if !k.GreaterEquals(k) {
		t.Fatal("GreaterEquals failed")
	}

	if k.LessThan(k) {
		t.Fatal("LessThan failed")
	}

	if !k.LessEquals(k) {
		t.Fatal("LessEquals failed")
	}
}

func TestBetween(t *testing.T) {
	k1 := makeTestKey([]byte{0x50, 0x50})
	k2 := makeTestKey([]byte{0x52, 0x51})
	k3 := makeTestKey([]byte{0x53, 0x51})

	if k1.Between(k2, k3) {
		t.Fatal("Not between failed")
	}

	if !k2.Between(k1, k1) {
		t.Fatal("Between one key failed")
	}

	if !k2.Between(k1, k3) {
		t.Fatal("Normal between failed")
	}

	if k2.Between(k3, k1) {
		t.Fatal("Looping not between failed")
	}

	if !k1.Between(k3, k2) {
		t.Fatal("Looping between 1 failed")
	}

	if !k3.Between(k2, k1) {
		t.Fatal("Looping between 2 failed")
	}
}
