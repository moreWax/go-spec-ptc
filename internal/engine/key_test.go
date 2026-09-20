package engine

import (
	"encoding/json"
	"testing"
)

func TestCanonicalKeyNormalizesObjectOrderAndWhitespace(t *testing.T) {
	a, err := CanonicalKey("query", json.RawMessage(`{"b":2,"a": [1, 9007199254740993]}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalKey("query", json.RawMessage("{\n  \"a\": [1,9007199254740993], \"b\": 2\n}"))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("canonical keys differ: %s != %s", a.String(), b.String())
	}
}

func TestCanonicalKeyRejectsNonObjectAndTrailingJSON(t *testing.T) {
	for _, raw := range []string{`[]`, `null`, `{"x":1} {"y":2}`, `{`} {
		if _, err := CanonicalKey("query", json.RawMessage(raw)); err == nil {
			t.Errorf("CanonicalKey(%q) succeeded", raw)
		}
	}
}
