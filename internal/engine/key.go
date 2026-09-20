// Package engine implements the concurrent speculation store used by the
// Reasonix spec-ptc extension. It ports the dispatch, adoption, FIFO claim,
// deterministic reuse, budget, cancellation, and stale-turn semantics from
// alexzhang13/spec-ptc without carrying over its Python shadow runtime.
package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Key is the canonical claim identity shared by speculative dispatch and the
// later real call. Tool remains separate for cheap diagnostics and map
// partitioning; Digest hashes the tool name and canonical JSON arguments.
type Key struct {
	Tool   string
	Digest [sha256.Size]byte
}

// String returns a compact, stable diagnostic form. It is not a wire token.
func (k Key) String() string {
	return k.Tool + ":" + hex.EncodeToString(k.Digest[:8])
}

// CanonicalKey normalizes a JSON argument object before hashing it. Go's JSON
// encoder sorts object keys; UseNumber avoids collapsing large integers through
// float64 while normalizing insignificant whitespace. Dispatch and claim must
// both call this function.
func CanonicalKey(tool string, arguments json.RawMessage) (Key, error) {
	if tool == "" {
		return Key{}, errors.New("specptc: empty tool name")
	}
	dec := json.NewDecoder(bytes.NewReader(arguments))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return Key{}, fmt.Errorf("specptc: decode arguments: %w", err)
	}
	if _, ok := value.(map[string]any); !ok {
		return Key{}, errors.New("specptc: arguments must be a JSON object")
	}
	if err := requireEOF(dec); err != nil {
		return Key{}, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return Key{}, fmt.Errorf("specptc: canonicalize arguments: %w", err)
	}
	h := sha256.New()
	_, _ = h.Write([]byte(tool))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(canonical)
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return Key{Tool: tool, Digest: digest}, nil
}

func requireEOF(dec *json.Decoder) error {
	var trailing any
	if err := dec.Decode(&trailing); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return fmt.Errorf("specptc: decode trailing arguments: %w", err)
	}
	return errors.New("specptc: arguments contain multiple JSON values")
}
