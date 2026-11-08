package headerwire

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestFieldsRoundTrip(t *testing.T) {
	want := Fields{"X-Bytes": {"\xff\x80", "\x00"}, "Text": {"Привет", "", "literal {\"base64\":\"/4A=\"}", "/4A="}}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `{"base64":"/4A="}`) {
		t.Fatalf("byte representation missing: %s", raw)
	}
	var got Fields
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bytes changed: %#v", got)
	}
}

func TestFieldsRejectMalformedEncoding(t *testing.T) {
	for _, raw := range []string{`{"X":[{"base64":"bad"}]}`, `{"X":[{"base64":"/4A=\n"}]}`, `{"X":[{"base64":"/4B="}]}`, `{"X":[{"base64":"/4A=","extra":"x"}]}`, `{"X":[{"other":"/4A="}]}`, `{"X":[null]}`, `{"X":[12]}`, "{\"X\":[\"\xff\"]}"} {
		var got Fields
		if err := json.Unmarshal([]byte(raw), &got); err == nil {
			t.Fatalf("accepted malformed encoding: %q", raw)
		}
	}
}

func TestFieldsReadLegacyAndNormalizeEmpty(t *testing.T) {
	var got Fields
	if err := json.Unmarshal([]byte(`{"X":["a","b",""],"Empty":[]}`), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, Fields{"X": {"a", "b", ""}, "Empty": {}}) {
		t.Fatalf("legacy %#v", got)
	}
	raw, err := json.Marshal(Fields{"Empty": nil})
	if err != nil || string(raw) != `{"Empty":[]}` {
		t.Fatalf("empty %s %v", raw, err)
	}
}
