package deliver

import "testing"

func TestJoin(t *testing.T) {
	tests := []struct {
		base  string
		path  string
		query string
		want  string
	}{
		{"https://example.test/hook", "/orders", "a=1", "https://example.test/hook/orders?a=1"},
		{"https://example.test/hook/", "orders", "", "https://example.test/hook/orders"},
		{"https://example.test/hook?x=1", "", "a=1", "https://example.test/hook?x=1&a=1"},
		{"https://example.test", "/orders", "", "https://example.test/orders"},
		{"https://example.test/hook?x=1", "", "", "https://example.test/hook?x=1"},
		{"https://example.test/hook/", "", "", "https://example.test/hook/"},
		{"https://example.test/hook", "/../orders", "", "https://example.test/hook/../orders"},
		{"https://example.test/hook%20name", "orders", "", "https://example.test/hook%20name/orders"},
		{"https://example.test/hook%20name/", "///", "", "https://example.test/hook%20name/"},
		{"https://example.test/hook/", "/", "", "https://example.test/hook/"},
	}
	for _, tc := range tests {
		got, err := Join(tc.base, tc.path, tc.query)
		if err != nil {
			t.Fatalf("Join(%q, %q, %q): %v", tc.base, tc.path, tc.query, err)
		}
		if got != tc.want {
			t.Fatalf("Join(%q, %q, %q) = %q, want %q", tc.base, tc.path, tc.query, got, tc.want)
		}
	}
}

func TestAddress(t *testing.T) {
	got, ok := Address("https://user:secret@example.test/hook", "/orders", "a=1")
	if !ok || got != "https://example.test/hook/orders?a=1" {
		t.Fatalf("address = %q ok=%v", got, ok)
	}
	got, ok = Address("https://user@example.test/hook", "", "")
	if !ok || got != "https://example.test/hook" {
		t.Fatalf("user = %q ok=%v", got, ok)
	}
	got, ok = Address("https://example.test/hook?token=secret", "", "a=1")
	if !ok || got != "https://example.test/hook?token=secret&a=1" {
		t.Fatalf("query = %q ok=%v", got, ok)
	}
	got, ok = Address("ftp://user:secret@example.test/hook", "/orders", "a=1")
	if ok || got != "" {
		t.Fatalf("rejected = %q ok=%v", got, ok)
	}
}

func TestJoinRejectsNonHTTP(t *testing.T) {
	for _, base := range []string{"", "://bad", "ftp://example.test/hook", "example.test/hook"} {
		if _, err := Join(base, "/orders", ""); err == nil {
			t.Fatalf("Join(%q) succeeded", base)
		}
	}
}
