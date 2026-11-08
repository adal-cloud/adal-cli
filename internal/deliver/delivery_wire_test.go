package deliver

import (
	"bytes"
	"context"
	"github.com/adal-cloud/adal-cli/internal/journal"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDeliveryWireHTTPAfterJournalReload(t *testing.T) {
	dir := os.Getenv("DELIVERY_WIRE_FIXTURE_DIR")
	if dir == "" {
		t.Skip("set DELIVERY_WIRE_FIXTURE_DIR for cross-repository wire verification")
	}
	for _, mode := range []string{"live", "catalog"} {
		t.Run(mode, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(dir, "assignment-"+mode+".json"))
			if err != nil {
				t.Fatal(err)
			}
			assignment, err := journal.Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			if assignment.Version != 4 {
				t.Fatalf("assignment version %d", assignment.Version)
			}
			received := make(chan bool, 1)
			receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				ok := err == nil && bytes.Equal(body, []byte{0, 255, 128}) && r.URL.RequestURI() == "/hook?a=1" && reflect.DeepEqual(r.Header.Values("X-Bytes"), []string{"\xff\x80", "café", "", "repeat", "repeat"}) && reflect.DeepEqual(r.Header.Values("X-Literal"), []string{"{\"base64\":\"/4A=\"}"}) && reflect.DeepEqual(r.Trailer.Values("X-End"), []string{"\x80\xff", "tail", ""}) && r.Header.Get("X-Hop") == "" && r.Trailer.Get("X-Hop") == ""
				received <- ok
				w.WriteHeader(204)
			}))
			defer receiver.Close()
			// Only the fixture's destination changes; payload bytes travel through every production codec.
			assignment.Endpoints[0].URL = receiver.URL
			stateDir := t.TempDir()
			book, err := journal.Open(stateDir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := book.Put(assignment); err != nil {
				t.Fatal(err)
			}
			if err := book.Close(); err != nil {
				t.Fatal(err)
			}
			book, err = journal.Open(stateDir)
			if err != nil {
				t.Fatal(err)
			}
			defer book.Close()
			restored := book.Stored()
			if len(restored) != 1 {
				t.Fatalf("stored assignments %d", len(restored))
			}
			result := Call(context.Background(), NewClient(), restored[0], restored[0].Endpoints[0])
			if result.Outcome != journal.OutcomeSuccess {
				t.Fatalf("HTTP delivery result %+v", result)
			}
			if !<-received {
				t.Fatal("wire bytes changed before reaching HTTP receiver")
			}
		})
	}
}
