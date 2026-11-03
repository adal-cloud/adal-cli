package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

const (
	stateName = "state.json"
	tempName  = "state.json.tmp"
	lockName  = "state.lock"
)

var chmodFile = func(name string, mode os.FileMode) error {
	return os.Chmod(name, mode)
}

// Dir is the journal directory for one server and one channel.
// The path contains hashes of the host and of the token.
// The path does not contain the token.
func Dir(host, token string) (string, error) {
	base, err := os.UserConfigDir()
	if err != nil || base == "" {
		return "", ErrUnavailable
	}
	return filepath.Join(base, "adal-cli", "journals", digest(host), digest(token)), nil
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// Open loads the journal in dir.
// It creates the directory when the directory is missing.
// The directory is private to the user.
// A second opener gets ErrBusy.
// A truncated or unreadable state file returns ErrCorrupt.
// A leftover temporary file is ignored.
func Open(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, ErrUnavailable
	}
	_ = os.Chmod(dir, 0o700)
	lock, err := os.OpenFile(filepath.Join(dir, lockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, ErrUnavailable
	}
	if err := lockExclusive(lock); err != nil {
		_ = lock.Close()
		return nil, err
	}
	j := &Journal{
		dir:    dir,
		lock:   lock,
		handed: map[string]bool{},
		state:  newSnapshot(),
		saver:  func(data []byte) error { return writeAtomic(dir, data) },
	}
	if err := j.load(); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return j, nil
}

// Close releases the single-writer lock.
func (j *Journal) Close() error {
	if j == nil || j.lock == nil {
		return nil
	}
	err := j.lock.Close()
	j.lock = nil
	return err
}

func (j *Journal) load() error {
	data, err := os.ReadFile(filepath.Join(j.dir, stateName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return ErrCorrupt
	}
	if len(data) == 0 {
		return ErrCorrupt
	}
	state, err := decodeSnapshot(data)
	if err != nil {
		return ErrCorrupt
	}
	j.state = state
	return nil
}

func writeAtomic(dir string, data []byte) error {
	tmp := filepath.Join(dir, tempName)
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	// The file is already created with mode 0600.
	// A failed mode change does not cancel the write.
	_ = chmodFile(tmp, 0o600)
	if err := os.Rename(tmp, filepath.Join(dir, stateName)); err != nil {
		return err
	}
	return nil
}

func decodeSnapshot(data []byte) (snapshot, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil || raw == nil {
		return snapshot{}, errors.New("journal is corrupt")
	}
	var file fileState
	if err := json.Unmarshal(data, &file); err != nil {
		return snapshot{}, err
	}
	return file.snapshot(), nil
}
