package tunnel

import (
	"errors"
	pb "github.com/adal-cloud/adal-cli/internal/tunnelpb"
)

var ErrClosed = errors.New("exchange is closed")

type Error struct{ Code pb.ErrorCode }

func (e *Error) Error() string     { return e.Code.String() }
func fail(code pb.ErrorCode) error { return &Error{Code: code} }
