package etcdplugin

import "github.com/pkg/errors"

var (
	ErrAlreadyLeased = errors.New("already leased")
	ErrNotLeased     = errors.New("not leased")
	ErrStaleRelease  = errors.New("stale release")
)

func IsAlreadyLeased(err error) bool {
	return errors.Is(err, ErrAlreadyLeased)
}

func IsStaleRelease(err error) bool {
	return errors.Is(err, ErrStaleRelease)
}
