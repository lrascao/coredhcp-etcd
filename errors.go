package etcdplugin

import "github.com/pkg/errors"

var (
	ErrAlreadyLeased = errors.New("already leased")
	ErrNotLeased     = errors.New("not leased")
	ErrStaleRelease  = errors.New("stale release")
	ErrNoBinding     = errors.New("no binding")
	ErrAnswered      = errors.New("answered by another server")
)

func IsAlreadyLeased(err error) bool {
	return errors.Is(err, ErrAlreadyLeased)
}

func IsStaleRelease(err error) bool {
	return errors.Is(err, ErrStaleRelease)
}

func IsNotLeased(err error) bool {
	return errors.Is(err, ErrNotLeased)
}

func IsNoBinding(err error) bool {
	return errors.Is(err, ErrNoBinding)
}

func IsAnswered(err error) bool {
	return errors.Is(err, ErrAnswered)
}
