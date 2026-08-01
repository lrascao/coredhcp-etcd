package etcdplugin

import "github.com/pkg/errors"

var (
	ErrAlreadyLeased = errors.New("already leased")
	ErrNotLeased     = errors.New("not leased")
)

func IsAlreadyLeased(err error) bool {
	return errors.Is(err, ErrAlreadyLeased)
}
