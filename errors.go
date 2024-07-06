package etcdplugin

import "github.com/pkg/errors"

var ErrAlreadyLeased = errors.New("already leased")

func IsAlreadyLeased(err error) bool {
	return errors.As(err, &ErrAlreadyLeased)
}
