package etcdplugin

import (
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func TestClampLeaseTime(t *testing.T) {
	c := Config{
		MinLeaseTime: time.Minute,
		MaxLeaseTime: 24 * time.Hour,
	}

	for _, tc := range []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"inside the range is left alone", time.Hour, time.Hour},
		{"the minimum is inclusive", time.Minute, time.Minute},
		{"the maximum is inclusive", 24 * time.Hour, 24 * time.Hour},
		{"below the minimum is raised", time.Second, time.Minute},
		{"zero is raised", 0, time.Minute},
		{"negative is raised", -time.Hour, time.Minute},
		{"above the maximum is lowered", 365 * 24 * time.Hour, 24 * time.Hour},
		// the largest lease time that fits in option 51, about 136 years
		{"the largest wire value is lowered", time.Duration(^uint32(0)) * time.Second, 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.clampLeaseTime(tc.in); got != tc.want {
				t.Errorf("clampLeaseTime(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// IsAlreadyLeased decides whether a client gets a NAK or has its packet
// dropped, so it must not answer yes to unrelated failures.
func TestIsAlreadyLeased(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"the sentinel itself", ErrAlreadyLeased, true},
		{"wrapped with %w", fmt.Errorf("ip is no longer free: %w", ErrAlreadyLeased), true},
		{"wrapped twice", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", ErrAlreadyLeased)), true},
		{"nil", nil, false},
		{"an unrelated error", errors.New("etcd connection refused"), false},
		{"an unrelated wrapped error", errors.Wrap(errors.New("i/o timeout"), "could not commit"), false},
		{"the other sentinel", ErrNotLeased, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsAlreadyLeased(tc.err); got != tc.want {
				t.Errorf("IsAlreadyLeased(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}

	// the check must not disturb the sentinel it compares against
	IsAlreadyLeased(errors.New("etcd connection refused"))
	if ErrAlreadyLeased.Error() != "already leased" {
		t.Errorf("the sentinel was overwritten to %q", ErrAlreadyLeased)
	}
}
