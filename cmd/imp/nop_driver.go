package imp

import (
	"context"

	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/driver"
	sErr "github.com/HeaInSeo/spawner/pkg/error"
)

// NopDriver is a driver.Driver that always returns ErrBackendUnavailable.
// Used as a safe fallback when the real execution backend fails to initialize.
// This prevents the process from panicking and allows the RunStore to
// preserve queued/held runs until backend connectivity is restored.
//
// ASSUMPTION: production adds a liveness/readiness probe that switches
// back to the real driver once connectivity is confirmed.
type NopDriver struct {
	driver.BaseDriver
}

var _ driver.Driver = (*NopDriver)(nil)

func (NopDriver) Prepare(_ context.Context, _ api.RunSpec) (driver.Prepared, error) {
	return nil, sErr.ErrBackendUnavailable
}
func (NopDriver) Start(_ context.Context, _ driver.Prepared) (driver.Handle, error) {
	return nil, sErr.ErrBackendUnavailable
}
func (NopDriver) Wait(_ context.Context, _ driver.Handle) (api.Event, error) {
	return api.Event{}, sErr.ErrBackendUnavailable
}
func (NopDriver) Signal(_ context.Context, _ driver.Handle, _ api.Signal) error {
	return sErr.ErrBackendUnavailable
}
func (NopDriver) Cancel(_ context.Context, _ driver.Handle) error {
	return sErr.ErrBackendUnavailable
}
