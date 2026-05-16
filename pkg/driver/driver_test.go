package driver_test

import (
	"context"
	"errors"
	"testing"

	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/driver"
)

type testPrepared struct{ driver.BasePrepared }
type testHandle struct{ driver.BaseHandle }
type testDriver struct{ driver.BaseDriver }

func (testDriver) Prepare(context.Context, api.RunSpec) (driver.Prepared, error) {
	return testPrepared{}, nil
}

func (testDriver) Start(context.Context, driver.Prepared) (driver.Handle, error) {
	return testHandle{}, nil
}

func (testDriver) Wait(context.Context, driver.Handle) (api.Event, error) {
	return api.Event{State: api.StateSucceeded}, nil
}

func (testDriver) Signal(context.Context, driver.Handle, api.Signal) error { return nil }
func (testDriver) Cancel(context.Context, driver.Handle) error             { return nil }

func TestEmbeddedBaseTypesSatisfyInterfaces(t *testing.T) {
	var (
		_ driver.Prepared = testPrepared{}
		_ driver.Handle   = testHandle{}
		_ driver.Driver   = testDriver{}
	)
}

func TestUnimplementedDriver_ReturnsSentinelError(t *testing.T) {
	d := driver.UnimplementedDriver{}

	if _, err := d.Prepare(context.Background(), api.RunSpec{}); !errors.Is(err, driver.ErrUnimplemented) {
		t.Fatalf("Prepare error = %v, want ErrUnimplemented", err)
	}
	if _, err := d.Start(context.Background(), testPrepared{}); !errors.Is(err, driver.ErrUnimplemented) {
		t.Fatalf("Start error = %v, want ErrUnimplemented", err)
	}
	if _, err := d.Wait(context.Background(), testHandle{}); !errors.Is(err, driver.ErrUnimplemented) {
		t.Fatalf("Wait error = %v, want ErrUnimplemented", err)
	}
	if err := d.Signal(context.Background(), testHandle{}, api.Signal{}); !errors.Is(err, driver.ErrUnimplemented) {
		t.Fatalf("Signal error = %v, want ErrUnimplemented", err)
	}
	if err := d.Cancel(context.Background(), testHandle{}); !errors.Is(err, driver.ErrUnimplemented) {
		t.Fatalf("Cancel error = %v, want ErrUnimplemented", err)
	}
}
