package imp

import (
	"context"
	"errors"
	"testing"

	"github.com/HeaInSeo/spawner/pkg/api"
	sErr "github.com/HeaInSeo/spawner/pkg/error"
)

func TestNopDriver_AlwaysReturnsBackendUnavailable(t *testing.T) {
	d := NopDriver{}

	if _, err := d.Prepare(context.Background(), api.RunSpec{}); !errors.Is(err, sErr.ErrBackendUnavailable) {
		t.Fatalf("Prepare error = %v", err)
	}
	if _, err := d.Start(context.Background(), testPrepared{}); !errors.Is(err, sErr.ErrBackendUnavailable) {
		t.Fatalf("Start error = %v", err)
	}
	if _, err := d.Wait(context.Background(), testHandle{}); !errors.Is(err, sErr.ErrBackendUnavailable) {
		t.Fatalf("Wait error = %v", err)
	}
	if err := d.Signal(context.Background(), testHandle{}, api.Signal{}); !errors.Is(err, sErr.ErrBackendUnavailable) {
		t.Fatalf("Signal error = %v", err)
	}
	if err := d.Cancel(context.Background(), testHandle{}); !errors.Is(err, sErr.ErrBackendUnavailable) {
		t.Fatalf("Cancel error = %v", err)
	}
}
