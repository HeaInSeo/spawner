package driver

import (
	"context"
	"errors"

	"github.com/HeaInSeo/spawner/pkg/api"
)

// TODO actor 처럼 하는 게 어떤지 생각해보자.

var ErrUnimplemented = errors.New("driver: unimplemented")

// 봉인을 위한 비공개 타입
type _sealed struct{}

type preparedMarker interface{ preparedMarker(_sealed) }

type BasePrepared struct{}

func (BasePrepared) preparedMarker(_sealed) {}

type Prepared interface {
	preparedMarker(_sealed)
	// 필요 시: Token() Token, Kind() string 등 추가 가능
}

type handleMarker interface{ handleMarker(_sealed) }

type BaseHandle struct{}

func (BaseHandle) handleMarker(_sealed) {}

type Handle interface {
	handleMarker(_sealed)
	// 필요 시: Token()/ID/Kind 등 추가 가능
}

type driverMarker interface{ driverMarker(_sealed) }

type BaseDriver struct{}

func (BaseDriver) driverMarker(_sealed) {}

type Driver interface {
	driverMarker(_sealed)
	Prepare(ctx context.Context, spec api.RunSpec) (Prepared, error)
	Start(ctx context.Context, p Prepared) (Handle, error)
	Wait(ctx context.Context, h Handle) (api.Event, error)
	Signal(ctx context.Context, h Handle, sig api.Signal) error
	Cancel(ctx context.Context, h Handle) error
}

type UnimplementedDriver struct{ BaseDriver }

func (UnimplementedDriver) Prepare(context.Context, api.RunSpec) (Prepared, error) {
	return nil, ErrUnimplemented
}
func (UnimplementedDriver) Start(context.Context, Prepared) (Handle, error) {
	return nil, ErrUnimplemented
}
func (UnimplementedDriver) Wait(context.Context, Handle) (api.Event, error) {
	return api.Event{}, ErrUnimplemented
}
func (UnimplementedDriver) Signal(context.Context, Handle, api.Signal) error { return ErrUnimplemented }
func (UnimplementedDriver) Cancel(context.Context, Handle) error             { return ErrUnimplemented }
