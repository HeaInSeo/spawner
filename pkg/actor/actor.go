package actor

import (
	"context"

	"github.com/HeaInSeo/spawner/pkg/api"
)

type Actor interface {
	EnqueueTry(api.Command) bool
	EnqueueCtx(context.Context, api.Command) bool
	OnIdle(func())
	OnTerminate(func())
	Loop(context.Context)
}
