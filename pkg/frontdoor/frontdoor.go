package frontdoor

import (
	"context"
	"fmt"
	"strings"

	"github.com/HeaInSeo/spawner/pkg/api"
	sErr "github.com/HeaInSeo/spawner/pkg/error"
)

type ResolveInput struct {
	Req  any
	Meta MetaContext
}

func (in ResolveInput) Clone() ResolveInput {
	in.Meta = in.Meta.Clone()
	return in
}

type ResolveResult struct {
	SpawnKey string
	Cmd      api.Command
}

type FrontDoor interface {
	// Resolve 여기서 SpawnKey 생성해서, 단일 actor 로 요청이 가도록 해야한다.
	// 여기서 단일 actor 란, 동일한 SpawnKey 로 라우팅 되는 actor 를 의미하는데, 그 이유는 여러 작업들이 일어날 수 있고, 여러 사용자가 있을 수 있기 때문이다.
	Resolve(ctx context.Context, in ResolveInput) (ResolveResult, error)
}

type TableFrontDoor struct{ rules []Rule }

func NewTableFrontDoor(rules ...Rule) *TableFrontDoor {
	return &TableFrontDoor{rules: rules}
}

// Resolve TODO 적용 규칙에 대해서는 생각해줘야 함.

func (fd *TableFrontDoor) Resolve(ctx context.Context, in ResolveInput) (ResolveResult, error) {
	in = in.Clone()
	for _, rl := range fd.rules {
		if rl.Match(in) {
			cmd, err := rl.BuildCmd(in)
			if err != nil {
				return ResolveResult{}, err
			}
			spawnKey := strings.TrimSpace(rl.SpawnKeyFn(in))
			if spawnKey == "" {
				return ResolveResult{}, fmt.Errorf("%w: rpc=%s", sErr.ErrInvalidSpawnKey, in.Meta.RPC)
			}
			cmd.Policy = cmd.Policy.WithDefaults()
			if err := cmd.Policy.Validate(); err != nil {
				return ResolveResult{}, err
			}
			return ResolveResult{
				SpawnKey: spawnKey,
				Cmd:      cmd,
			}, nil
		}
	}
	return ResolveResult{}, fmt.Errorf("%w: rpc=%s", sErr.ErrNoMatch, in.Meta.RPC)
}
