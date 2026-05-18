package frontdoor

import (
	"strings"

	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/policy"
)

type Rule struct {
	// predicate 이고, 일종의 조건함수 임.
	Match func(ResolveInput) bool
	// spawnKey 를 생성 하는 함수.
	SpawnKeyFn func(ResolveInput) string
	// 실제로 명령을 수행하는 정보를 가져와서 담는 함수.
	BuildCmd func(ResolveInput) (api.Command, error)
}

// 기본 타입

type Pred func(in ResolveInput) bool

// 조합기

func And(ps ...Pred) Pred {
	return func(in ResolveInput) bool {
		for _, p := range ps {
			if !p(in) {
				return false
			} // 단락 평가
		}
		return true
	}
}
func Or(ps ...Pred) Pred {
	return func(in ResolveInput) bool {
		for _, p := range ps {
			if p(in) {
				return true
			}
		}
		return false
	}
}

func Not(p Pred) Pred { return func(in ResolveInput) bool { return !p(in) } }

func HasRPC(name string) Pred { return func(in ResolveInput) bool { return in.Meta.RPC == name } }

func HasTenant() Pred { return func(in ResolveInput) bool { return in.Meta.TenantID != "" } }

func TenantIn(allow ...string) Pred {
	set := map[string]struct{}{}
	for _, t := range allow {
		set[t] = struct{}{}
	}
	return func(in ResolveInput) bool { _, ok := set[in.Meta.TenantID]; return ok }
}

// 제네릭 타입 매칭

func HasReqType[T any]() Pred {
	return func(in ResolveInput) bool { _, ok := in.Req.(*T); return ok }
}

// 헤더/메타 검사(있다면)

/* func HeaderEq(k, v string) Pred {
	return func(in ResolveInput) bool { return in.Meta.Headers[k] == v }
} */

func MetaEq(key, val string) Pred {
	key = strings.ToLower(key)
	return func(in ResolveInput) bool {
		v, ok := in.Meta.Get(key)
		return ok && v == val
	}
}

// 사용 예시

// HTTP 헤더 X-Cmd: Cancel
// Match: MetaEq("hdr.x-cmd", "Cancel")

// gRPC 메타 x-cmd: Cancel
// Match: MetaEq("md.x-cmd", "Cancel")

// 시스템 프린시펄 제외

func NotSystemPrincipal() Pred {
	return func(in ResolveInput) bool { return !strings.HasPrefix(in.Meta.Principal, "system/") }
}

// 사용 예시
/*
Match: And(
	HasRPC("RunE"),
	HasReqType[api.RunSpec](),
	TenantIn("geno-core", "clinic-a"),
	NotSystemPrincipal(),
)
*/

// Cancel 규칙 예시 참고만 할뿐.

var CancelRule = Rule{
	Match: func(in ResolveInput) bool { return in.Meta.RPC == "Cancel" },
	SpawnKeyFn: func(in ResolveInput) string {
		cr := in.Req.(*api.CancelReq)
		if cr.SpawnID != "" {
			return cr.SpawnID
		}
		if cr.IdemKey != "" {
			return lookupSpawnID(cr.IdemKey, in.Meta.TenantID)
		}
		return "" // 라우터에서 에러로 처리
	},
	BuildCmd: func(in ResolveInput) (api.Command, error) {
		cr := in.Req.(*api.CancelReq)
		pol := policy.DefaultPolicyB(0) // 혹은 policyFn에서 Cancel에 맞는 일부만 사용
		return api.NewCancelCommand(cr, pol)
	},
}

func lookupSpawnID(idemKey, tenantID string) string {
	/*
		if id, ok := idemIndex.Lookup(tenantID, idemKey); ok {
			return id
		}
	*/
	return ""
}
