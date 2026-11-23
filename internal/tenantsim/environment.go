// Package tenantsim 构造只存在于本地进程内的模拟多租户环境：两个并行
// 运行的策略写入端（V1 旧端 / V2 新端）、资源版本与撤销世代状态，以及
// 一个会把旧数据“追赶”回来的模拟副本。所有数据都在内存中，不接触网络。
package tenantsim

import (
	"fmt"
	"sort"
	"sync"

	"multitenantmigration/internal/migrate"
	"multitenantmigration/internal/policy"
)

// Tenant 是一个模拟租户。
type Tenant struct {
	ID    string
	Roles []string
	// Prefixes 是该租户受管资源的前缀。
	Prefixes []string
}

// ResourceMeta 记录资源的当前版本与撤销世代（V2 维度）。
type ResourceMeta struct {
	Version       uint64
	RevocationGen uint64
}

// Environment 是完整的模拟环境，实现 policy.V1Store / policy.V2Store。
type Environment struct {
	mu sync.RWMutex

	tenants map[string]*Tenant
	v1rules map[string][]policy.V1Rule
	v2rules map[string][]policy.V2Rule
	// resourceMeta: tenantID|path -> meta
	resourceMeta map[string]ResourceMeta
	// coord 提供迁移状态与世代地板；撤销生效时地板决定规则的最低有效世代。
	coord *migrate.Coordinator
}

// New 创建空环境。
func New(coord *migrate.Coordinator) *Environment {
	return &Environment{
		tenants:      map[string]*Tenant{},
		v1rules:      map[string][]policy.V1Rule{},
		v2rules:      map[string][]policy.V2Rule{},
		resourceMeta: map[string]ResourceMeta{},
		coord:        coord,
	}
}

// AddTenant 注册模拟租户并为其每个 (角色, 前缀) 建立一条 V1 规则与一条
// 同内容的 V2 规则（初始版本 1、撤销世代 1）。
func (e *Environment) AddTenant(t *Tenant) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.tenants[t.ID]; exists {
		return fmt.Errorf("tenantsim: duplicate tenant %s", t.ID)
	}
	e.tenants[t.ID] = t
	for _, prefix := range t.Prefixes {
		for _, role := range t.Roles {
			rule := policy.V1Rule{
				TenantID: t.ID, Role: role, Prefix: prefix,
				CanRead: true, CanWrite: role == t.Roles[0],
			}
			e.v1rules[t.ID] = append(e.v1rules[t.ID], rule)
			e.v2rules[t.ID] = append(e.v2rules[t.ID], policy.V2Rule{
				V1Rule: rule, ResourceVersion: 1, RevocationGen: 1,
			})
		}
		e.resourceMeta[metaKey(t.ID, prefix)] = ResourceMeta{Version: 1, RevocationGen: 1}
	}
	return nil
}

func metaKey(tenant, path string) string { return tenant + "|" + path }

// V1Write 是旧写入端入口：直接改写 V1 规则，不携带版本/世代。
func (e *Environment) V1Write(rule policy.V1Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.v1rules[rule.TenantID] = upsertV1(e.v1rules[rule.TenantID], rule)
}

// UpdateV2Rule 按租户+角色+前缀定位并整体替换一条 V2 规则（找不到则追加）。
func (e *Environment) UpdateV2Rule(rule policy.V2Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.v2rules[rule.TenantID] = upsertV2(e.v2rules[rule.TenantID], rule)
}

// V2Write 是新写入端入口：携带资源版本与撤销世代；低于迁移协调器世代
// 地板的写入会被拒绝（防止任何来源把已撤销规则写回）。
func (e *Environment) V2Write(rule policy.V2Rule) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.coord != nil {
		if rs, ok := e.coord.Resource(migrate.ResourceKey{
			TenantID: rule.TenantID, Prefix: rule.Prefix,
		}); ok && rule.RevocationGen < rs.GenFloor {
			return fmt.Errorf(
				"tenantsim: V2 write below generation floor rejected (rule gen %d < floor %d)",
				rule.RevocationGen, rs.GenFloor)
		}
	}
	e.v2rules[rule.TenantID] = upsertV2(e.v2rules[rule.TenantID], rule)
	return nil
}

// BumpResourceVersion 模拟资源内容更新带来的版本递增。
func (e *Environment) BumpResourceVersion(tenant, path string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.resourceMeta[metaKey(tenant, path)]
	m.Version++
	e.resourceMeta[metaKey(tenant, path)] = m
}

// Revoke 把资源撤销世代抬到 newGen。已签发的 V2 规则保持旧世代不变，
// 因而立刻落在资源世代之后而失效——这正是“撤销”的语义；重新授权必须
// 显式通过 V2Write 签出携带新世代的规则。V1 端无法表达世代，撤销在
// V1 视图下不可见，这也是必须完成迁移的原因之一。
func (e *Environment) Revoke(tenant, prefix string, newGen uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.resourceMeta[metaKey(tenant, prefix)]
	if newGen <= m.RevocationGen {
		return
	}
	m.RevocationGen = newGen
	e.resourceMeta[metaKey(tenant, prefix)] = m
}

// ReplicateStaleRulesFromV1 模拟“过期副本追赶”：它只能看到 V1 旧规则，
// 试图把世代 1 的 V2 规则重新写回来。返回 V2 端实际接受/拒绝的数量。
func (e *Environment) ReplicateStaleRulesFromV1(tenant string) (accepted, rejected int) {
	e.mu.RLock()
	snapshot := append([]policy.V1Rule(nil), e.v1rules[tenant]...)
	e.mu.RUnlock()
	for _, r1 := range snapshot {
		stale := policy.V2Rule{V1Rule: r1, ResourceVersion: 1, RevocationGen: 1}
		if err := e.V2Write(stale); err != nil {
			rejected++
			continue
		}
		accepted++
	}
	return accepted, rejected
}

// ---- policy.V1Store / policy.V2Store 实现 ----

// V1Rules 返回租户的 V1 规则快照（按前缀、角色排序保证输出稳定）。
func (e *Environment) V1Rules(tenant string) []policy.V1Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := append([]policy.V1Rule(nil), e.v1rules[tenant]...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Prefix != out[j].Prefix {
			return out[i].Prefix < out[j].Prefix
		}
		return out[i].Role < out[j].Role
	})
	return out
}

// V2Rules 返回租户的 V2 规则快照。
func (e *Environment) V2Rules(tenant string) []policy.V2Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := append([]policy.V2Rule(nil), e.v2rules[tenant]...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Prefix != out[j].Prefix {
			return out[i].Prefix < out[j].Prefix
		}
		return out[i].Role < out[j].Role
	})
	return out
}

// ResourceState 实现 policy.V2Store：资源世代同时受模拟状态与协调器
// 世代地板约束（取较大者），确保撤销地板在任何视图下都生效。
func (e *Environment) ResourceState(tenant, path string) (uint64, uint64, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	// 允许用具体路径查询前缀级资源。
	var (
		meta ResourceMeta
		ok   bool
	)
	for prefix, m := range e.resourceMeta {
		// map key 形如 tenant|prefix
		storedTenant, storedPrefix := splitMetaKey(prefix)
		if storedTenant == tenant && len(path) >= len(storedPrefix) &&
			path[:len(storedPrefix)] == storedPrefix {
			meta, ok = m, true
			break
		}
	}
	if !ok {
		return 0, 0, false
	}
	if e.coord != nil {
		if rs, exists := e.coord.Resource(migrate.ResourceKey{TenantID: tenant, Prefix: bestPrefix(e, tenant, path)}); exists && rs.GenFloor > meta.RevocationGen {
			meta.RevocationGen = rs.GenFloor
		}
	}
	return meta.Version, meta.RevocationGen, true
}

func bestPrefix(e *Environment, tenant, path string) string {
	best := ""
	for key := range e.resourceMeta {
		t, p := splitMetaKey(key)
		if t == tenant && len(path) >= len(p) && path[:len(p)] == p && len(p) > len(best) {
			best = p
		}
	}
	return best
}

func splitMetaKey(k string) (string, string) {
	for i := 0; i < len(k); i++ {
		if k[i] == '|' {
			return k[:i], k[i+1:]
		}
	}
	return k, ""
}

func upsertV1(rules []policy.V1Rule, rule policy.V1Rule) []policy.V1Rule {
	for i := range rules {
		if rules[i].Role == rule.Role && rules[i].Prefix == rule.Prefix {
			rules[i] = rule
			return rules
		}
	}
	return append(rules, rule)
}

func upsertV2(rules []policy.V2Rule, rule policy.V2Rule) []policy.V2Rule {
	for i := range rules {
		if rules[i].Role == rule.Role && rules[i].Prefix == rule.Prefix {
			rules[i] = rule
			return rules
		}
	}
	return append(rules, rule)
}

// Tenants 返回已注册租户 ID 的排序切片。
func (e *Environment) Tenants() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.tenants))
	for id := range e.tenants {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
