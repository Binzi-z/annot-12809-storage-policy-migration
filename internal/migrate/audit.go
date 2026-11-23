package migrate

import (
	"sort"

	"multitenantmigration/internal/storage"
)

// AuditEvent 是面向审计导出的扁平记录（不暴露哈希实现细节但保留链字段）。
type AuditEvent struct {
	Seq      uint64
	Kind     string
	Payload  string
	PrevHash string
	Hash     string
}

// AuditTrail 按顺序读出全部已提交事件并校验哈希链。
// 抢救模式下返回损坏点之前的事件，错误中保留 ErrCorruption 供调用方区分。
func (c *Coordinator) AuditTrail() ([]AuditEvent, error) {
	var out []AuditEvent
	var envs []Envelope
	read := func(_ uint64, batch [][]byte) error {
		for _, raw := range batch {
			env, err := decodeEnvelope(raw)
			if err != nil {
				return err
			}
			envs = append(envs, env)
		}
		return nil
	}
	var err error
	if c.readOnly {
		_, err = c.store.ReplayPrefix(read)
	} else {
		err = c.store.Replay(read)
	}
	if err != nil && !storage.IsCorruption(err) {
		return nil, err
	}
	if verifyErr := VerifyChain(envs); verifyErr != nil {
		return nil, verifyErr
	}
	for _, env := range envs {
		out = append(out, AuditEvent{
			Seq: env.Seq, Kind: env.Kind, Payload: string(env.Payload),
			PrevHash: env.PrevHash, Hash: env.Hash,
		})
	}
	return out, err
}

// AuditSummary 返回按事件类型聚合的计数，供演示与巡检快速核对。
func (c *Coordinator) AuditSummary() (map[string]int, error) {
	events, err := c.AuditTrail()
	summary := map[string]int{}
	for _, e := range events {
		summary[e.Kind]++
	}
	return summary, err
}

// SortedAuditKinds 便于稳定输出。
func SortedAuditKinds(summary map[string]int) []string {
	out := make([]string, 0, len(summary))
	for k := range summary {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
