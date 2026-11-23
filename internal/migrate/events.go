package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Envelope 是所有 WAL 记录的统一外壳。Payload 为具体事件的 JSON；
// PrevHash/Hash 构成逐条记录的哈希链，任何被篡改或错位的记录都会在
// 重放时被发现（区别于存储层 CRC 的位腐检测，这里检测的是逻辑完整性）。
type Envelope struct {
	Seq      uint64          `json:"seq"`
	Kind     string          `json:"kind"`
	Payload  json.RawMessage `json:"payload"`
	PrevHash string          `json:"prev"`
	Hash     string          `json:"hash"`
}

// 事件类型。命名即审计语义，写入后永不复用、永不就地修改。
const (
	evRegister          = "resource.registered"
	evTransition        = "state.transition"
	evLegacyWrite       = "legacy.write"
	evMirrorApplied     = "v2.mirror_applied"
	evDualDrainComplete = "dualwrite.drained"
	evRotate            = "revocation.rotated"
	evRollback          = "state.rollback"
	evSnapshotImported  = "snapshot.imported"
	evRetryScheduled    = "retry.scheduled"
	evRetryReset        = "retry.reset"
	evRetryExhausted    = "retry.exhausted"
	evQuarantine        = "resource.quarantined"
)

// ---- 具体事件载荷 ----

type resourceKeyed struct {
	TenantID string `json:"tenant"`
	Prefix   string `json:"prefix"`
}

type transitionPayload struct {
	resourceKeyed
	From   State  `json:"from"`
	To     State  `json:"to"`
	Reason string `json:"reason"`
}

type legacyWritePayload struct {
	resourceKeyed
	LegacySeq uint64 `json:"legacy_seq"`
	Detail    string `json:"detail"`
}

type mirrorPayload struct {
	resourceKeyed
	LegacySeq uint64 `json:"legacy_seq"`
	MirrorSeq uint64 `json:"mirror_seq"`
}

type drainPayload struct {
	resourceKeyed
	Watermark uint64 `json:"watermark"`
}

type rotatePayload struct {
	resourceKeyed
	OldGenFloor uint64 `json:"old_gen_floor"`
	NewGenFloor uint64 `json:"new_gen_floor"`
	Reason      string `json:"reason"`
}

type rollbackPayload struct {
	resourceKeyed
	From     State  `json:"from"`
	To       State  `json:"to"`
	GenFloor uint64 `json:"gen_floor_held"` // 回滚时携带并校验的世代地板
	Reason   string `json:"reason"`
}

type snapshotPayload struct {
	resourceKeyed
	Source    string `json:"source"`
	GenFloor  uint64 `json:"gen_floor"`
	Watermark uint64 `json:"watermark"`
}

type retryPayload struct {
	resourceKeyed
	Attempt   int    `json:"attempt"`
	BackoffMs int    `json:"backoff_ms"`
	Reason    string `json:"reason"`
}

type quarantinePayload struct {
	resourceKeyed
	Reason string `json:"reason"`
}

func keyed(k ResourceKey) resourceKeyed {
	return resourceKeyed{TenantID: k.TenantID, Prefix: k.Prefix}
}

// seal 序列化事件并计算哈希链。哈希输入包含序号、类型、载荷与上一条
// 哈希，因此重排、替换、删条都会断链。
func seal(seq uint64, kind string, payload any, prevHash string) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, err
	}
	env := Envelope{Seq: seq, Kind: kind, Payload: raw, PrevHash: prevHash}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%s|%s|%s", seq, kind, raw, prevHash)))
	env.Hash = hex.EncodeToString(sum[:])
	return env, nil
}

// VerifyChain 校验一条信封链的哈希连续性。
func VerifyChain(envs []Envelope) error {
	prev := ""
	for _, env := range envs {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%s|%s|%s", env.Seq, env.Kind, env.Payload, env.PrevHash)))
		want := hex.EncodeToString(sum[:])
		if env.PrevHash != prev {
			return fmt.Errorf("migrate: hash chain broken at seq %d: prev mismatch", env.Seq)
		}
		if env.Hash != want {
			return fmt.Errorf("migrate: hash chain broken at seq %d: content mismatch", env.Seq)
		}
		prev = env.Hash
	}
	return nil
}

// encodeBatch 把若干已封签事件编码为一个 WAL 批量。
func encodeBatch(envs []Envelope) ([][]byte, error) {
	out := make([][]byte, 0, len(envs))
	for _, env := range envs {
		raw, err := json.Marshal(env)
		if err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	return out, nil
}

func decodeEnvelope(raw []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}
