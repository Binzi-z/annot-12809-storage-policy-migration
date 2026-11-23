package demo

import (
	"bytes"
	"path/filepath"
	"testing"
)

// TestAllScenariosPass 让 go test 也能驱动完整端到端演示：六个场景的
// 不变量全部被程序化断言，而不只是打印日志。
func TestAllScenariosPass(t *testing.T) {
	var buf bytes.Buffer
	results, err := Run(Config{
		DataDir:     filepath.Join(t.TempDir(), "demo"),
		Workers:     2,
		MaxAttempts: 5,
		Out:         &buf,
	})
	if err != nil {
		t.Fatalf("run: %v\noutput:\n%s", err, buf.String())
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("scenario %q failed: %v\noutput:\n%s", r.Title, r.Err, buf.String())
		}
	}
	t.Logf("\n%s", buf.String())
}
