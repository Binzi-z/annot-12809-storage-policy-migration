// Command migrator 在本机运行多租户策略迁移演示。它只使用模拟租户与
// 本地嵌入式存储，不访问网络。默认数据目录位于当前工作区 ./.migrator-data。
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"multitenantmigration/internal/demo"
)

func main() {
	dataDir := flag.String("data-dir", defaultDataDir(), "嵌入式 WAL/快照的数据目录")
	workers := flag.Int("workers", 2, "迁移 worker 池大小")
	maxAttempts := flag.Int("max-attempts", 5, "单步骤重试预算硬上界")
	flag.Parse()

	results, err := demo.Run(demo.Config{
		DataDir:     *dataDir,
		Workers:     *workers,
		MaxAttempts: *maxAttempts,
		Out:         os.Stdout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "启动失败: %v\n", err)
		os.Exit(2)
	}
	failed := 0
	for _, r := range results {
		if r.Err != nil {
			failed++
		}
	}
	if failed > 0 {
		fmt.Fprintf(os.Stdout, "%d/%d 个场景失败（数据保留在 %s 供审计）\n", failed, len(results), *dataDir)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "全部 %d 个场景通过。审计数据保留在 %s\n", len(results), *dataDir)
}

func defaultDataDir() string {
	wd, err := os.Getwd()
	if err != nil {
		return filepath.Join(os.TempDir(), "migrator-data")
	}
	return filepath.Join(wd, ".migrator-data")
}
