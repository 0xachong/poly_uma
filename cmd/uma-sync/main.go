// uma-sync：UMA OO 数据同步 + HTTP 查询服务
//
// 用法：
//
//	uma-sync -wss wss://polygon.xxx/KEY [选项]
//
// 环境变量优先级低于命令行 flag。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/polymas/poly_uma/internal/api"
	"github.com/polymas/poly_uma/internal/audit"
	"github.com/polymas/poly_uma/internal/store"
	"github.com/polymas/poly_uma/internal/syncer"
	"github.com/polymas/poly_uma/internal/uma"
)

func main() {
	wss := flag.String("wss", envOr("POLYGON_WSS_URL", ""), "Polygon WebSocket URL（wss://...）")
	rpc := flag.String("rpc", envOr("POLYGON_RPC_URL", ""), "HTTP RPC URL（空则从 -wss 推导）")
	sqlitePath := flag.String("sqlite", envOr("SQLITE_PATH", "uma_oo_events.sqlite"), "本地 SQLite 路径")
	mysqlDSN := flag.String("mysql", envOr("MYSQL_DSN", ""), "MySQL DSN（可选，用于审计）")
	apiAddr := flag.String("api-addr", envOr("API_ADDR", "0.0.0.0:7002"), "HTTP 监听地址")
	reconnect := flag.Duration("reconnect-delay", 10*time.Second, "断线初始重连间隔（指数退避至 60s）")
	proxy := flag.String("proxy", envOr("HTTP_PROXY", ""), "Gamma API 代理（可选）")
	flag.Parse()

	if *wss == "" {
		log.Fatal("[ERROR] 必须提供 -wss WSS 地址")
	}

	// ── 本地 SQLite ──────────────────────────────────────────────────────────
	db, err := store.Open(*sqlitePath)
	if err != nil {
		log.Fatalf("[ERROR] 打开 SQLite %s: %v", *sqlitePath, err)
	}
	defer db.Close()
	log.Printf("[INFO] SQLite: %s", *sqlitePath)

	// ── 最近 12h 尾盘/争议事件内存缓存（propose + dispute），供 API 快速响应 ─────
	recent := store.NewRecentCache()
	go recent.RunEvict(10 * time.Minute)

	// ── 远程 MySQL 审计（可选）──────────────────────────────────────────────
	var au *audit.MySQL
	if *mysqlDSN != "" {
		au, err = audit.Open(*mysqlDSN)
		if err != nil {
			log.Printf("[WARN] MySQL 连接失败（跳过审计）: %v", err)
		} else {
			defer au.Close()
			log.Printf("[INFO] MySQL 审计库已连接")
		}
	}

	// ── 上下文 + 信号 ────────────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("[INFO] 收到退出信号，正在关闭…")
		cancel()
	}()

	// ── 同步 goroutine ───────────────────────────────────────────────────────
	httpURL := *rpc
	if httpURL == "" {
		httpURL = uma.WssToHttp(*wss)
	}
	cfg := syncer.Config{
		WssURL:         *wss,
		HttpRPCURL:     httpURL,
		ReconnectDelay: *reconnect,
		ProxyURL:       *proxy,
	}
	go func() {
		syncer.Run(ctx, cfg, db, au, recent)
	}()

	// ── HTTP API（前台阻塞）──────────────────────────────────────────────────
	if err := api.ListenAndServe(ctx, *apiAddr, db, recent); err != nil {
		log.Printf("[INFO] HTTP 服务退出: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
