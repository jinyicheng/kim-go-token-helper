package tokenHelper

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"
)

// TokenRefresher 中心化Token刷新器
// 全局只需一个活跃实例，支持多实例主备高可用
type TokenRefresher struct {
	rdb         *redis.Client
	refreshFunc RefreshFunc // 业务注入的刷新函数
	config      *Config

	instanceID string
	stopChan   chan struct{}
	stopOnce   sync.Once

	cacheKey       string
	masterLockKey  string
	refreshChannel string

	unlockScript *redis.Script
	renewScript  *redis.Script
}

// NewTokenRefresher 创建刷新器实例
// rdb: Redis客户端
// refreshFunc: 业务方实现的第三方Token获取函数
// opts: 可选配置项
func NewTokenRefresher(rdb *redis.Client, refreshFunc RefreshFunc, opts ...Option) *TokenRefresher {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// 生成实例唯一ID
	b := make([]byte, 16)
	_, _ = rand.Read(b)

	r := &TokenRefresher{
		rdb:            rdb,
		refreshFunc:    refreshFunc,
		config:         cfg,
		instanceID:     fmt.Sprintf("%x", b),
		stopChan:       make(chan struct{}),
		cacheKey:       cfg.KeyPrefix + ":cache",
		masterLockKey:  cfg.KeyPrefix + ":master:lock",
		refreshChannel: cfg.KeyPrefix + ":refresh:channel",
		// 安全解锁脚本：仅锁持有者可删除
		unlockScript: redis.NewScript(`
			if redis.call("GET", KEYS[1]) == ARGV[1] then
				return redis.call("DEL", KEYS[1])
			end
			return 0
		`),
		// 安全续期脚本：仅锁持有者可续期
		renewScript: redis.NewScript(`
			if redis.call("GET", KEYS[1]) == ARGV[1] then
				return redis.call("EXPIRE", KEYS[1], ARGV[2])
			end
			return 0
		`),
	}

	return r
}

// Start 启动刷新服务，阻塞直到抢主成功并开始运行
// 传入ctx可控制服务生命周期
func (r *TokenRefresher) Start(ctx context.Context) error {
	fmt.Printf("[token-refresher] 启动，Key前缀: %s，开始抢占主节点...\n", r.config.KeyPrefix)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopChan:
			return nil
		default:
		}

		// 抢占主节点锁
		lockSuccess, err := r.rdb.SetNX(ctx, r.masterLockKey, r.instanceID, r.config.MasterLockExpire).Result()
		if err != nil {
			fmt.Printf("[token-refresher] 抢主异常: %v，%v后重试\n", err, r.config.RetryInterval)
			time.Sleep(r.config.RetryInterval)
			continue
		}

		if !lockSuccess {
			// 备机待机，定期重试抢主
			time.Sleep(r.config.LockRenewInterval)
			continue
		}

		fmt.Println("[token-refresher] 成为主节点，开始接管Token刷新")

		// 启动锁续期看门狗
		go r.keepLockAlive(ctx)

		// 启动立即刷新一次
		if err := r.refreshWithRetry(ctx); err != nil {
			fmt.Printf("[token-refresher] 首次刷新失败: %v\n", err)
		}

		// 启动定时巡检
		go r.startCheckLoop(ctx)
		return nil
	}
}

// Stop 停止刷新服务，幂等安全
func (r *TokenRefresher) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})
}

// 锁续期看门狗
func (r *TokenRefresher) keepLockAlive(ctx context.Context) {
	ticker := time.NewTicker(r.config.LockRenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = r.unlockScript.Run(ctx, r.rdb, []string{r.masterLockKey}, r.instanceID).Err()
			return
		case <-r.stopChan:
			_ = r.unlockScript.Run(ctx, r.rdb, []string{r.masterLockKey}, r.instanceID).Err()
			return
		case <-ticker.C:
			res, err := r.renewScript.Run(ctx, r.rdb,
				[]string{r.masterLockKey},
				r.instanceID,
				int(r.config.MasterLockExpire.Seconds()),
			).Int()
			if err != nil || res == 0 {
				fmt.Println("[token-refresher] 锁续期失败，丢失主节点身份")
				return
			}
		}
	}
}

// 定时巡检循环
func (r *TokenRefresher) startCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(r.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopChan:
			return
		case <-ticker.C:
			token, valid, err := r.loadTokenFromCache(ctx)
			if err != nil {
				fmt.Printf("[token-refresher] 读取缓存失败: %v\n", err)
				continue
			}

			needRefresh := !valid || time.Until(token.ExpireAt) < r.config.PreRefreshAhead
			if needRefresh {
				if err := r.refreshWithRetry(ctx); err != nil {
					fmt.Printf("[token-refresher] 刷新失败: %v\n", err)
				}
			}
		}
	}
}

// 带重试的刷新流程
func (r *TokenRefresher) refreshWithRetry(ctx context.Context) error {
	var lastErr error

	for i := 0; i < r.config.MaxRefreshRetry; i++ {
		newToken, err := r.refreshFunc(ctx)
		if err != nil {
			lastErr = err
			fmt.Printf("[token-refresher] 第%d次调用第三方接口失败: %v\n", i+1, err)
			time.Sleep(r.config.RetryInterval)
			continue
		}

		ttl := time.Until(newToken.ExpireAt)
		if ttl <= 0 {
			ttl = time.Minute
		}

		data, _ := json.Marshal(newToken)
		if err := r.rdb.Set(ctx, r.cacheKey, data, ttl).Err(); err != nil {
			return fmt.Errorf("写入缓存失败: %w", err)
		}

		_ = r.rdb.Publish(ctx, r.refreshChannel, "ok").Err()
		fmt.Println("[token-refresher] Token刷新成功，已广播通知")
		return nil
	}

	return lastErr
}

// 从缓存读取Token
func (r *TokenRefresher) loadTokenFromCache(ctx context.Context) (*ThirdToken, bool, error) {
	var data []byte
	var err error
	data, err = r.rdb.Get(ctx, r.cacheKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		return nil, false, err
	}

	var token ThirdToken
	if err = json.Unmarshal(data, &token); err != nil {
		return nil, false, err
	}

	if time.Now().After(token.ExpireAt) {
		return nil, false, nil
	}
	return &token, true, nil
}
