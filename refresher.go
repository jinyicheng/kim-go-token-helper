package tokenHelper

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// TokenRefresher 中心化Token刷新器
// 核心能力：
//   - 多实例主备高可用，全局仅一个实例负责刷新
//   - 锁自动续期，服务崩溃自动释放锁
//   - 退位广播触发秒级主备切换
//   - 缓存丢失自动检测并恢复
//   - 预刷新机制，业务无感知续期
//   - 刷新并发去重，避免重复调用第三方接口
type TokenRefresher struct {
	rdb         *redis.Client // Redis 客户端
	refreshFunc RefreshFunc   // 业务注入的第三方Token刷新函数
	config      *Config       // 配置项

	instanceID string        // 实例唯一标识，用于锁持有者校验
	stopChan   chan struct{} // 停止信号通道
	stopOnce   sync.Once     // 保证 Stop 方法幂等，重复调用不 panic
	refreshing atomic.Bool   // 全局刷新标记，原子操作，防止并发重复刷新

	// Redis Key 集合
	cacheKey        string // Token 缓存键
	masterLockKey   string // 主节点选举锁键
	refreshChannel  string // Token刷新完成广播频道
	abdicateChannel string // 主节点退位广播频道

	unlockScript *redis.Script // 安全解锁 Lua 脚本
	renewScript  *redis.Script // 安全续期 Lua 脚本
}

// NewTokenRefresher 创建刷新器实例
// 参数：
//
//	rdb: Redis 客户端实例
//	refreshFunc: 业务方实现的第三方Token获取函数
//	opts: 可选配置项
func NewTokenRefresher(rdb *redis.Client, refreshFunc RefreshFunc, opts ...Option) *TokenRefresher {
	// 加载默认配置并应用自定义选项
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// 生成 16 字节随机实例唯一 ID
	b := make([]byte, 16)
	_, _ = rand.Read(b)

	return &TokenRefresher{
		rdb:         rdb,
		refreshFunc: refreshFunc,
		config:      cfg,
		instanceID:  fmt.Sprintf("%x", b),
		stopChan:    make(chan struct{}),

		// 拼接完整 Redis Key
		cacheKey:        cfg.KeyPrefix + ":cache",
		masterLockKey:   cfg.KeyPrefix + ":master:lock",
		refreshChannel:  cfg.KeyPrefix + ":refresh:channel",
		abdicateChannel: cfg.KeyPrefix + ":master:abdicate",

		// Lua 脚本：安全解锁，仅锁持有者可以删除，避免误删其他实例的锁
		unlockScript: redis.NewScript(`
			if redis.call("GET", KEYS[1]) == ARGV[1] then
				return redis.call("DEL", KEYS[1])
			end
			return 0
		`),

		// Lua 脚本：安全续期，仅锁持有者可以续期，避免极端双主场景
		renewScript: redis.NewScript(`
			if redis.call("GET", KEYS[1]) == ARGV[1] then
				return redis.call("EXPIRE", KEYS[1], ARGV[2])
			end
			return 0
		`),
	}
}

// Start 启动刷新服务，常驻运行
// 特点：
//   - 丢失主节点后自动重新参与选举
//   - Redis 故障恢复后自动恢复服务
//   - 收到退位广播立即发起抢锁
func (r *TokenRefresher) Start(ctx context.Context) error {
	log.Printf("[token-refresher] 服务启动，Key前缀: %s\n", r.config.KeyPrefix)

	// 订阅退位广播频道，收到消息立即触发抢锁
	pubsub := r.rdb.Subscribe(ctx, r.abdicateChannel)
	defer pubsub.Close()
	abdicateCh := pubsub.Channel()

	// 外层主循环：只要服务未停止，就不断尝试抢主
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopChan:
			return nil
		default:
		}

		// 尝试抢占主节点锁
		isMaster, err := r.tryBecomeMaster(ctx)
		if err != nil {
			log.Printf("[token-refresher] 抢主异常: %v，%v后重试\n", err, r.config.RetryInterval)
			time.Sleep(r.config.RetryInterval)
			continue
		}

		// 抢锁成功，作为主节点运行
		if isMaster {
			log.Println("[token-refresher] 成为主节点，开始接管Token刷新")
			err = r.runAsMaster(ctx)
			if err != nil && !errors.Is(err, errLostMaster) {
				log.Printf("[token-refresher] 主节点运行异常: %v\n", err)
			}
			log.Println("[token-refresher] 丢失主节点身份，重新参与选举")
			time.Sleep(r.config.RetryInterval)
			continue
		}

		// 未抢到锁，作为备机待机
		// 两种情况触发下一次抢锁：
		// 1. 轮询时间到
		// 2. 收到主节点退位广播（秒级响应）
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopChan:
			return nil
		case <-time.After(r.config.MasterElectionInterval):
			// 定时轮询抢锁
		case <-abdicateCh:
			log.Println("[token-refresher] 收到主节点退位通知，立即发起抢锁")
		}
	}
}

// Stop 停止刷新服务，幂等安全
func (r *TokenRefresher) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})
}

// errLostMaster 丢失主节点身份错误
var errLostMaster = errors.New("lost master status")

// tryBecomeMaster 尝试抢占主节点锁
// 使用 SETNX 原子操作，保证全局只有一个实例能抢到锁
func (r *TokenRefresher) tryBecomeMaster(ctx context.Context) (bool, error) {
	return r.rdb.SetNX(ctx, r.masterLockKey, r.instanceID, r.config.MasterLockExpire).Result()
}

// runAsMaster 作为主节点运行完整流程
// 启动锁续期看门狗、立即刷新、定时巡检，直到丢失主节点或服务停止
func (r *TokenRefresher) runAsMaster(ctx context.Context) error {
	// 主节点丢失信号通道
	lostMaster := make(chan struct{})
	var once sync.Once
	// 通知主节点丢失，确保只关闭一次通道
	notifyLost := func() {
		once.Do(func() { close(lostMaster) })
	}

	// 启动锁续期看门狗协程
	go func() {
		defer notifyLost()
		r.keepLockAlive(ctx)
	}()

	// 上任立即执行一次刷新
	// 作用：冷启动初始化、Redis重启数据丢失后立即恢复
	go r.triggerRefresh(ctx)

	// 启动定时巡检协程，负责预刷新
	checkDone := make(chan struct{})
	go func() {
		defer close(checkDone)
		r.startCheckLoop(ctx, lostMaster)
	}()

	// 阻塞等待停止信号或丢失主节点
	select {
	case <-ctx.Done():
		r.releaseMaster(ctx)
		return ctx.Err()
	case <-r.stopChan:
		r.releaseMaster(ctx)
		return nil
	case <-lostMaster:
		// 丢失主节点时广播退位通知，让备机立刻接管
		_ = r.rdb.Publish(ctx, r.abdicateChannel, "abdicate").Err()
		return errLostMaster
	}
}

// releaseMaster 主动释放主节点
// 释放锁并广播退位通知，让备机秒级接管
func (r *TokenRefresher) releaseMaster(ctx context.Context) {
	_ = r.unlockScript.Run(ctx, r.rdb, []string{r.masterLockKey}, r.instanceID).Err()
	_ = r.rdb.Publish(ctx, r.abdicateChannel, "abdicate").Err()
}

// keepLockAlive 锁续期看门狗
// 功能：
//  1. 定期续期锁，保证正常运行时一直持有主节点身份
//  2. 连续失败达到阈值后判定丢失主节点
//  3. 顺带检测缓存是否丢失，丢失立即触发刷新
func (r *TokenRefresher) keepLockAlive(ctx context.Context) {
	ticker := time.NewTicker(r.config.LockRenewInterval)
	defer ticker.Stop()

	failCount := 0 // 连续失败计数

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopChan:
			return
		case <-ticker.C:
			// 原子校验持有者后续期
			res, err := r.renewScript.Run(ctx, r.rdb,
				[]string{r.masterLockKey},
				r.instanceID,
				int(r.config.MasterLockExpire.Seconds()),
			).Int()

			if err == nil && res == 1 {
				// 续期成功，重置失败计数
				failCount = 0

				// 顺带检测缓存Key是否存在，开销极低（一条EXISTS命令）
				// 用于快速发现缓存误删、Redis重启数据丢失等场景
				exist, _ := r.rdb.Exists(ctx, r.cacheKey).Result()
				if exist == 0 {
					log.Println("[token-refresher] 检测到缓存Key丢失，立即触发刷新")
					go r.triggerRefresh(ctx)
				}
				continue
			}

			// 续期失败，计数累加
			failCount++
			log.Printf("[token-refresher] 锁续期失败，连续失败次数: %d\n", failCount)

			// 达到阈值，判定丢失主节点，退出看门狗
			if failCount >= r.config.RenewFailThreshold {
				log.Println("[token-refresher] 锁续期连续失败，判定丢失主节点")
				return
			}
		}
	}
}

// startCheckLoop 定时巡检循环
// 负责预刷新：Token 快过期时提前刷新，业务无感知
func (r *TokenRefresher) startCheckLoop(ctx context.Context, lostMaster <-chan struct{}) {
	ticker := time.NewTicker(r.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopChan:
			return
		case <-lostMaster:
			return
		case <-ticker.C:
			// 读取当前Token状态
			token, valid, err := r.loadTokenFromCache(ctx)
			if err != nil {
				log.Printf("[token-refresher] 读取缓存失败: %v\n", err)
				continue
			}

			// 触发刷新条件：
			// 1. Token已失效
			// 2. Token剩余有效期小于预刷新阈值
			needRefresh := !valid || time.Until(token.ExpireAt) < r.config.PreRefreshAhead
			if needRefresh {
				go r.triggerRefresh(ctx)
			}
		}
	}
}

// triggerRefresh 异步触发刷新，原子去重
// 所有刷新入口统一走这里，保证同一时间只有一个刷新在执行
// 避免多处同时触发导致重复调用第三方接口
func (r *TokenRefresher) triggerRefresh(ctx context.Context) {
	// CAS 原子操作：正在刷新则直接跳过
	if !r.refreshing.CompareAndSwap(false, true) {
		return
	}
	defer r.refreshing.Store(false)

	if err := r.refreshWithRetry(ctx); err != nil {
		log.Printf("[token-refresher] 刷新执行失败: %v\n", err)
	}
}

// refreshWithRetry 带重试的完整刷新流程
// 调用第三方接口 → 写入缓存 → 广播通知
func (r *TokenRefresher) refreshWithRetry(ctx context.Context) error {
	var lastErr error

	for i := 0; i < r.config.MaxRefreshRetry; i++ {
		// 调用业务注入的第三方接口
		newToken, err := r.refreshFunc(ctx)
		if err != nil {
			lastErr = err
			log.Printf("[token-refresher] 第%d次调用第三方接口失败: %v\n", i+1, err)
			time.Sleep(r.config.RetryInterval)
			continue
		}

		// 计算剩余有效期，写入Redis缓存
		ttl := time.Until(newToken.ExpireAt)
		if ttl <= 0 {
			ttl = time.Minute // 兜底：防止第三方返回已过期的Token
		}

		data, _ := json.Marshal(newToken)
		if err := r.rdb.Set(ctx, r.cacheKey, data, ttl).Err(); err != nil {
			return fmt.Errorf("写入缓存失败: %w", err)
		}

		// 广播刷新完成信号，唤醒所有阻塞等待的业务服务
		_ = r.rdb.Publish(ctx, r.refreshChannel, "ok").Err()
		log.Println("[token-refresher] Token刷新成功，已广播通知")
		return nil
	}

	return lastErr
}

// loadTokenFromCache 从Redis读取Token并校验有效性
func (r *TokenRefresher) loadTokenFromCache(ctx context.Context) (*Token, bool, error) {
	data, err := r.rdb.Get(ctx, r.cacheKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		return nil, false, err
	}

	var token Token
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, false, err
	}

	// 校验是否已过期
	if time.Now().After(token.ExpireAt) {
		return nil, false, nil
	}
	return &token, true, nil
}
