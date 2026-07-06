package tokenHelper

import "time"

// Config 工具包全量配置项
type Config struct {
	KeyPrefix         string        // Redis Key 前缀，支持多套第三方 Token 隔离
	MasterLockExpire  time.Duration // 主节点锁过期时间，服务崩溃后自动释放
	LockRenewInterval time.Duration // 锁续期间隔，必须小于锁过期时间
	CheckInterval     time.Duration // Token 有效期巡检间隔
	PreRefreshAhead   time.Duration // 提前多久触发预刷新
	MaxRefreshRetry   int           // 刷新失败最大重试次数
	RetryInterval     time.Duration // 刷新失败重试间隔
	MaxWaitTime       time.Duration // 业务端最大阻塞等待时长
}

// Option 配置选项函数类型
type Option func(*Config)

// 默认配置
func defaultConfig() *Config {
	return &Config{
		KeyPrefix:         defaultKeyPrefix,
		MasterLockExpire:  defaultMasterLockExpire,
		LockRenewInterval: defaultLockRenewInterval,
		CheckInterval:     defaultCheckInterval,
		PreRefreshAhead:   defaultPreRefreshAhead,
		MaxRefreshRetry:   defaultMaxRefreshRetry,
		RetryInterval:     defaultRetryInterval,
		MaxWaitTime:       defaultMaxWaitTime,
	}
}

// WithKeyPrefix 自定义 Redis Key 前缀（用于多套不同第三方 Token 隔离）
func WithKeyPrefix(prefix string) Option {
	return func(c *Config) {
		c.KeyPrefix = prefix
	}
}

// WithMasterLockExpire 自定义主节点锁过期时间
// 建议值：必须大于第三方接口最大耗时，一般 15~60 秒
func WithMasterLockExpire(d time.Duration) Option {
	return func(c *Config) {
		c.MasterLockExpire = d
	}
}

// WithLockRenewInterval 自定义锁续期间隔
// 建议值：为锁过期时间的 1/3 ~ 1/2，保证网络波动下仍能成功续期
func WithLockRenewInterval(d time.Duration) Option {
	return func(c *Config) {
		c.LockRenewInterval = d
	}
}

// WithCheckInterval 自定义 Token 有效期巡检间隔
// 建议值：为预刷新提前时长的 1/5 ~ 1/3，巡检越频繁，过期遗漏风险越低
func WithCheckInterval(d time.Duration) Option {
	return func(c *Config) {
		c.CheckInterval = d
	}
}

// WithPreRefreshAhead 自定义预刷新提前时长
// 建议值：为 Token 总有效期的 1/10 ~ 1/6，预留充足重试时间
func WithPreRefreshAhead(d time.Duration) Option {
	return func(c *Config) {
		c.PreRefreshAhead = d
	}
}

// WithRefreshRetry 自定义刷新重试参数
func WithRefreshRetry(maxTimes int, interval time.Duration) Option {
	return func(c *Config) {
		c.MaxRefreshRetry = maxTimes
		c.RetryInterval = interval
	}
}

// WithMaxWaitTime 自定义业务端最大阻塞等待时长
func WithMaxWaitTime(d time.Duration) Option {
	return func(c *Config) {
		c.MaxWaitTime = d
	}
}
