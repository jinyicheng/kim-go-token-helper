package tokenHelper

import "time"

// Config 工具包全量配置项
// 所有参数均有默认值，通过 Option 模式按需覆盖
type Config struct {
	// KeyPrefix Redis Key 前缀，不同第三方Token使用不同前缀隔离
	KeyPrefix string

	// MasterLockExpire 主节点锁过期时间，服务崩溃后自动释放
	MasterLockExpire time.Duration

	// LockRenewInterval 主节点锁续期间隔，必须小于锁过期时间
	LockRenewInterval time.Duration

	// RenewFailThreshold 续期连续失败阈值，达到则判定丢失主节点
	RenewFailThreshold int

	// CheckInterval Token有效期巡检间隔，用于预刷新检测
	CheckInterval time.Duration

	// PreRefreshAhead 预刷新提前时长，Token快过期时提前刷新
	PreRefreshAhead time.Duration

	// MaxRefreshRetry 刷新失败最大重试次数
	MaxRefreshRetry int

	// RetryInterval 刷新失败重试间隔
	RetryInterval time.Duration

	// MaxWaitTime 业务端最大阻塞等待时长
	MaxWaitTime time.Duration

	// MasterElectionInterval 备机抢锁轮询间隔，值越小切换越快
	MasterElectionInterval time.Duration
}

// Option 配置选项函数类型，Go 工具包标准配置模式
type Option func(*Config)

// defaultConfig 返回默认配置
func defaultConfig() *Config {
	return &Config{
		KeyPrefix:              defaultKeyPrefix,
		MasterLockExpire:       defaultMasterLockExpire,
		LockRenewInterval:      defaultLockRenewInterval,
		RenewFailThreshold:     defaultRenewFailThreshold,
		CheckInterval:          defaultCheckInterval,
		PreRefreshAhead:        defaultPreRefreshAhead,
		MaxRefreshRetry:        defaultMaxRefreshRetry,
		RetryInterval:          defaultRetryInterval,
		MaxWaitTime:            defaultMaxWaitTime,
		MasterElectionInterval: defaultElectionInterval,
	}
}

// ==================== 配置选项函数 ====================

// WithKeyPrefix 自定义 Redis Key 前缀
// 不同第三方平台使用不同前缀，实现多套Token隔离
func WithKeyPrefix(prefix string) Option {
	return func(c *Config) {
		c.KeyPrefix = prefix
	}
}

// WithMasterLockExpire 自定义主节点锁过期时间
func WithMasterLockExpire(d time.Duration) Option {
	return func(c *Config) {
		c.MasterLockExpire = d
	}
}

// WithLockRenewInterval 自定义锁续期间隔
func WithLockRenewInterval(d time.Duration) Option {
	return func(c *Config) {
		c.LockRenewInterval = d
	}
}

// WithRenewFailThreshold 自定义续期连续失败阈值
// 值越小切换越快，抗抖动能力越弱
func WithRenewFailThreshold(n int) Option {
	return func(c *Config) {
		c.RenewFailThreshold = n
	}
}

// WithCheckInterval 自定义 Token 有效期巡检间隔
func WithCheckInterval(d time.Duration) Option {
	return func(c *Config) {
		c.CheckInterval = d
	}
}

// WithPreRefreshAhead 自定义预刷新提前时长
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

// WithMasterElectionInterval 自定义备机抢锁轮询间隔
func WithMasterElectionInterval(d time.Duration) Option {
	return func(c *Config) {
		c.MasterElectionInterval = d
	}
}
