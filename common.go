package tokenHelper

import (
	"context"
	"time"
)

// Token 第三方凭证通用结构体
// 刷新服务与业务服务两端结构必须保持一致
type Token struct {
	AccessToken string    `json:"access_token"` // 第三方访问令牌
	ExpireAt    time.Time `json:"expire_at"`    // 令牌过期时间点
}

// RefreshFunc 第三方Token刷新函数类型
// 业务方实现该函数并注入工具包，无需修改工具内部逻辑
type RefreshFunc func(ctx context.Context) (*Token, error)

// ==================== 默认常量配置 ====================
// 所有默认值均为生产环境经验值，可通过 Option 自定义调整
const (
	// defaultKeyPrefix Redis Key 默认前缀，用于多套第三方Token隔离
	defaultKeyPrefix = "token:third"

	// defaultMasterLockExpire 主节点锁默认过期时间
	// 服务崩溃后锁自动释放，建议为第三方接口最大耗时的2~3倍
	defaultMasterLockExpire = 30 * time.Second

	// defaultLockRenewInterval 主节点锁续期间隔
	// 必须小于锁过期时间，建议为过期时间的1/3，预留网络波动重试空间
	defaultLockRenewInterval = 10 * time.Second

	// defaultRenewFailThreshold 锁续期连续失败阈值
	// 连续失败达到该次数则判定丢失主节点，触发退位重选举
	defaultRenewFailThreshold = 3

	// defaultCheckInterval Token有效期巡检间隔
	// 用于预刷新检测，Token快过期时提前刷新
	defaultCheckInterval = 1 * time.Minute

	// defaultPreRefreshAhead 预刷新提前时长
	// Token剩余有效期小于该值时触发预刷新，旧Token仍有效，业务无感知
	defaultPreRefreshAhead = 5 * time.Minute

	// defaultMaxRefreshRetry 刷新失败最大重试次数
	defaultMaxRefreshRetry = 3

	// defaultRetryInterval 失败重试间隔
	defaultRetryInterval = 2 * time.Second

	// defaultMaxWaitTime 业务端最大阻塞等待时长
	// 超过该时间仍未刷新成功则返回超时错误
	defaultMaxWaitTime = 30 * time.Second

	// defaultElectionInterval 备机抢锁轮询间隔
	// 值越小主备切换越快，Redis压力略高，内网稳定环境建议1~2s
	defaultElectionInterval = 2 * time.Second
)
