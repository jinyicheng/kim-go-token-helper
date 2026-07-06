package tokenHelper

import (
	"context"
	"time"
)

// Token 第三方凭证通用结构体
type Token struct {
	AccessToken string    `json:"access_token"`
	ExpireAt    time.Time `json:"expire_at"`
}

// RefreshFunc 第三方Token刷新函数类型
// 业务方实现该函数，传入工具包即可，无需修改工具内部逻辑
type RefreshFunc func(ctx context.Context) (*Token, error)

// 默认常量配置
const (
	defaultMasterLockExpire  = 30 * time.Second
	defaultLockRenewInterval = 10 * time.Second
	defaultCheckInterval     = 1 * time.Minute
	defaultPreRefreshAhead   = 5 * time.Minute
	defaultMaxRefreshRetry   = 3
	defaultRetryInterval     = 2 * time.Second
	defaultMaxWaitTime       = 30 * time.Second
	defaultKeyPrefix         = "token:third"
)
