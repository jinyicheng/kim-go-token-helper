package tokenHelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TokenReader 业务端Token只读客户端
// 特点：
//   - 仅拥有读权限，不接触第三方密钥
//   - Token有效时直接返回，无效时阻塞等待刷新完成
//   - 先订阅后双检，彻底避免消息漏收竞态
type TokenReader struct {
	rdb    *redis.Client
	config *Config

	cacheKey       string // Token 缓存键
	refreshChannel string // 刷新完成广播频道
}

// NewTokenReader 创建业务端读取实例
func NewTokenReader(rdb *redis.Client, opts ...Option) *TokenReader {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return &TokenReader{
		rdb:            rdb,
		config:         cfg,
		cacheKey:       cfg.KeyPrefix + ":cache",
		refreshChannel: cfg.KeyPrefix + ":refresh:channel",
	}
}

// GetToken 获取有效Token
// 缓存有效直接返回；无效则阻塞等待刷新完成，超时返回错误
func (tr *TokenReader) GetToken(ctx context.Context) (*Token, error) {
	// 第一步：优先读取缓存
	token, valid, err := tr.loadToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取Token缓存失败: %w", err)
	}
	if valid {
		return token, nil
	}

	// 第二步：缓存无效，进入阻塞等待流程
	return tr.waitForNewToken(ctx)
}

// waitForNewToken 阻塞等待刷新完成
// 采用「先订阅 → 二次校验 → 再阻塞」标准顺序
// 彻底解决经典竞态：刷新消息早于订阅导致的白等问题
func (tr *TokenReader) waitForNewToken(ctx context.Context) (*Token, error) {
	// 创建带超时的上下文，控制最大阻塞时长
	waitCtx, cancel := context.WithTimeout(ctx, tr.config.MaxWaitTime)
	defer cancel()

	// 1. 先订阅广播频道
	pubsub := tr.rdb.Subscribe(waitCtx, tr.refreshChannel)
	defer pubsub.Close()

	// 2. 订阅成功后二次校验缓存
	// 关键作用：如果在订阅前刷新服务已经完成刷新，直接返回，无需空等
	token, valid, err := tr.loadToken(waitCtx)
	if err != nil {
		return nil, err
	}
	if valid {
		return token, nil
	}

	// 3. 缓存仍无效，阻塞等待刷新完成广播
	_, err = pubsub.ReceiveMessage(waitCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, errors.New("等待Token刷新超时，请检查刷新服务状态")
		}
		return nil, fmt.Errorf("等待刷新异常: %w", err)
	}

	// 4. 收到刷新通知后，重新读取最新Token
	token, valid, err = tr.loadToken(waitCtx)
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, errors.New("收到刷新通知但Token仍无效")
	}

	return token, nil
}

// loadToken 从Redis读取Token并校验是否过期
func (tr *TokenReader) loadToken(ctx context.Context) (*Token, bool, error) {
	data, err := tr.rdb.Get(ctx, tr.cacheKey).Bytes()
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

	// 已过期则返回无效
	if time.Now().After(token.ExpireAt) {
		return nil, false, nil
	}
	return &token, true, nil
}
