package tokenHelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

// TokenReader 业务端Token只读客户端
// 无权限刷新Token，仅能读取缓存，无效时阻塞等待刷新完成
type TokenReader struct {
	rdb    *redis.Client
	config *Config

	cacheKey       string
	refreshChannel string
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
	token, valid, err := tr.loadToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取Token缓存失败: %w", err)
	}
	if valid {
		return token, nil
	}

	return tr.waitForNewToken(ctx)
}

// 阻塞等待刷新完成
func (tr *TokenReader) waitForNewToken(ctx context.Context) (*Token, error) {
	waitCtx, cancel := context.WithTimeout(ctx, tr.config.MaxWaitTime)
	defer cancel()

	// 1. 先订阅广播频道
	pubsub := tr.rdb.Subscribe(waitCtx, tr.refreshChannel)
	defer pubsub.Close()

	// 2. 订阅成功后二次校验缓存，避免订阅前已刷新导致白等
	token, valid, err := tr.loadToken(waitCtx)
	if err != nil {
		return nil, err
	}
	if valid {
		return token, nil
	}

	// 3. 阻塞等待刷新广播
	_, err = pubsub.ReceiveMessage(waitCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, errors.New("等待Token刷新超时，请检查刷新服务状态")
		}
		return nil, fmt.Errorf("等待刷新异常: %w", err)
	}

	// 4. 收到通知后重新读取最新Token
	token, valid, err = tr.loadToken(waitCtx)
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, errors.New("收到刷新通知但Token仍无效")
	}

	return token, nil
}

// 从缓存读取并校验Token
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

	if time.Now().After(token.ExpireAt) {
		return nil, false, nil
	}
	return &token, true, nil
}
