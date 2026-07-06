# kim-go-token-helper
token助手，简化token的部分实践

| 参数                  | 默认值  | 调优建议                                        |
|---------------------|------|---------------------------------------------|
| `MasterLockExpire`  | 30s  | 接口耗时越长，该值要越大；建议为接口最大耗时的 2~3 倍               |
| `LockRenewInterval` | 10s  | 必须小于 `MasterLockExpire`；建议为其 1/3，留出重试空间     |
| `CheckInterval`     | 1min | Token 有效期越短，巡检应越频繁；建议为预刷新时长的 1/3            |
| `PreRefreshAhead`   | 5min | Token 有效期 2h 建议 5~10min；有效期 24h 建议 30~60min |

[//]: # ()
[//]: # (```go)

[//]: # (package main)

[//]: # ()
[//]: # (import &#40;)

[//]: # (	"context")

[//]: # (	"fmt")

[//]: # (	"time")

[//]: # (	"github.com/redis/go-redis/v9")

[//]: # (	tokenHelper "github.com/jinyicheng/kim-go-token-helper")

[//]: # (&#41;)

[//]: # ()
[//]: # (func main&#40;&#41; {)

[//]: # (	// 初始化Redis客户端)

[//]: # (	rdb := redis.NewClient&#40;&redis.Options{)

[//]: # (		Addr: "127.0.0.1:6379",)

[//]: # (		DB:   0,)

[//]: # (	}&#41;)

[//]: # (	defer rdb.Close&#40;&#41;)

[//]: # ()
[//]: # (	// 创建刷新器，注入你自己的刷新函数)

[//]: # (	refresher := tokenHelper.NewTokenRefresher&#40;rdb, myRefreshFunc,)

[//]: # (		tokenHelper.WithKeyPrefix&#40;"token:wechat"&#41;, // 不同第三方用不同前缀隔离)

[//]: # (		tokenHelper.WithPreRefreshAhead&#40;10*time.Minute&#41;,)

[//]: # (	&#41;)

[//]: # ()
[//]: # (	// 启动服务)

[//]: # (	ctx := context.Background&#40;&#41;)

[//]: # (	if err := refresher.Start&#40;ctx&#41;; err != nil {)

[//]: # (		panic&#40;err&#41;)

[//]: # (	})

[//]: # ()
[//]: # (	// 常驻运行)

[//]: # (	select {})

[//]: # (})

[//]: # ()
[//]: # (// myRefreshFunc 【你自己的业务逻辑】调用第三方接口获取Token)

[//]: # (func myRefreshFunc&#40;ctx context.Context&#41; &#40;*thirdtoken.ThirdToken, error&#41; {)

[//]: # (	// ===== 在这里写你真实的HTTP调用逻辑 =====)

[//]: # (	// 例如调用微信获取access_token、钉钉获取token等)

[//]: # (	log.Println&#40;"正在调用第三方接口获取Token..."&#41;)

[//]: # (	time.Sleep&#40;500 * time.Millisecond&#41; // 模拟网络耗时)

[//]: # ()
[//]: # (	now := time.Now&#40;&#41;)

[//]: # (	return &tokenHelper.Token{)

[//]: # (		AccessToken: "real_access_token_" + fmt.Sprint&#40;now.Unix&#40;&#41;&#41;,)

[//]: # (		ExpireAt:    now.Add&#40;2 * time.Hour&#41;,)

[//]: # (	}, nil)

[//]: # (})

[//]: # (```)