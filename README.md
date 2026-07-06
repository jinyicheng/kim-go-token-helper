# kim-go-token-helper
token助手，简化token的部分实践

| 参数                  | 默认值  | 调优建议                                        |
|---------------------|------|---------------------------------------------|
| `MasterLockExpire`  | 30s  | 接口耗时越长，该值要越大；建议为接口最大耗时的 2~3 倍               |
| `LockRenewInterval` | 10s  | 必须小于 `MasterLockExpire`；建议为其 1/3，留出重试空间     |
| `CheckInterval`     | 1min | Token 有效期越短，巡检应越频繁；建议为预刷新时长的 1/3            |
| `PreRefreshAhead`   | 5min | Token 有效期 2h 建议 5~10min；有效期 24h 建议 30~60min |