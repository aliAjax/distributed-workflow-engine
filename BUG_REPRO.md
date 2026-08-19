# Bug 复现说明

## 问题
租户认证错误链断裂，过期 key 未返回 sentinel，导致上层无法判断错误类型。

## 触发
使用过期 key 认证，或仓库返回 sentinel 错误时调用 Authenticate。

## 错误信息
`errors.Is(err, domain.ErrAPIKeyExpired)` 为 false，仓库错误无法被命中。
