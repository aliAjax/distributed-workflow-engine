# Bug 复现说明

## 问题
事件发布和持久化错误链被 `%v` 断开，外部信号 resolver 错误未包装，导致上层无法识别底层错误。

## 触发
persister、publisher 或 resolver 返回底层错误时调用事件发布或外部信号处理。

## 错误信息
`errors.Is(err, sentinel)` 为 false，日志中只能看到无营养包装信息。
