# Bug 复现说明

## 问题
策略并发释放闭包使用非同步 bool 防止重复释放，并发调用 release 产生数据竞争。

## 触发
多个 goroutine 同时调用 AcquireTenantSlot 返回的 release。

## 错误信息
竞态检测报告 `WARNING: DATA RACE`，且 release 可能执行多次。
