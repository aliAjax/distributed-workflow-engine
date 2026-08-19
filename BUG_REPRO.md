# Bug 复现说明

## 问题
Scheduler Tick 循环未检查 context 取消，`@every` 非正时长未被拒绝。

## 触发
context 已取消时仍有一批到期 trigger；注册 `@every 0s` 时仍成功。

## 错误信息
取消后仍触发后续任务；非正时长未报错。
