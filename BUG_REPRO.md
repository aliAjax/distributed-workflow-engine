# Bug 复现说明

## 问题
Health/Ready 循环未检查 context 取消，checker 超时上下文使用 Background，WithCheck 未合并状态。

## 触发
健康检查期间取消 context，或 checker 返回 down。

## 错误信息
取消后仍执行所有 checker，down check 未反映到整体状态。
