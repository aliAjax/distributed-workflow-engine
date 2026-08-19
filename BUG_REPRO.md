# Bug 复现说明

## 问题
执行状态机缺失 waiting 到 running、waiting 节点到 scheduled 的合法转换，取消动作使用错误源状态。

## 触发
创建执行并让节点进入 waiting，收到外部信号恢复时状态无法推进；对 paused 执行取消时按 running 处理。

## 错误信息
定向测试失败，例如 waiting 执行无法转换到 running、取消 paused 执行时实际转换来自 running。
