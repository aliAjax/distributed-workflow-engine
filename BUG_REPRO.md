# Bug 复现说明

## 问题
Runner 在 worker 完成前关闭错误通道，导致取消时向已关闭 channel 发送错误并 panic。

## 触发
启动多个 worker 后取消 context，worker 返回错误时触发。

## 错误信息
panic: send on closed channel
