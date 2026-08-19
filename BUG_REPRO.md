# Bug 复现说明

## 问题
StartNodes 和 Dependents 复用同一底层切片，历史结果被后续调用污染；发布新版本未推进 current_version。

## 触发
先调用 StartNodes，再调用 Dependents，然后继续使用第一次结果。

## 错误信息
第一次起始节点列表内容变成下游节点 id，版本号不递增。
