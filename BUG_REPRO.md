# Bug 复现说明

## 问题
HTTP 超时中间件使用 Background 替代父 context，writeJSON 未写状态码，readyz 状态判断反了，分页和 JSON 解码边界错误。

## 触发
请求超时、写入 JSON、服务 down 时请求 readyz、负数 offset/零 limit、未知 JSON 字段。

## 错误信息
父取消未传播，状态码错误，down 时返回 200。
