# Bug 复现说明

## 问题
JSON 类型扫描 NULL 后未初始化容器，nil 容器序列化不是空 JSON；空迁移目录未回落，DSN 未补 sslmode。

## 触发
扫描数据库 NULL JSON 后写入，或序列化 nil JSONMap/StringSlice。

## 错误信息
`panic: assignment to entry in nil map`。
