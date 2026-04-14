# 数据同步工具CLi

- 连接 MySQL / Postgres
- 选择目标表（通过 `list-tables` 查看）
- 通过配置的接口获取 JSON 数据
- 将字段按配置映射并同步到目标表，支持 upsert
- 支持测试接口数据获取

## 构建

```bash
cargo build
```
