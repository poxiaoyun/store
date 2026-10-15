# 测试约定

本模块的测试分两层，两者缺一不可。

## 常规测试

`go test ./...` 运行无外部依赖的单元测试（含 SQL 语句生成、schema 规划、条件渲染、
方言差异等纯逻辑）。这是每次改动的下限，不是覆盖证明。

## 真实后端契约测试

`storetest` 是一份共享的 Store 契约套件，各后端在 `*_test.go` 中以
`storetest.Run` 接入。它按环境变量门控：未配置时用例 `t.Skip`，而 `go test ./...`
把它们显示为 `ok`。**跳过等同于未测试**：只要声明了某项能力（`Capabilities`），
就必须用真实后端跑一次套件，不能把 `ok` 当作已覆盖。

| 后端 | 复用已有实例 | 启动临时实例 |
|---|---|---|
| MySQL | `MYSQL_URI` | `MYSQL_INTEGRATION=1` |
| PostgreSQL | `POSTGRESQL_URI` | `POSTGRESQL_INTEGRATION=1` |
| MongoDB | `MONGODB_URI` | `MONGODB_INTEGRATION=1` |
| etcd | `ETCD_ENDPOINTS` | `ETCD_INTEGRATION=1` |

`*_INTEGRATION` 通过 `testkit/container` 发现容器运行时（podman 或 docker）并启动
临时实例，结束时销毁。运行示例：

```sh
POSTGRESQL_INTEGRATION=1 go test ./sql/ -run TestPostgreSQLStoreConformance -count=1
MYSQL_INTEGRATION=1 go test ./sql/ -run TestMySQLStoreConformance -count=1
MONGODB_INTEGRATION=1 go test ./mongo/ -run TestStoreConformance -count=1
```

## 已知环境限制

- MongoDB 8.0 在 Linux 内核 6.19 及更新版本上拒绝启动（SERVER-121912）；该环境需
  改用 8.2 或更新镜像，或通过 `MONGODB_URI` 指向外部实例。
