# 测试约定

本模块的测试分两层，两者缺一不可。

## 常规测试

`go test ./...` 运行单元测试，并为 etcd、etcdcache 启动嵌入式 etcd，为 REST
启动本地 HTTP server。SQL 与 MongoDB 的真实数据库测试仍受环境变量控制。
这是每次改动的下限，不是全部真实后端的覆盖证明。

## 真实后端契约测试

`storetest` 是一份共享的 Store 契约套件，各后端在 `*_test.go` 中以
`storetest.Run` 接入。SQL 与 MongoDB 按环境变量门控：未配置时用例 `t.Skip`，而 `go test ./...`
把它们显示为 `ok`。**跳过等同于未测试**：只要 fixture 声明支持某项行为，
就必须用真实后端跑一次套件，不能把 `ok` 当作已覆盖。

| 后端 | 复用已有实例 | 启动临时实例 |
|---|---|---|
| MySQL | `MYSQL_URI` | `MYSQL_INTEGRATION=1` |
| PostgreSQL | `POSTGRESQL_URI` | `POSTGRESQL_INTEGRATION=1` |
| MongoDB | `MONGODB_URI` | `MONGODB_INTEGRATION=1` |
| etcd / etcdcache | — | 套件自动启动嵌入式 etcd |
| REST | — | 本地 HTTP server，底层使用 InMemory |

`*_INTEGRATION` 通过 `testkit/container` 发现容器运行时（podman 或 docker）并启动
临时实例，结束时销毁。运行示例：

```sh
POSTGRESQL_INTEGRATION=1 go test ./sql/ -run TestPostgreSQLStoreConformance -count=1
MYSQL_INTEGRATION=1 go test ./sql/ -run TestMySQLStoreConformance -count=1
MONGODB_INTEGRATION=1 go test ./mongo/ -run TestStoreConformance -count=1
```

Fixture 的 Capabilities 必须是测试维护的独立预期，不能从被测实现的
`Capabilities()` 方法复制。测试同时检查根 Store 与派生 Scope 的三项能力声明。
查询、分页、索引、批量、TTL 和 DryRun 等支持范围由 Fixture 的测试选项独立指定，
不要求在生产接口中增加同名能力。测试选项为 false 时，应验证相应请求返回 Unsupported；
唯一约束无法支持时验证构造失败。
支持能力需要正向用例；不支持的显式请求必须验证 Unsupported，并通过 Get 核对内容、
版本及生命周期没有发生副作用。无变化写入允许 ResourceVersion 保持或推进，但版本条件
不能被无变化优化绕过。

Scope 隔离检查覆盖根作用域下的 Get、List、Count、单对象写入及支持的批量写入，
不能只验证两个同级 Scope。Watch 初始化等待由后端和请求 Context 拥有，套件不自动
重试初始化错误。

Fixture.ResourceWideIDs 声明资源内跨 Scope 的 ID 唯一性。SQL 开启该声明；套件分别验证
根 Scope、相同 Scope 与跨 Scope 的重复创建结果，并检查已有对象不受影响。

生命周期契约覆盖终止对象的可见性、禁止新增 finalizer、重复删除、批量写入与 TTL 互斥。
支持 Watch 的实现还须验证移除最后一个 finalizer 时直接产生删除事件，不能先发布一个
已清空 finalizer 但尚未物理删除的中间对象。

## 已知环境限制

- MongoDB 8.0 在 Linux 内核 6.19 及更新版本上拒绝启动（SERVER-121912）；该环境需
  改用 8.2 或更新镜像，或通过 `MONGODB_URI` 指向外部实例。

写入测试通过显式 Get 验证持久化结果，不依赖 Update/Patch 回填对象。
SQL 的真实数据库测试还约束请求成本：普通 Patch 不预读或回读、List 不隐式统计、
普通批量 Patch 使用一次集合写入。执行次数观察真实数据库调用，不模拟数据库结果。
分页测试验证续传结束与查询上下文绑定；Generation 测试显式声明跟踪字段并验证未声明时不递增。
