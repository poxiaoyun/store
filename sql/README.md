# SQL Store

SQL Store 接收调用方管理的数据库连接与 `Dialect`，构造时建立或补充注册资源的表和索引。
连接可以在多个 Store 间共享；连接池配置与关闭由调用方负责，构造失败也不会关闭传入连接。

固定使用 PostgreSQL 时：

```go
import (
    storesql "xiaoshiai.cn/store/sql"
    "xiaoshiai.cn/store/sql/postgres"
)

db, err := postgres.Open(ctx, connectionString)
if err != nil {
    return err
}
defer db.Close()

storage, err := storesql.NewStorage(ctx, schema, db, postgres.Dialect{})
```

MySQL 使用 `mysql.Open(ctx, dsn)` 与 `mysql.Dialect{}`。`dsn` 是原生
`go-sql-driver/mysql` 格式，例如 `user:password@tcp(host:3306)/database`。
MySQL Open 启用时间解码、UTC 和 matched-row 计数，以满足 Store 的时间与条件写入语义。
PostgreSQL Open 接收 pgx 支持的 URL 或 keyword/value 连接字符串。
两个 Open 入口都会创建不存在的目标数据库，因此需要建库权限；PostgreSQL 建库连接使用
`postgres` 维护数据库。已有连接可直接传入 `NewStorage`，无需调用这些入口。

运行时选择内置数据库时，使用可选工厂：

```go
import "xiaoshiai.cn/store/sql/universal"

db, dialect, err := universal.Open(ctx, driver, connectionString)
if err != nil {
    return err
}
defer db.Close()

storage, err := storesql.NewStorage(ctx, schema, db, dialect)
```

`driver` 支持 `mysql` 和 `postgres`，未知值返回 Unsupported。连接字符串遵循选中驱动的
格式。自定义方言直接实现 `sql.Dialect` 并注入核心，无需注册或修改内置工厂。

CRUD、Patch、分页与事务语义见[公共使用指南](../README.md)；SQL 的具体支持范围见
[SQL 设计契约](./DESIGN.md)。
