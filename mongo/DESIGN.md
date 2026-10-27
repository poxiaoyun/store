# MongoDB Store 设计

`store/mongo` 使用 MongoDB Go Driver 实现 [Store 公共契约](../DESIGN.md)。本模块拥有
BSON 编码、change stream Watch、事务与驱动错误映射；序列和队列是独立构造的接口，
使用调用方传入的 collection。

## 构造与编码

`NewMongoStorage` 复制传入的 Schema，并为每个资源准备 collection 与索引。实例默认使用
`GlobalBsonRegistry`；`WithBSONRegistry` 可以为单个实例提供自己的 registry。同一个
registry 同时编码持久化值与查询操作数，避免写入和过滤使用不同表示。默认 BSON 选项
使用 JSON struct tag 并省略零值结构体；`GlobalBsonRegistry` 内置 `meta.Time` 与
`resource.Quantity` 的专用 codec。

构造期间打开客户端后，collection 或索引初始化失败会先断开该客户端。构造成功的实例
由调用方通过 `Close(ctx)` 释放。Watch 依赖 replica set 或 sharded cluster 提供的
change stream。

## 补丁

支持 Merge Patch、JSON Patch 的字段子集和 Field Patch。JSON Patch 只支持对象字段的
add、replace 与 remove；test、copy 和 move 返回 Unsupported，路径按对象字段解释，
不表示数组下标。replace 会创建缺失字段，remove 对缺失字段是 no-op。

Field Patch 的 operations 按数组顺序生效。路径互不重叠且不涉及业务字段时使用单个
update document（`$set`、`$unset`、`$inc` 和 `resourceVersion`）；否则每个操作对应
一个 pipeline stage，以保留声明顺序。服务端拥有的字段、generation 和 finalizers 不能
通过字段补丁修改，status 只能通过 `Status()` 修改。

补丁的 filter 同时携带 selector 与前置条件，条件判断和写入是单文档原子操作。写入未
命中时，目标不存在返回 NotFound，存在但不满足条件返回 Conflict。

## 事务

`Transaction` 使用 snapshot read concern 与 majority write concern。默认、
ReadCommitted 和 RepeatableRead 请求都由 snapshot 语义执行，其他隔离级别返回
Unsupported；超时通过 Context 生效。驱动的 `WithTransaction` 负责重试瞬态冲突并处理
未知提交结果；错误转换保留 Context 错误与 `TransientTransactionError` 标签，避免中断
驱动的重试或回滚。

## 错误映射

`mongo.ErrNoDocuments` 映射为 NotFound，重复键映射为 AlreadyExists，无效搜索表达式映射
为 BadRequest，其他驱动错误保留错误链并映射为 Internal。Context 取消与瞬态事务错误
原样透传。
