# Store 调用指南

`store` 为业务模块提供统一的对象存储接口、资源定义、作用域、查询选项和并发控制语义。业务模块应依赖 `store.Store`，并由组合根选择具体存储实现。

本文档面向 Store 调用方。直接 Store 实现必须遵守的完整行为、并发、删除、查询和 Watch 契约见 [DESIGN.md](./DESIGN.md)。各 backend 与子包的构造方式、支持范围及实现契约见
[SQL](./sql/README.md)、[MongoDB](./mongo/DESIGN.md)、[REST](./rest/README.md)、
[selector](./selector/README.md) 和 [task](./task/README.md)。

## 作用域与调用位置

先定义本次操作使用的 `store.Store`，一次性应用完整 Scope，再通过该变量执行 `Get`、`List`、`Create`、`Update`、`Patch` 或 `Delete`。不要把 `Store.Scope(...)` 内联到具体存储操作中；显式变量使有效作用域可以被直接检查和复用。

对于 `Get` 和 `List`，在调用前紧邻声明接收结果，不要在声明和存储操作之间插入无关语句或空行。

```go
storage := service.Store.Scope(
  appbase.ScopeTenant(tenant),
  appbase.ScopeApplication(application),
)

list := &store.List[ApplicationVersion]{}
if err := storage.List(ctx, list, options...); err != nil {
  return nil, err
}
```

Scope 是有序层级。普通读写只作用于完整的当前 Scope；只有需要读取后代 Scope，并且具体实现声明支持该能力时，才使用 `WithSubScopes()`。

ID 的唯一范围由 backend 声明：可以要求同一资源内跨所有 Scope 唯一，也可以允许不同
Scope 复用 ID；违反唯一范围返回 AlreadyExists。更严格的唯一性不改变 Scope 隔离。

资源名在 `ResourceSchema.ResourceName` 中声明，为空时按对象类型名推导；`Unstructured`
和 `ObjectList` 使用自身的 resource 字段。

## 更新与并发

普通的读取—修改—写入流程必须保留读取到的 `ResourceVersion`。调用 `Get` 后修改对象再执行 `Update`，或者 `CreateOrUpdate` 返回已有对象时，不要把 `ResourceVersion` 重置为零。

只有明确需要绕过正常乐观并发控制的特殊操作才能重置 `ResourceVersion`，并且必须在赋值处用注释说明原因。

读取要求匹配观察到的对象身份或精确版本时，使用 `WithPreconditions`。不存在或 selector 不匹配返回 NotFound，前置条件不匹配返回 Conflict；调用者直接使用成功读取的结果，不再手动比较版本。后续 Update 保留该版本执行 CAS。

删除必须作用于调用方读取的对象时，使用 `WithResourceVersion` 或
`WithUID`；条件不匹配返回 Conflict。

Update、Patch、Status 写入和 Delete 不保证刷新输入对象。需要最新字段或版本时，
显式 Get；这是独立的读取快照。Create 返回生成的身份与初始元数据。
单对象写入目标不存在时返回 NotFound；对象存在但版本、UID 或 selector 条件不匹配时返回 Conflict。

Generation 的递增规则由各 backend 自行定义；对象元数据和 status 的变化不推进 Generation。

只修改少数字段时优先使用原子 `Patch`，精确身份及版本条件通过 `WithPreconditions` 提交。只有操作具有完整替换语义，或者新值依赖刚读取的完整对象版本时才使用 `Update`；后一种情况应保留版本，使并发变化返回 Conflict，而不是覆盖其他写入。

余额、计数等相对更新使用字段 patch，避免先读后写：

```go
err := storage.Patch(ctx, account, store.FieldPatch{Operations: []store.FieldOperation{
  {Op: store.FieldOpInc, Path: "balance", Value: int64(-30)},
}}, store.WithFieldRequirements(
  selector.NewRequirement("balance", selector.GreaterThanOrEqual, int64(30)),
))
```

`FieldPatch` 的操作按数组顺序应用；`Path` 是点分隔的字段路径（如 `balance`、
`nested.count`）。字段补丁是可选能力，是否支持某个路径由具体 backend 决定；不支持时
必须拒绝整个 patch，不能产生部分效果。

新值依赖存储中的其他字段时，用 `FieldOpExpr` 把计算交给 backend，同样不需要先读取：

```go
total := store.Cond(
  store.Gt(store.Field("quantity"), store.Literal(int64(0))),
  store.Add(store.Field("total"), store.Mul(store.Field("price"), store.Field("quantity"))),
  store.Field("total"),
)
err := storage.Patch(ctx, order, store.FieldPatch{Operations: []store.FieldOperation{
  {Op: store.FieldOpExpr, Path: "total", Expr: &total},
}})
```

`Expr` 是 backend 无关的表达式树，只能引用存储中的字段与字面量；字段引用使用与
`FieldPatch` 的 `Path` 相同的点分隔语法。表达式在同一次原子写入中按写前的存储值求值，
支持算术、比较、布尔组合、`cond` 和 `ifNull`，具体 backend 是否支持由实现决定，不支持
时返回 Unsupported。

## 查询与错误

List 使用 `WithLimit`、`WithContinue` 和 `WithOffset`。正数 Limit 限制本批数量，
零值不限制。Continue 是后端生成的不透明字符串；为空开始查询，响应为空表示遍历结束。
Offset 仅指定首批跳过的对象数，不能与非空 Continue 同时使用。

续传须保持资源、Scope、筛选与排序等查询上下文一致；可以调整每批 Limit。
普通分页不保证并发修改期间不重不漏，也不承诺跨请求固定快照。
List 不统计总数，ObjectList 不携带 total；需要总数时显式调用 Count。
Count 支持与 List 相同的筛选、Search 和 SearchFields，独立调用不自动共享快照。

公开接口的 page/size 可通过 `ListOptionsFromMeta` 转换为 Offset/Limit。
请求默认值、页码换算和总数响应组装属于业务或协议层。

跨操作共享的选项使用同一个领域入口，例如 `WithSubScopes`、
`WithResourceVersion`、`WithFieldRequirements` 和 `WithTTL`；具体 option 类型
按需要实现多个 `ApplyToXxx`。

调用方必须把 continuation token 视为 Store 生成的不透明值，并原样传回后续查询。未指定排序时不要依赖结果顺序；需要稳定顺序时显式提供 Store 支持的排序字段。

`Capabilities()` 仅提供 `Watch`、`Finalizers`、`OptimisticLock`，用于选择观察、删除与
并发策略。事务通过 `TransactionStore` 接口使用。其他可选请求直接调用相应操作，
backend 无法执行时返回 Unsupported；调用方不能因此忽略条件或改为无条件写入。
基础 CRUD 与精确 Scope 是基础契约。

保留 Store 返回的领域错误语义，例如 NotFound、AlreadyExists、Conflict、Unsupported 和 ResourceExpired。调用方可以在业务接口处映射这些错误，但不应把并发冲突或不支持的操作降级为成功。

`store/etcdcache` 的 `List` 和 `Count` 会等待对应资源完成首次同步或重建。等待由操作的
Context 控制，cache 未能在 deadline 前恢复时返回 Context 错误；调用方应为请求设置合适的
deadline，不要用固定次数重试代替。

## 删除与 TTL

`Capabilities().Finalizers` 表示后端支持延迟删除。业务需要标记删除时，在删除前添加自己
拥有的保留 finalizer；Delete 设置 DeletionTimestamp 后，对象仍可被 Get、List 和 Count
读取。终止后只能移除 finalizer，不能新增；Update 或 Patch 移除最后一个时原子完成物理
删除。重复 Delete 不改变已有删除时间，也不追加新的传播标记。

DeleteBatch 与 PatchBatch 按 requirements 选择对象，每个对象遵守相同的单对象规则。
批量操作不承诺整体原子性，返回错误时可能已有对象生效。具体 backend 对批量请求的筛选
和 finalizer 组合可能有额外限制，不支持的组合在副作用前返回 Unsupported。

TTL 与 finalizer 不能共存。原生 etcd 支持 Create 和普通 Update 设置 TTL，未指定 TTL 的
后续写入保留已有过期安排；Status 写入不能设置 TTL。etcdcache 只支持 Create 设置 TTL，
对仍受 TTL 控制的对象执行 Update、Patch 或 Status 写入返回 Unsupported；Delete 仍可
物理删除这类对象。为 TTL 对象添加 finalizer（包括 Foreground/Orphan 删除传播）会被拒绝。

## 组合查询条件

查询表达式由本项目的 [`selector`](./selector/README.md) 包定义，Store 通过
`store.Requirements` alias 暴露条件集合。`Requirements` 中的顶层条件按 AND
组合。一个 `Requirement` 也可以用
`And`、`Or` 和 `Not` 递归组合子条件。例如：

```go
visible := selector.Requirement{
    Operator: selector.Or,
    Requirements: store.Requirements{
        selector.RequirementEqual("visibility", "public"),
        selector.RequirementEqual("owner", subjectID),
    },
}

err := storage.List(ctx, list, store.WithFieldRequirements(visible))
```

`None` 匹配零个对象，`All` 匹配全部对象；零值 Requirement 等同于 `None`。空的顶层
`Requirements` 表示调用方没有增加选择条件。组合条件和普通叶子条件一样，必须在排序、
分页和 Count 前执行。

Label 和 Field Requirement 是两个分别求值、最后用 AND 连接的表达式；一个组合节点不能
同时引用 label 和 field。无效节点返回 BadRequest；有效但具体 adapter 无法无损执行的条件
返回 Unsupported。条件的结构、规范文本与求值语义由 selector 拥有，完整说明见
[selector 使用指南](./selector/README.md)。

## 序号分配

`store.Sequence` 按调用方提供的稳定 namespace 独立分配序号。相同 namespace 的序号唯一递增，允许
空洞；不同 namespace 分别计数。namespace 是不透明字符串，`/` 不表示层级，也不隐式
关联 `Store.Scope`。业务编号与对象 ID、ResourceVersion 独立，不通过 CRUD 分配。

内存序列用 `inmemory.NewSequence()` 构造，数据只属于该实例，进程退出后丢失。
MongoDB 序列用 `mongo.NewSequence(database.Collection("sequences"))` 构造；使用同一
collection 的实例共享序列，调用方负责数据库连接生命周期。

```go
var numbers store.Sequence = inmemory.NewSequence()
number, err := numbers.Next(ctx, "issues/repository-uid")
```

## 队列

`store.Queue` 提供独立的 `Enqueue` 与 `Consume` 接口，分别接收 `store.EnqueueOptions`
和 `store.ConsumeOptions`，消费函数类型为 `store.QueueHandleFunc`。

内存实现使用 `inmemory.NewQueue()`；MongoDB 实现使用
`mongo.NewQueue(database.Collection("queue"), &mongo.QueueOptions{})`，调用方管理数据库
连接生命周期。`CheckInterval` 控制 MongoDB 队列的轮询间隔，零值使用一分钟。

相同 ID 的待消费消息会替换内容及延迟，处理中和已完成的 ID 重复入队直接忽略。
`After` 是最早执行延迟；`MaxWorkers` 限制一次 `Consume` 的并发，零值为 1。
消费函数返回错误时，消息保留供下次消费，本次 `Consume` 取消其他 worker、等待退出后返回错误。
消费函数需要响应 Context 取消；返回 nil 的消息仍会完成确认，失败消息保留原始内容。
成功完成的 ID 持续占用去重记录，直到对应内存实例或 MongoDB collection 被销毁。

MongoDB 消费需要 replica set 或 sharded cluster 提供 change stream。通知用于及时唤醒，
轮询负责延迟消息和补漏；积压消息连续领取，不在每条消息间等待。连接的超时配置也适用于
退出时的确认或释放操作。进程崩溃或数据库确认失败可能留下处理中消息，Queue 不自动回收；
需要执行租约和自动重试时使用 task。
