# Store 调用指南

`store` 为业务模块提供统一的对象存储接口、资源定义、作用域、查询选项和并发控制语义。业务模块应依赖 `store.Store`，并由组合根选择具体存储实现。

本文档面向 Store 调用方。直接 Store 实现必须遵守的完整行为、并发、删除、查询和 Watch 契约见 [DESIGN.md](./DESIGN.md)。

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

Scope 是有序层级。普通读写只作用于完整的当前 Scope；只有需要读取后代 Scope，并且具体实现声明支持该能力时，才使用 `IncludeSubScopes`。

## 更新与并发

普通的读取—修改—写入流程必须保留读取到的 `ResourceVersion`。调用 `Get` 后修改对象再执行 `Update`，或者 `CreateOrUpdate` 返回已有对象时，不要把 `ResourceVersion` 重置为零。

只有明确需要绕过正常乐观并发控制的特殊操作才能重置 `ResourceVersion`，并且必须在赋值处用注释说明原因。

读取要求匹配观察到的对象身份或精确版本时，使用 `WithPreconditions`。不存在或 selector 不匹配返回 NotFound，前置条件不匹配返回 Conflict；调用者直接使用成功读取的结果，不再手动比较版本。后续 Update 保留该版本执行 CAS。

删除必须作用于调用方读取的对象时，使用 `WithResourceVersion` 或
`WithUID`；条件不匹配返回 Conflict。

只修改少数字段时优先使用原子 `Patch`，精确身份及版本条件通过 `WithPreconditions` 提交。只有操作具有完整替换语义，或者新值依赖刚读取的完整对象版本时才使用 `Update`；后一种情况应保留版本，使并发变化返回 Conflict，而不是覆盖其他写入。

## 查询与错误

使用 `ListOptionsFromMeta` 将公开的 `meta.ListOptions` 转成
`[]store.ListOption`，并直接追加服务端约束后传给 `Store.List`。标准
Store option 是实现 `ApplyToXxx` 的具体值；不得传入捕获闭包，也不得依赖
完整 Options 替换的特殊顺序。公开请求保持页码形态（`page/size` 或
`continue/limit`），换算出的执行意图是窗口或游标，见
[meta 契约](../meta/README.md#列表与分页)。HTTP 列表 seam 可在转换前使用
`meta.DefaultPage`、
`meta.DefaultContinuation` 和 `meta.DefaultSort` 补充请求默认值。这些 option 只在
各自字段为零时写入自己的字段；`DefaultPage` 和 `DefaultContinuation` 遇到另一组
非空意图字段时都不应用，避免默认行为覆盖显式请求。query 先解析，option 随后按
声明顺序应用。未配置默认值且 `Limit`、`Size` 均不为正数时，由拥有请求的服务
决定不分页行为。

`ApplyListOptions` 只按声明顺序展开 option，不返回错误。Store 实现按执行意图
选择行为：`Continue` 非 nil 时执行游标读（空 token 表示首批）；否则执行窗口读，
跳过 `Offset` 并最多返回 `Limit` 个，`Limit` 为零表示返回剩余全部匹配对象。
所有实现都支持窗口读；不支持游标读的实现返回 Unsupported。

结果只携带 Store 事实：窗口读返回 `total` 与空 `continue`，游标读返回下一批
`continue`（空表示遍历结束）且不返回 `total`。`page/size/limit` 属于请求契约，
由协议层回显，不由 Store 序列化。

跨操作共享的选项使用同一个领域入口，例如 `WithSubScopes`、
`WithResourceVersion`、`WithFieldRequirements` 和 `WithTTL`；具体 option 类型
按需要实现多个 `ApplyToXxx`。

调用方必须把 continuation token 视为 Store 生成的不透明值，并原样传回后续查询。未指定排序时不要依赖结果顺序；需要稳定顺序时显式提供 Store 支持的排序字段。

调用可选能力前检查 `Capabilities()`。基础 CRUD、精确 Scope 和正常的乐观并发语义不是可关闭的可选能力。

保留 Store 返回的领域错误语义，例如 NotFound、AlreadyExists、Conflict、Unsupported 和 ResourceExpired。调用方可以在业务接口处映射这些错误，但不应把并发冲突或不支持的操作降级为成功。

`store/etcdcache` 的 `List` 和 `Count` 会等待对应资源的 watch cache 完成首次同步或重建。等待由调用操作的 Context 控制；cache 未能在 deadline 前恢复时返回 Context 错误。调用方应为请求设置合适的 deadline，不需要针对 `storage is (re)initializing` 设置固定次数重试。

## 组合查询条件

查询表达式由共享的 [`selector`](../selector) 包定义，Store 通过
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

`None` 匹配零个对象，`All` 匹配全部对象；零值 Requirement 等同于
`None`。空的顶层 `Requirements` 表示调用方没有增加选择条件，因此匹配全部对象。
组合条件和普通叶子条件一样，必须在排序、分页和 Count 前执行。

Label 和 Field Requirement 是两个分别求值、最后用 AND 连接的表达式；一个
组合节点不能同时引用 label 和 field。调用前检查相应的 `LabelSelector` 或
`FieldSelector` capability。有效但具体 adapter 无法无损执行的条件返回
Unsupported；无效节点返回 BadRequest。

`Requirements.String()` 和 `ParseRequirements` 使用同一套 selector 风格文本：

```go
expression := requirements.String()
requirements, err = selector.ParseRequirements(expression)
```

平铺表达式沿用 Kubernetes label selector 的写法；递归表达式增加 `&&`、`||`、
`!(...)`、`all()` 和 `none()`。包含分隔符的字符串自动加引号。协议 operand 按
selector 语义解析成字符串，由 Requirement 求值层完成布尔值、数值和时间比较；
`null` 单独表示 nil。Requirement 值支持 nil、字符串、布尔值、整数、有限浮点数和时间。
