# REST Store

`store/rest` 提供 `store.Store` 的 HTTP adapter。

- `NewServer` 和 `NewRemoteStore` 暴露并访问通用 Store HTTP 协议。
- 对象级 List、Watch、Get、Create、Patch、Update 和 Delete 函数用于已有领域路由，接受调用方创建的具体 `store.Object` 或 `store.ObjectList`。

构造 Client 时显式提供本地 Schema 和远端承诺的 Capabilities：

```go
client := storerest.NewRemoteStore(endpoint, schema, capabilities)
```

Capabilities 只包含 Watch、Finalizers 和 OptimisticLock，由部署远端的组合根提供，
并在派生 Scope 后保留。其他可选行为由具体请求判断。HTTP 能传输某个选项
不代表远端支持它；远端返回的 Unsupported 必须保留。写入请求的 DryRun 会传递给后端，
后端不支持时明确拒绝，不会退化为真实写入。

## List 请求选项

普通 HTTP List 使用 `ListOptionsFromRequest`，参数只传请求默认值：

```go
options, err := storerest.ListOptionsFromRequest(
    r,
    meta.DefaultPage(1, 20),
    meta.DefaultSort("creationTimestamp-"),
)
```

业务过滤、跨 scope 等 Store 条件在转换后追加：

```go
options = append(options,
    store.WithSubScopes(),
    store.WithFieldRequirements(selector.RequirementEqual("published", true)),
)
```

需要检查或归一化公开请求字段时，直接展开解析和转换，不要再次读取 query 后用
Store option 覆盖：

```go
requestOptions, err := api.GetListOptions(r, meta.DefaultPage(1, 20))
if err != nil {
    return err
}
requestOptions.Sort = normalizeSort(requestOptions.Sort)
options, err := store.ListOptionsFromMeta(requestOptions)
```

`meta.ListOption` 只表达请求默认值，`store.ListOption` 表达 Store 查询条件。
selector 转换错误在 HTTP seam 返回 BadRequest。公开 page/size 请求换算为 Offset/Limit；
通用 Store Client 统一传输 limit、continue、offset。续传 token 原样传递，由后端验证
查询上下文。List 不隐式 Count，通用列表响应没有 total；总数由业务显式查询并组装。

Update 和 Patch 成功响应只确认操作结果，Client 不把响应当作完整对象回填。
需要最新对象时显式 Get。Create 仍然返回生成的身份和初始元数据。

Update 不清除 ResourceVersion。请求对象携带非零版本时，底层 Store 按其乐观并发契约处理；只有调用方明确传入零值时才执行无条件更新。

## Requirement 协议

Client 会把 Label 和 Field Requirements 编码成 selector 风格的递归表达式，写入对应的
`labelSelector` 和 `fieldSelector` query。`None`、`All`、`And`、`Or`、`Not` 和
叶子节点都会传给 Server，由底层 Store 在排序、分页和修改前执行。底层 adapter
不能执行的合法操作符由 Server 返回 Unsupported。

原有的平铺 selector 写法保持不变，递归条件在同一参数中增加括号、`&&`、`||` 和
`!(...)`。调用方直接传入
`store.WithLabelRequirements` 或 `store.WithFieldRequirements`，由 Client 调用
`Requirements.String()` 并完成 query 转义。
