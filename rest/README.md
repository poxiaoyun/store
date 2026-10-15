# REST Store

`store/rest` 提供 `store.Store` 的 HTTP adapter。

- `NewServer` 和 `NewRemoteStore` 暴露并访问通用 Store HTTP 协议。
- 对象级 List、Watch、Get、Create、Patch、Update 和 Delete 函数用于已有领域路由，接受调用方创建的具体 `store.Object` 或 `store.ObjectList`。

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
selector 转换错误在 HTTP seam 返回 BadRequest。公开请求按 `Limit>0` 选择游标读、
否则 `Size>0` 选择窗口读（`Page<1` 按 1 处理）的优先级换算为执行意图；Store
HTTP Server 另接受 `offset` 作为 `page/size` 之外更直接的窗口位置，Client 把
窗口的 `Offset`/`Limit` 写成 `offset`/`size`，把游标的 `Continue`/`Limit` 写成
`continue`/`limit`。响应中的 `page/size/limit` 由 Server 按请求回显。没有分页值且
调用方未提供默认策略时，由拥有请求的服务决定不分页行为。

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
