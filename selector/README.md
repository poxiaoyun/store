# Selector 使用指南

`xiaoshiai.cn/store/selector` 提供 Store、backend 和协议 adapter 共用的递归布尔选择表达式。
它拥有表达式结构、校验、规范文本和基于键值查找的内存求值，不拥有对象模型、
存储能力或数据库查询编译。

一个 `Requirement` 表示常量、组合或叶子条件；顶层 `Requirements` 隐式使用 AND：

```go
import "xiaoshiai.cn/store/selector"

visible := selector.Requirement{
    Operator: selector.Or,
    Requirements: selector.Requirements{
        selector.RequirementEqual("visibility", "public"),
        selector.RequirementEqual("owner", subjectID),
    },
}
```

零值 `Requirement` 是 `None`，空 `Requirements` 和空 `And` 匹配全部对象，空 `Or`
匹配零个对象。`In`、`NotIn` 和 `Contains` 允许空 `Values`；空集合条件匹配零个对象。

`ParseRequirements` 和 `Requirements.String` 使用同一套 selector 文本。平铺条件兼容
常见的 Kubernetes selector 写法，递归表达式增加 `&&`、`||`、`!(...)`、`all()` 和
`none()`。

```go
requirements, err := selector.ParseRequirements("visibility=public || owner=alice")
expression := requirements.String()
```

用 map 构造等值条件时使用 `RequirementsFromMap`；它按字段名升序生成条件，同一映射总是
得到相同的条件序列，便于稳定序列化和分页绑定。

Store、backend 和协议 adapter 必须保留完整的布尔结构；无法无损执行的合法操作符应由
调用模块按自己的错误契约拒绝，不能忽略或弱化条件。
