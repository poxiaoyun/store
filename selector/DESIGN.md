# Selector 设计契约

## 所有权

`xiaoshiai.cn/store/selector` 是本项目递归布尔选择表达式的唯一 owner。它拥有 `Operator`、`Requirement`、
`Requirements`、结构校验、规范文本解析与格式化，以及共享的键值比较语义。

对象和字段映射、Store options、数据库编译、能力声明、协议参数及错误映射
属于各自调用模块。依赖方向统一指向 selector：

```text
store ───┐
backend ─┼──> selector
rest ────┘
```

## 表达式

顶层 `Requirements` 是隐式 AND。零值 `Requirement` 是 `None`；空 `Requirements`
和空 `And` 为真，空 `Or` 为假，`Not` 只接受一个子条件。常量和组合节点不能携带
Key 或 Values，叶子节点不能携带子 Requirements。

单值比较要求一个 operand。`In`、`NotIn` 和 `Contains` 不对 Values 数量施加下限，
空 Values 是合法结构并匹配零个对象。调用方需要显式恒真或恒假条件时使用 `All` 或
`None`，不从 operand 数量推断另一种操作符。

值支持 nil、字符串、布尔值、整数、有限浮点数、`time.Time` 和共享的
`meta.Time`。文本协议中的非 nil operand 保持字符串；求值时数字、布尔值和时间
可以与对应的文本表示比较。缺失键只满足 `DoesNotExist`、非空 `NotEquals` 和非空
`NotIn`。

`RequirementsFromMap` 按字段名升序生成等值条件。同一映射产生相同的条件序列，供调用方稳定序列化及绑定分页查询。

## 文本与 adapter

`Requirements.String` 生成 `ParseRequirements` 可解析的规范文本。顶层逗号表示
AND；组合节点使用 `&&`、`||`、`!(...)` 和括号；常量使用 `all()` 与 `none()`。
包含分隔符或空白的字符串使用双引号及 Go 字符串转义，未加引号的 `null` 表示 nil。

外部 selector、对象模型和数据库都是 adapter seam。adapter 可以把表达式编译到
自己的查询表示，但必须完整保留结构和语义；无法表达时返回其所属模块定义的错误。
