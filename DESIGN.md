# Store 实现契约

本文定义 Store 的职责、公共行为与失败语义。直接存储、远程 adapter 和缓存 adapter
遵守同一契约；具体实现的内部布局与协议映射由各自的 DESIGN 定义。

## 职责与能力

Store 拥有对象持久化、资源身份、服务端元数据、查询和资源生命周期。业务校验、默认值、
业务状态转换与授权由业务模块拥有，不能依赖通用 Store hook 覆盖所有业务入口。
OwnerReferences 由 Store 保存，依赖遍历与级联删除由外部 garbage collector 拥有。
业务编号及其分配也由业务模块或独立分配器拥有，不改变 Store ID 的含义。
`Sequence` 是根包中的独立分配接口，不增加 Store CRUD 的职责或能力声明。调用方通过
稳定 namespace 划分序列；同一序列的分配必须原子完成，返回唯一递增的正整数，允许空洞。
namespace 作为不透明字符串标识分配空间，不解析层级，也不从 Store Scope 自动派生。
内存实现的序列属于实例生命周期；持久化实现的序列属于调用方指定的存储空间。
序列实现与对象存储实现位于对应 backend 包中，可以独立构造和选择。

`Queue` 是根包中独立的入队与消费接口，其操作选项与处理函数由根包拥有，实现放在对应
backend 包中并独立构造。Queue 不属于对象 CRUD 的必选能力，也不复用 controller 的
对象协调工作队列；两者分别拥有任务消费与对象重新协调的语义。
Queue 按消息 ID 去重：待消费消息允许替换内容与执行时间，处理中及已完成的 ID 忽略
重复入队。延迟表示最早可执行时间，不保证精确唤醒或 FIFO；并发限制属于单次 Consume。
处理失败必须保留消息、停止本次消费并返回错误；重启消费由调用方决定，不内置重试策略。
正常退出须等待本次处理结束并完成确认或释放；成功完成的 ID 保留至队列存储生命周期结束。
Queue 不承诺进程崩溃后的领取恢复或 exactly-once；需要租约与重试策略的执行由 task 拥有。

核心可以依赖 `common/errors` 复用领域错误；对象元数据 `meta` 与查询表达式 `selector`
由本项目独立维护。HTTP 参数、状态码和业务分页响应由协议 adapter 拥有，不进入核心契约。

CRUD、Count、精确 Scope、服务端元数据、普通写入与 status 的隔离，以及 id 升降序排序
属于基础契约。finalizer、版本条件写入、分页、Offset、分页快照、Watch、TTL、DryRun、
批量操作等行为可以具有不同的后端支持程度；支持的行为必须满足这里规定的语义。

`Capabilities()` 只描述影响调用方工作方式的三个方向：Watch、Finalizers 与
OptimisticLock。声明分别承诺快照到实时事件的连续观察、延迟删除生命周期与原子版本
条件写入。事务通过可选的 `TransactionStore` 接口表达。

查询、分页、投影、TTL、DryRun 和批量请求的支持范围由 backend 定义，不逐项暴露发现
开关。操作入口按自身约束检查，不支持的功能或组合必须在产生副作用前返回 Unsupported，
不能忽略选项、弱化条件或返回看似成功的部分查询结果。调用方可以直接请求所需行为，
无需先查询能力表；能力概览也不替代具体请求校验。

所有资源必须先在 Schema 中注册。`Schema()` 返回独立快照，调用方修改该快照不能影响
Store；`Scope()` 派生对象提供相同定义的独立快照。Schema 是本地契约，REST adapter
的构造方也须显式提供，不能通过远程响应猜测资源定义。

Schema 注册只验证资源定义，不因某个 backend 缺少可选操作能力而失败。普通非唯一索引
是可选优化声明，backend 可以不建立，查询成本不由 Store 统一保证。唯一约束是资源
必须满足的不变量：backend 绑定 Schema、构造 Store 时必须确认能够执行声明的唯一约束，
无法保证时构造失败，不能等到写入时忽略约束。

## 对象、身份与 Scope

对象采用扁平结构：公共元数据、顶层业务字段以及资源可选的顶层 status。
Name 和 Description 是可修改的展示元数据，不承担唯一身份；APIVersion 仅透传，
Store 不据此转换对象或选择路由。

对象通过 Resource、完整有序 Scope 与 ID 定位。ID 的唯一范围由 backend 明确声明：
可以允许不同 Scope 复用 ID，也可以要求同一资源内跨所有 Scope 唯一；违反唯一范围返回
AlreadyExists。更严格的 ID 唯一性不改变 Scope 隔离，所有读写仍须匹配请求的完整 Scope。UID 区分
同一身份位置上的不同对象生命周期。Create 的空 ID 生成 UUID，显式非空 ID 原样保留，
不增加额外格式要求。ID、UID、Resource、Scopes 与 CreationTimestamp 在创建后不可改写。
创建时 UID、ResourceVersion、Generation、CreationTimestamp、Resource、Scopes 和
DeletionTimestamp 由 Store 决定，调用方预填值不能覆盖服务端值。身份或唯一约束冲突
返回 AlreadyExists。

资源名默认由对象类型名按统一复数规则推导。只有既有持久化或外部协议名称构成明确约束时，
对象才通过 `ResourceName() string` 指定不同名称。

Scope 是可变深度的有序路径，资源类型是字面值，可以含点号，也可以在不同位置重复；
顺序和每一级名称均参与身份判定。普通 Get、List、Count 和写操作精确匹配完整当前
Scope，根 Scope 不代表所有后代。`IncludeSubScopes` 显式包含当前 Scope 及其后代，
只有支持该能力的 backend 才能执行。Scope 派生不能修改原 Store 的路径。

Backend 可以只支持可无损表示的 Scope 子集，超出范围的请求返回 Unsupported。
列投影或其他派生字段不能抹去路径顺序、重复层级或深度。返回对象必须保留完整持久化
Scope，子 Scope 查询和字段投影也不例外。根 Scope 的空数组、null 与省略形式等价。

## ResourceVersion、Generation 与 Status

ResourceVersion 是并发控制版本，成功创建后为正数。持久化内容实际改变时必须推进版本；
能够准确判断未发生变化时应保持版本，但允许 backend 在无变化的成功写入中推进版本。
调用方不能把版本变化当作业务内容变化的证明，也不能假设每次成功调用都会改变版本。
无变化优化必须在前置条件验证之后，不能把过期条件请求变成成功。

Generation 从 1 开始，只跟踪 ResourceSchema 明确声明的业务字段；没有声明时不跟踪变化。
一次写入中任一被跟踪字段实际改变，Generation 加一。公共元数据与顶层 status 不得列为
跟踪字段，未被声明的业务字段变化也不推进 Generation。生命周期合并与字段比较必须保留
JSON 数值完整精度；等价数值表示不构成变化，字符串与数值不相互转换。backend 无法准确执行
声明的跟踪字段时，在绑定 Schema 时拒绝，不能静默忽略或退化为每次写入递增。

Status 是否存在由资源类型决定，对没有 status 的资源执行 Status 写入返回 Unsupported。
Create 可以初始化 status；普通 Update、Patch 和 PatchBatch 保留存储中的 status。
`Status().Update` 与 `Status().Patch` 只能改变顶层 status 和 ResourceVersion，不能修改
其他元数据、业务字段或 Generation，并遵守同样的版本与前置条件规则。

## 前置条件与原子写入

`Preconditions` 的 UID 和 ResourceVersion 字段均可选：nil 表示未指定，非 nil 的空 UID
或零版本仍要求精确匹配。UID 防止同 ID 重建后的新对象被旧请求误操作；ResourceVersion
限定调用方接受的对象版本。`WithPreconditions` 只覆盖自身非空字段，不清除先前条件。

Update 对象的 ResourceVersion 为零表示无版本条件，非零表示条件写入。Patch 不把传入对象
上的版本隐式视为前置条件；Merge Patch 的顶层 resourceVersion 非零时构成版本条件，
省略或零值表示无该条件。JSON Patch 可以用 `test /resourceVersion` 表达条件，test 失败
是补丁校验错误，后续操作不得执行。单对象 Patch、Status Patch、Get 和 Delete 均可显式
提供 `WithPreconditions`。Delete 对象上的 UID、ResourceVersion 不产生隐式条件，
`WithUID` 和 `WithResourceVersion` 设置 Delete 的相应前置条件。

版本条件写入是可选能力；不支持时，Update、Patch、Status 或 Delete 携带的版本条件都必须
在副作用前返回 Unsupported，包括补丁内的版本条件。不能把读取后无条件写入当作原子条件写入。
UID 条件、selector 和其他已支持条件也不能静默丢弃。

Get 在同一对象快照上判断 selector 与精确前置条件：对象不存在或 selector 不匹配返回
NotFound，已选中对象的前置条件不匹配返回 Conflict。单对象写入没有附加条件时，目标不存在
返回 NotFound；携带版本、UID 或 selector 条件时，目标不存在或条件不匹配统一返回 Conflict。
JSON Patch test 失败仍属于补丁校验错误。条件之间按 AND 组合，字段投影不能绕过条件。

Create 通过输入对象返回生成的身份和初始元数据。Update、Patch、Status 写入及 Delete 只
承诺操作结果，不保证刷新输入对象；调用方不能依赖输入对象获得完整持久化结果或最新版本。
需要读取时显式调用 Get，该读取属于独立快照。adapter 和辅助函数不隐式补充回读。

Patch 的类型和字段范围由 backend 明确限定；支持的请求须完整遵守对应补丁语义。无法准确
执行时返回 Unsupported，不近似合并，也不以隐藏的逐对象读取模拟未声明的能力。

单对象条件判断、补丁应用、元数据维护和生命周期结果必须原子完成。条件失败不能改变对象、
推进版本或产生 Watch 事件。实现可以采用原子语句、事务或内部 CAS；调用方固定的前置条件
与一次写入使用的内部 CAS 版本是不同概念。内部 CAS 失败后，只有重新读取并重新判断原始
条件、重新计算写入结果，才能继续尝试；重试受请求 Context 控制。

## 删除、批量操作与 TTL

支持 finalizer 的 backend 对 Delete 使用统一生命周期：没有 finalizer 时物理删除；
存在 finalizer 时设置一次不可变的 DeletionTimestamp，对象继续可读。终止期间 finalizer
只能减少，不能新增，DeletionTimestamp 不能清除或重写。Update 或 Patch 移除最后一个
finalizer 时必须完成物理删除。不能支持该生命周期的 backend 必须拒绝需要 finalizer 的请求。

业务通过在删除前添加自己拥有的保留 finalizer 实现标记删除，并负责决定何时移除。
Store 不解释业务 finalizer 名称，也不提供另一套删除标记或恢复流程。Get、List 和 Count
默认包含仍然存在的终止对象；需要排除时由调用方显式提供条件。

删除传播默认 Background。首次进入删除流程时，Foreground 和 Orphan 分别通过
`foregroundDeletion` 和 `orphan` finalizer 表达外部 GC 的工作。Store 只管理这些标记，
不遍历 OwnerReferences 或递归删除对象；传播请求也不能绕过终止后禁止新增 finalizer 的规则。

DeleteBatch 按 requirements 选择对象，不提供每个对象的 UID 或版本前置条件。
DeleteBatch 和 PatchBatch 中每个对象都遵守单对象的身份、条件、status、版本、Generation、
唯一约束与生命周期规则。批量匹配零个对象时成功且无副作用。批量操作不承诺整体原子性，
返回错误时可能已有对象完成操作；需要全有或全无时使用 backend 支持的显式事务。

TTL 是可选能力，仅用于没有 finalizer 的对象。不能为带 finalizer 的对象设置 TTL，
也不能为仍受 TTL 控制的对象添加 finalizer；约束覆盖创建、更新、补丁和批量入口。
Backend 可以使用原生 TTL 到期机制，不由 Store 引入过期控制器来执行 finalizer 清理。

支持 DryRun 时，Create、Update、Patch、Delete、Batch 和 Status 写入均须完成正常校验
并返回模拟结果，但不能持久化、产生 Watch 事件、创建 TTL 或修改索引。

## 查询、分页与统计

List 与 Count 使用相同的 Scope、LabelRequirements、FieldRequirements、Search 和
SearchFields 匹配语义。Search 是大小写不敏感的子串匹配，默认搜索 id 与 name；显式
SearchFields 覆盖默认字段，多字段按 OR 匹配。条件必须在排序、分页和统计之前执行。
Count 返回全部匹配对象的精确数量，不涉及排序、分页或字段投影。

排序字段、方向及其与分页和索引的组合由 backend 决定支持范围，不统一要求声明索引或
复合排序同方向。支持的排序必须遵守字段值语义：数值按数值比较并保留精度，字符串按字典序；
缺失字段与 null 等价，升序在有值字段前，降序在后。主字段相等时比较下一个排序字段，
不能通过把数值转成字符串改变次序。无显式 Sort 的非分页查询不承诺结果顺序。

支持字段投影时，Fields 可以选择字段或嵌套路径，未选择的业务字段省略；公共元数据与完整
Scope 始终保留。无法无损执行某个过滤、排序或投影请求时返回 Unsupported。

List 用 Limit、Continue 与 Offset 表达分页：

- Limit 为正数时表示本批返回数量上限；零值不限制数量，不隐式选择默认批量。
- Continue 是 backend 生成的不透明字符串。请求为空时开始查询，非空时继续对应遍历。
- Offset 只为新查询指定跳过的匹配对象数。非空 Continue 与非零 Offset 同时出现返回 BadRequest。
- 分页与 Offset 是独立的可选能力。不支持分页时拒绝正数 Limit 或非空 Continue，不能仅截断
  结果；不支持 Offset 时拒绝非零 Offset。
- 响应 Continue 为空表示遍历结束。遍历尚未结束时必须返回续传 token，调用方不能仅凭
  本批对象数少于 Limit 判断结束。

Token 绑定资源、完整查询 Scope、是否包含后代、过滤条件与排序。调用方原样传回 token，
查询上下文变化后须开始新查询；不匹配的上下文或格式无效的 token 返回 BadRequest。
已过期或无法恢复的位置返回 ResourceExpired，不能静默从头开始。Token 格式及其内部的
offset、排序位置或原生游标由 backend 拥有，不构成调用方协议，也不替代每次请求的授权。

Backend 必须为分页维持确定的遍历顺序。未指定业务排序时可选择内部顺序；显式排序值相同时，
须用能区分查询范围内每个对象的位置继续遍历。跨 Scope 查询不能仅靠可能重复的 ID 定位。
普通分页不保证并发修改期间仍然不重不漏。

跨批次固定快照通过显式的可选请求表达，不支持时返回 Unsupported。快照请求的新鲜度要求
在建立快照时满足，随后所有批次读取该快照；过期后明确失败，不能切换到更新的数据视图。
使用 token 或 keyset 本身不代表具有快照保证。

ObjectList 承载对象、Resource、Scopes、ResourceVersion 与 Continue。List 不执行总数
统计；需要总数的调用方显式调用 Count，并在业务或协议响应中组装。独立的 List 和 Count
不自动共享快照，分页快照也不自动为另一次 Count 提供一致性。page、size 与请求 Limit 的
回显、默认分页策略和页码换算均由协议或业务模块拥有。

## Options 与 selector

操作选项使用开放的 variadic option interface。标准 option 是实现
`ApplyToXxx(*XxxOptions)` 的具体值，不使用捕获闭包；最终的 XxxOptions 不同时充当
option。标量按声明顺序覆盖，requirements 按顺序追加。`ApplyXxxOptions` 是统一展开入口，
不执行查询或根据 backend 能力改写意图，操作入口负责其拥有的验证。

同一语义跨多个操作时复用一个具体 option 类型与 `WithXxx` 入口，并实现各自的 ApplyToXxx。
因此 Scope 范围、selector、Search 和 SearchFields 在适用的 List、Count 等操作间共享
语义。公开请求到这些 option 的转换由协议 adapter 拥有。

Labels 和 Annotations 必须无损保存，包括含 `.`, `/` 或 `$` 的键。声明 `LabelSelector` 的实现必须直接按 Requirement 的键匹配，不能因底层数据库路径语法或 Kubernetes label parser 改变键的含义。Annotations 不自动成为 label selector。

本项目 [`selector`](./selector/DESIGN.md) 包（`xiaoshiai.cn/store/selector`）定义递归的布尔选择条件。
`store.Requirements = selector.Requirements` 是 Store API 和各 backend
直接使用的集合类型别名；selector 是表达式语义的唯一 owner。顶层 Requirements 按
AND 组合，其中每个 Requirement 可以是常量、组合节点或一个键上的叶子条件：

```go
type Requirement struct {
    Operator     Operator
    Key          string
    Values       []any
    Requirements Requirements
}
```

有效字段形状如下：

| Operator | Key | Values | Requirements |
| --- | --- | --- | --- |
| `None`, `All` | empty | empty | empty |
| `And`, `Or` | empty | empty | zero or more children |
| `Not` | empty | empty | exactly one child |
| `Exists`, `DoesNotExist` | non-empty | empty | empty |
| single-value comparisons and `Like` | non-empty | exactly one value | empty |
| `In`, `NotIn`, `Contains` | non-empty | zero or more values | empty |

零值 Requirement 是 `None`，不匹配任何对象。空的顶层 `Requirements` 和空的
`And` 匹配全部对象，空的 `Or` 不匹配任何对象。`Not` 对完整的唯一子表达式取反。
缺失键只满足 `DoesNotExist`、`NotEquals` 和 `NotIn`；组合操作在叶子结果上继续
求值。集合操作符允许空 Values；空集合条件不匹配对象。

每个 Store 操作在读取或修改数据前验证 Label 和 Field Requirements。无效字段
形状和未知 Operator 返回 BadRequest。adapter 无法无损执行某个有效操作符或组合
时，必须在产生部分结果或效果前返回 Unsupported。Requirements 必须在排序、分页、
Count 和修改前执行；Watch 使用同一个表达式分别判断旧对象和新对象，再生成成员关系
转换事件。

Label 和 Field 查询中具体字段、叶子操作符与布尔组合的支持范围由 backend 定义，
不通过 Capabilities 枚举。只支持较窄外部 selector 协议的 adapter
只能执行可无损表示的表达式，其余合法请求返回 Unsupported，不能弱化条件。
Label Requirements 和 Field Requirements 是两个最后用 AND 连接的表达式；一个组合
节点不能混合 label 键和 field 键。

Requirement 的规范文本编码由 `selector` 拥有，并扩展 Kubernetes selector 的可读语法。
顶层逗号仍表示 AND；组合节点使用 `&&`、`||`、`!(...)` 和括号；常量写作
`all()` 与 `none()`。叶子沿用 `key=value`、`key in (a,b)`、`!key` 等 selector
形式，并增加 Store 已有的比较和包含操作符。`Requirements.String` 生成规范文本，
`ParseRequirements` 解析同一语法；常用的平铺 Kubernetes label selector 保持相同
写法。

值默认使用 selector 风格的裸文本。包含语法分隔符或空白的字符串使用双引号和 Go
字符串转义；字符串 `"null"` 也加引号，以区别 nil 的 `null` 字面量。除 nil 外，协议
解析出的 operand 都是字符串，与 Kubernetes selector 一致；Requirement 求值层负责
与字段中的布尔值、数值和时间比较。因此具体 Go 数值宽度不是协议语义。Requirement
值只接受 nil、字符串、布尔值、整数、有限浮点数和时间；其他值在 Store 或协议执行前
验证失败。HTTP adapter 只传输该文本，不维护第二套 Requirement wire model。

## Watch 与缓存

Watch、initial Watch 与历史续传分别表达不同的可选能力。普通 Watch 成功返回时订阅必须
已经生效；事件有序且不能静默丢失。有界事件通道采用背压，取消 Context 或幂等 Stop 后
Events 最终关闭；运行期失败发送 terminal error event 后关闭通道。

Delete 事件携带删除前的完整对象。Update 分别判断旧、新对象是否匹配 ID、Scope 和
Label/Field selector：不匹配到匹配产生 Create，均匹配产生 Update，匹配到不匹配产生
Delete，均不匹配则不发送事件。设置 DeletionTimestamp 后仍存在的对象依此规则产生事件，
不能仅因收到 Delete 请求就把它当作已经物理删除。

支持 initial Watch 时，`WithSendInitialEvents()` 在首个 Bookmark 前建立完整状态，
Bookmark 后连续提供变更。调用方先把事件应用到暂存状态，首个 Bookmark 表示该状态已经
构成权威快照，此时原子发布；空快照也发送 Bookmark。快照与实时事件衔接不能留空档，
Bookmark 不能仅表示一次普通 List 已结束。

缓存构建要求 initial Watch 能力，不能用无连续性保证的普通 List 加 Watch 拼接。
首个 Bookmark 前连接中断时丢弃暂存状态，重建成功后才发布新状态。

`WatchEvent.ResourceVersion` 是可选的全局 checkpoint，与对象 ResourceVersion 不同。
支持恢复位置的 backend 可以发送正数 checkpoint，并接受 `WithResourceVersion(R)`
请求其后的事件。没有历史续传能力、位置无法识别或历史已过期时返回 ResourceExpired，
调用方重新建立 initial Watch。对象版本的最大值不能被当作全局 Watch 位置。

缓存和索引视图是权威 Store 的派生读模型，写入始终委托权威 Store，不形成第二个写入源。
默认 Get/List 从权威 Store 读取；缓存只能服务显式允许缓存的读取，不能改变调用方要求的
新鲜度。读取 ResourceVersion 与精确 Preconditions 独立：nil 表示权威读取，零允许缓存，
正值要求满足不早于指定版本的读取保证。对象版本、集合读取版本与 Watch checkpoint 不能
互相推导；无法执行所请求保证时明确失败。

cache、etcdcache 的缓存读取在首次同步或重建期间等待其同步完成条件，等待由调用操作的
Context 控制；Context 结束返回其错误。读取来源由请求契约决定，不能因为缓存未就绪而
临时切换来源，也不能用固定重试次数或延时猜测缓存已经就绪。

## 契约验证

`store/storetest` 应验证基础契约、声明支持能力的正向行为，以及不支持能力的明确拒绝。
测试 fixture 独立声明预期能力与具体请求的支持范围，后者不扩展生产 Capabilities。
不能根据被测实现的声明或本次请求结果决定跳过测试；不支持的请求也必须验证明确拒绝。

版本前置条件与无变化写入、所有写路径的 Generation/Status/生命周期、TTL 与 finalizer
互斥、Scope 精确性、分页结束与 token 失效、显式快照、List/Count 谓词一致、唯一约束及
Watch 连续性均属于公共契约的测试范围。后端特有的编码、事务和索引机制由后端测试验证；
真实后端的运行与环境要求由 [TESTING.md](./TESTING.md) 拥有。
