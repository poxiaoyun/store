# SQL Store 设计

`store/sql` 通过 `database/sql`、MySQL 与 PostgreSQL 驱动实现
[Store 公共契约](../DESIGN.md)。表布局、SQL 语句、驱动错误与方言差异由本模块拥有，
不引入 ORM。SQL 的执行方式不能改变公共的身份、版本、查询或删除语义。

## 表布局与 Scope

表名是资源名，列名采用持久化模型字段的表示名。一个表示名只对应一列，模型重复声明
同一表示名时拒绝构造。Scope 列来自 ResourceSchema 声明，资源类型通过
`ScopeResourceToFieldName` 单数化为列名，并对应模型字段。

SQL 支持受限的 Scope 路径：只有声明的 Scope 列能够与完整有序路径一一对应时，才能用
这些列定位对象。路径的顺序、深度和每一级名称必须参与验证与匹配；重复资源类型、列名
碰撞或其他无法无损表示的路径返回 Unsupported。完整 scopes 值保存于对象中，派生列
不能覆盖或替代这份信息。根 Scope 精确匹配根对象，不能通过省略所有 Scope 条件查询全表。

每种资源始终以 id 为主键，在所有 Scope 中唯一；创建后 id 与 Scope 均不可修改。
不同 Scope 创建相同 id 返回 AlreadyExists。ScopeKeys 用于 Scope 映射和查询索引，
不参与主键；读写仍按完整 Scope 隔离，不能只凭全局唯一 id 跨 Scope 访问。
数据库必须在并发写入下保证身份及声明的唯一约束，列可空性不能削弱这些保证。
可选业务字段省略时可写入 NULL，不能因此放宽身份或唯一约束。

时间字段落为 `timestamptz` 或 `datetime(6)`；项目拥有的元数据时间类型在编解码层与
`time.Time` 转换，不按普通结构体落 JSON。map、slice、struct 落为 `jsonb` 或 `json`。
字符串默认使用 text；被物理索引且需要有界键的字符串默认使用 varchar(255)，可通过
ResourceSchema.FieldSpecs 的 Size 声明大小。

## Schema 与索引

构造 Store 时对注册资源建立所需表结构，并通过数据库 introspection 补充缺失的列与索引。
自动结构维护不删除列、不改变已有字段类型或约束；这些变更由显式迁移拥有。
已有表必须具有仅包含 id 的主键，否则构造失败，由显式迁移建立约束。

普通索引是可选优化，方言不支持的普通索引可以不建立。唯一约束必须在 Store 可用前确认
能够完整执行，包括字段、Scope、表达式和空值规则；无法表示、建立或确认等价时构造失败。
已有同名索引不能仅凭名称或 Unique 标志被认定满足声明，尤其不能把不同嵌套表达式当作
同一个唯一约束。SQL 不以先查询后无条件写入替代数据库的并发唯一性保证。

嵌套路径索引由方言映射为表达式索引或生成列索引。方言无法表示的 partial 唯一索引或
复合可空唯一索引必须在构造时拒绝，不能自动替换成不同的唯一性语义。

## 查询、排序与分页

FieldRequirements 指向列或嵌套 JSON 路径，LabelRequirements 指向 labels 文档内的字面键。
标签 key 中的点号不能被解释为多段字段路径。条件结构由项目 selector 的规则验证，
无效条件返回 BadRequest，合法但方言不能无损执行的条件返回 Unsupported。

嵌套路径由方言负责提取、赋值与删除。比较和排序必须保留字段值的类型语义，不能一律转成
JSON 文本后比较数值。无法表达某个字段或类型组合的比较时明确拒绝。排序可以支持未索引
字段及混合升降序，但具体范围由方言与资源映射决定，不能改变根契约规定的排序结果。

List 与 Count 共用 Scope、selector、Search 和 SearchFields 的条件语义。Count 使用
匹配条件统计全部对象，不应用分页、排序或投影；List 只查询对象和续传所需的信息，
不隐式执行 COUNT。两个独立调用不承诺来自同一数据库快照。

LIMIT、OFFSET 和 keyset 都是 SQL 的内部定位机制。分页能力只有在本模块能够维持确定
顺序、生成并验证续传 token、正确标记遍历结束时才可声明；单独执行 LIMIT 不构成完整分页。
不支持该能力时，对正数 Limit 或非空 Continue 返回 Unsupported，不能截断后返回空 token。
Offset 是独立能力，仅用于新查询，不能与非空 Continue 同时使用。

Limit 为零不限制数量；只请求 Offset 时，由方言表达跳过后返回全部匹配对象的语义。
分页排序相等时必须补足查询范围内的唯一定位，跨 Scope 查询不能只用 id 区分对象。
跨批次快照是独立能力：普通事务或单条 SELECT 的一致性不自动成为跨请求快照保证，
无法在续传期间维持同一快照时必须拒绝显式快照请求。

## 写入、版本与生命周期

单对象条件判断、写入、元数据计算和最终生命周期结果必须原子完成，可以使用一条语句或
事务。Update 保留存储中的 status 与不可变元数据；Patch 在当前存储值上应用，不把传入
对象上的 ResourceVersion 隐式作为条件。调用方版本条件、UID 条件及 requirements
必须按根契约执行，不能因 SQL 表达式限制而丢弃。

普通写入直接执行 SQL，不预读完整对象、不回读结果、不执行内部 CAS 重试。
ResourceVersion 使用数据库表达式递增，包括等值赋值。带条件的写入未匹配时返回 Conflict，
无附加条件的单对象写入未匹配时返回 NotFound，不为细分失败原因追加查询。

Generation 只比较 ResourceSchema 声明的业务字段，没有声明时保持不变。比较基于同一次
写入的旧值与新值，保持类型和数值精度；元数据与 status 不参与，等价赋值不推进。
`Status().Update` 与 `Status().Patch` 只改变 status 与 ResourceVersion。SQL 只接受顶层标量
Generation 跟踪字段；复杂结构或嵌套跟踪声明在绑定 Schema 时返回 Unsupported。

Delete 根据当前 finalizer 决定设置 DeletionTimestamp 或物理删除。终止期间不能新增
finalizer 或改写删除时间；Update/Patch 清空最后一个 finalizer 时，物理删除是同一次
原子生命周期操作的一部分，不能留下已成功返回但未完成删除的中间状态。Delete 与 finalizer
修改可以使用事务和必要的生命周期元数据读取；普通字段 Patch 与 Status 写入不承担这些成本。

PatchBatch 的列赋值不能直接覆盖服务端元数据或 status，但 Store 必须为每个对象维护版本、
声明字段的 Generation 和生命周期。DeleteBatch 不能通过直接 DELETE 绕过 finalizer。
批量操作使用集合语句，不先 List 再逐对象执行。不支持的补丁或生命周期组合在副作用前
返回 Unsupported。普通批量调用不声明跨 backend 的整体原子性。

驱动错误映射为 common/errors 的领域语义：唯一冲突为 AlreadyExists，非空或检查约束
违反为 BadRequest，引用缺失为 NotFound。HTTP 状态码由 REST adapter 决定。未识别的驱动错误保留错误链，不附加 SQL 文本或绑定参数。

## 事务与能力范围

`Transaction` 用 BeginTx 打开事务，回调收到绑定该事务的 Store；所有参与事务的操作
都必须通过它执行。嵌套 Transaction 返回 Unsupported。默认隔离级别采用后端默认值，
显式 WithIsolationLevel 无法执行时返回 Unsupported。

死锁与序列化失败按 WithMaxRetries 重试，默认 3 次，退避上限 100ms；重新执行回调前
必须结束并回滚失败事务。回调须允许重复执行，不能假设事务内的外部副作用会随数据库回滚。

本适配器不提供 Watch、initial Watch、历史事件续传、TTL 或 DryRun，不用普通查询近似
这些保证。单对象与批量 Patch 使用相同的补丁语义，不能用不同引擎的近似 JSON 行为
替代请求。

SQL Patch 仅支持 Merge Patch，允许标量赋值、整数组替换、一层 JSON 对象合并及键删除。
嵌套对象合并、JSON Patch 在副作用前返回 Unsupported。空对象合并保留已有对象键；
null 删除字段，不能把合并当成整列替换。单对象与批量写入使用同一套赋值语义。
批量 Patch 不支持设置非空 finalizer 或携带 resourceVersion 条件；只有无 selector 时允许
批量清空 finalizer，并且必须
在同一事务中删除已经终止的匹配对象。Update 的完整替换包含 finalizer 替换，清空时需要
事务完成删除；普通字段 Patch 与 Status 写入只执行一条 UPDATE。

DeleteBatch 不接受依赖 resourceVersion 或 deletionTimestamp 的筛选，避免删除标记改变
后续物理删除的匹配范围。集合操作在事务内先更新并锁定匹配行，再完成物理删除。
