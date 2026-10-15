# REST Store 设计

`store/rest` 拥有 HTTP 与 [Store 公共契约](../DESIGN.md) 之间的映射。HTTP query、body、
状态码与流式 Watch 由本模块处理；核心 Store 拥有查询、前置条件和生命周期语义。

Server 与 Client 实现可远程访问的通用 Store 协议；对象级函数供已有领域路由复用
CRUD 与 Watch。两者共享 Store option 和对象语义，但分别拥有请求参数与响应形态。
资源 Schema 由构造方提供。能力声明必须反映远端能够提供的语义，不能因为 HTTP 可以
编码某个字段就宣称支持该能力。

## 列表请求与响应

通用 Store HTTP 协议使用 limit、continue 和 offset 表达统一分页。Client 原样传递
Limit、Continue、Offset 及显式快照请求；Server 将它们映射为 Store option，不根据
backend 类型替调用方切换分页方式、清除 token 或取消快照要求。

空 continue 开始查询，非空值用于续传。非空 continue 与非零 offset 同时提供时返回
BadRequest。没有默认策略时，limit 零值不限制返回数量。backend 不支持分页、Offset
或快照时，Server 保留 Unsupported，不能只返回截断的对象或悄悄降级到普通读取。

对象级业务接口可以使用 page/size 请求形态，由协议 adapter 换算为
`Offset=(max(Page,1)-1)*Size` 和 Limit。请求默认值只补齐未指定的意图，不覆盖显式
token 或位置；业务约束在转换后以具体 Store option 追加。公开请求解析、默认值、页码
换算和最终响应组装由本模块或业务接口拥有，不由 Store 核心依赖公开分页请求类型。

Client 不解析或重建 token，只负责 HTTP 编码与原样传输。资源、Scope、过滤条件与排序
须维持同一查询上下文，由 Store 验证；格式无效或上下文不匹配返回 BadRequest，失效
位置返回 ResourceExpired。响应 continue 为空才表示遍历结束，不能根据本批数量或
是否包含总数推断结束。快照续传不能自动改为新查询。

通用 List 响应承载对象及 Store 列表元数据，不承担总数统计。Count 是独立操作，传递与
List 相同的 Scope、selector、Search 和 SearchFields，不携带分页、排序或投影。
需要 total 的业务接口显式调用 Count 后组装响应，可以在一次 HTTP 请求内完成这两次
Store 调用；独立 List 与 Count 不自动共享快照。

page、size 与 limit 的响应回显属于业务接口。协议 adapter 不向核心 ObjectList 回填
请求参数或统计结果，也不为了组装分页响应隐式执行 Count。

## 查询条件与读取语义

Client 使用本项目 selector 的文本编码，将完整 LabelRequirements、FieldRequirements
分别写入 labelSelector、fieldSelector。编码保留 None、All、And、Or、Not 和叶子节点，
不在客户端弱化表达式或通过拉取后过滤近似执行。Server 使用同一 selector 解析器，
解析失败映射为 BadRequest，不维护另一套表达式语义。

Search 与 SearchFields 在 List、Count 间无损传递；Fields 只影响支持投影的读取。
分页、排序、搜索、投影及分页快照请求不进入 Watch。资源、Scope 和适用于操作的查询条件
保持各自的 Store 语义，不能由 HTTP 路由重新解释 Scope 的层级身份。

读取 resourceVersion 的缺省、零值及正值保留其新鲜度含义，不能把缺省转换为允许缓存。
精确读取前置条件使用 preconditionUID、preconditionResourceVersion，与新鲜度独立；
按 query 参数是否出现保留指针语义，显式空 UID 或零版本不能视为未提供。

## 写入、删除与错误

Update 保留对象携带的 ResourceVersion。Patch 和 Status Patch 的显式前置条件通过
preconditionUID、preconditionResourceVersion 传递，与补丁内容分离；Merge Patch 中
顶层 resourceVersion 的含义由 Store 拥有。Client 不把 Unsupported 改为无条件写入，
也不在 HTTP adapter 中另外维护版本、Generation 或 finalizer。

单对象 Delete 不使用 request body，uid、resourceVersion、propagationPolicy、dryRun、
labelSelector 和 fieldSelector 通过 query 传递。参数是否出现决定对应前置条件是否存在，
Server 转换一次后调用底层 Delete；DeleteBatch 使用 dryRun 和 selectors，不接受逐对象
UID 或版本前置条件。批量操作保持 Store 的部分成功可能性，不暗示整体事务。

Store 删除成功可能表示对象进入终止状态；adapter 不能据此自行认定已物理删除，不能绕过
finalizer 或把终止对象隐式从查询中隐藏。业务保留 finalizer 的命名及移除时机由业务拥有。

Server 保留 common/errors 的语义进行 HTTP 映射：BadRequest 为 400，NotFound 为 404，
Conflict 为 409，ResourceExpired 为 410，JSON Patch test 失败的补丁校验错误为 422。
Unsupported 保持可识别的领域错误，不降级为成功。Client 恢复对应领域错误供调用方判断。

Watch 传输必须保留事件顺序、initial Bookmark、checkpoint 和 terminal error，不能把
流已建立误认为初始状态已完成。连接取消与 Stop 传递到 Watch 生命周期；远端不支持
initial Watch 或历史续传时保留相应错误，不由 adapter 拼接普通 List 与 Watch。
