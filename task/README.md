# task

`task` 定义了提交、执行和管理异步任务的公共接口，适合组织初始化、发送通知、生成文件、异步订单处理和定时记账等场景。`task/inmemory` 提供用于测试和本地开发的进程内实现，进程退出后任务会丢失。

任务采用 **at-least-once** 语义，同一个任务可能因失败重试或异常恢复而执行多次，因此 Handler 必须幂等。

## 接口

调用方应依赖所需的最小接口：

- `Submitter`：业务代码提交任务。
- `Worker`：进程启动时注册并运行 Handler。
- `Manager`：管理端查询、取消和人工重试任务。

内存实现同时支持这三个接口：

```go
tasks := inmemory.New(inmemory.Options{
    MaxWorkers: 4,
})
```

需要任务在进程异常退出后继续存在时，可以使用 MongoDB 实现：

```go
tasks, err := mongodb.New(ctx, database.Collection("tasks"), mongodb.Options{
    MaxWorkers: 4,
})
```

MongoDB 实现要求 MongoDB 5.0 或更新版本。多个进程可以共享同一个 collection，并分别运行 Worker。

任务可能处于 `Pending`、`Running`、`Succeeded`、`Dead` 或 `Canceled` 状态。不可重试的错误和耗尽尝试次数都会使任务进入 `Dead`。

## 提交任务

```go
func submitOrganizationProvision(
    ctx context.Context,
    submitter task.Submitter,
    organizationID string,
) (string, error) {
    payload, err := json.Marshal(struct {
        OrganizationID string `json:"organizationID"`
    }{
        OrganizationID: organizationID,
    })
    if err != nil {
        return "", err
    }

    return submitter.Submit(ctx, task.Task{
        Type:           "organization.provision.v1",
        Payload:        payload,
        IdempotencyKey: "organization.provision/" + organizationID,
        Labels: map[string]string{
            "organizationID": organizationID,
        },
    }, task.SubmitOptions{})
}
```

`Submit` 成功表示任务已经被接受。调用请求的 `context.Context` 随后被取消，不代表任务也被取消。

`IdempotencyKey` 用于避免同一业务动作被重复提交。相同任务和选项重复提交时应返回原任务 ID；相同 key 用于不同任务时返回 `ErrConflict`。key 应来自稳定的业务标识，例如 `organization.provision/{organizationID}`。

`Labels` 用于任务查询和筛选，`Annotations` 用于保存不参与执行的附加信息。

需要延迟执行时设置 `NotBefore`。它表示最早执行时间，不保证任务准时开始：

```go
id, err := submitter.Submit(ctx, work, task.SubmitOptions{
    NotBefore: time.Now().Add(10 * time.Minute),
})
```

Payload 建议只保存业务对象 ID，由 Handler 执行时读取最新数据。不要在 Payload 中放置密码或令牌等敏感信息。如果 Payload 发生不兼容变化，可以把版本写进 `Type`，例如 `organization.provision.v2`。

## 处理任务

Worker 在运行前注册任务类型对应的 Handler：

```go
err := worker.Register(
    "organization.provision.v1",
    task.HandlerFunc(func(ctx context.Context, info task.TaskInfo) error {
        var payload struct {
            OrganizationID string `json:"organizationID"`
        }
        if err := json.Unmarshal(info.Payload, &payload); err != nil {
            return task.NoRetry(err)
        }

        return provision(ctx, payload.OrganizationID)
    }),
    task.HandlerOptions{
        MaxAttempts: 10,
        Timeout:     5 * time.Minute,
    },
)
if err != nil {
    return err
}

return worker.Run(ctx)
```

Handler 的返回语义：

```go
return nil                                  // 成功
return err                                  // 使用默认退避策略重试
return task.NoRetry(err)                    // 不再重试，进入 Dead
return task.RetryAfter(err, 30*time.Second) // 至少等待指定时间后重试
```

Payload 无效或违反不可恢复的业务规则时使用 `NoRetry`；网络错误、超时和暂时不可用通常直接返回。Handler 必须能安全地重复执行，例如通过业务唯一键避免重复创建或扣费。

## 管理任务

```go
page, err := manager.List(ctx, meta.ListOptions{
    Size:          20,
    Sort:          "-time",
    FieldSelector: "status.state=Dead",
    LabelSelector: "organizationID=" + organizationID,
})
if err != nil {
    return err
}
for _, item := range page.Items {
    log.Info("dead task", "id", item.ID, "error", item.Status.LastError)
}

info, err := manager.Get(ctx, id)
if err != nil {
    return err
}

switch info.Status.State {
case task.StateSucceeded:
    // 已完成
case task.StateDead:
    // 展示 info.Status.LastError，允许人工重试
}

if err := manager.Cancel(ctx, id); err != nil {
    return err
}

if err := manager.Retry(ctx, id, time.Time{}); err != nil {
    return err
}
```

`Manager.List` 使用 common 的 `meta.ListOptions`，但不同实现能够支持的搜索、排序和选择能力可能不同。内存和 MongoDB 实现支持 page 分页和 `LabelSelector`；`Size>0` 选择 page，`Page<1` 按第一页处理，否则不分页。它们不支持 continuation，因此 `Limit>0` 返回 `ErrInvalidArgument`；未选择 continuation 时忽略 `Continue`。`FieldSelector` 支持 `id`、`type`、`idempotencyKey`、`status.state`、`status.attempt`、`status.notBefore` 和 `creationTimestamp`；`Sort` 支持其中除 `idempotencyKey` 外的字段，并可以使用 `time` 作为 `creationTimestamp` 的别名。两种实现都不支持非空 `Search`。

`Manager.Cancel` 只接受 `Pending` 任务。成功返回表示任务已经进入 `Canceled`，不会再开始新的 Handler 执行尝试；`Running`、`Succeeded` 和 `Dead` 任务返回 `ErrInvalidState`。`Manager.Retry` 可以把 `Dead` 或 `Canceled` 任务重新变为 `Pending`，传入零值时间表示立即重试。

任务状态只描述异步执行情况，不能替代订单、支付或账本自身的业务状态和幂等约束。
