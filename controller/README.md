# Controller

Controller 从 Source 接收对象 key，按 key 去重并串行协调同一对象。默认一个 worker，待处理任务在 worker 空闲后立即执行。WithConcurrent 设置单个 Controller 的 worker 数量，非正值使用默认值。

需要限制协调频率时，在初始化时显式传入 WithRateLimit：

```go
worker := controller.NewTypedController(
    "objects",
    reconciler,
    controller.WithConcurrent[ObjectKey](4),
    controller.WithRateLimit[ObjectKey](10, 100),
)
```

WithRateLimit 使用令牌桶：第一个参数是每秒协调次数，第二个参数是突发额度。额度由同一 Controller 的全部 worker 共享；复用初始化选项创建多个 Controller 时，各自拥有独立额度。等待额度响应运行 Context 的取消，取消后不会调用 Reconcile。

执行限速和失败退避分别控制。ControllerOptions.RateLimiter 为失败 key 计算重试延迟；Result.Requeue 与 RequeueAfter 表达调用方要求的再次协调时间。InitializeReconciler 在启动 worker 前执行。

验证：`go test -race ./controller`。时钟与并发测试使用 testing/synctest。
