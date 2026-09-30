# 与 LangGraph 的对照(Native API vs LangGraph)

> **来源声明(信息真实性)**:langgraph 一侧来自官方文档
> `docs.langchain.com/oss/python/langgraph/`(`graph-api`、`streaming` 页,
> 本次抓取时点);`time-travel` 概念页正文为 JS 渲染、未能在抓取中提取,
> 该行只断言"官方存在 time-travel 概念与 update_state/fork API",细节以官方
> 文档为准。ReactiveGraph 一侧全部来自本仓库代码,不是转述。
>
> 本表只对照**已实现的能力面**,不覆盖双方全部 API。

## 对照表

| 能力 | LangGraph(官方文档) | ReactiveGraph(原生) | 等价性 / 差异 |
|---|---|---|---|
| **图构建** | `StateGraph` + `add_node` / `add_edge` / `compile` | `GraphBuilder.task(...).on(event, ...)` → `build()`;边=**事件路由** | 能力等价:都是"节点 + 连接";ReactiveGraph 把"连接"统一表达为事件→任务映射,任务天然可被多事件触发 |
| **条件分支** | `add_conditional_edges(source, router_fn, path_map)`(graph-api 页出现 9 次) | **事件分发即条件边**:调用方按条件 emit 不同事件;任务内部可决定后续事件;`on=("a","b")` 多事件监听 | 能力等价,形状不同:langgraph 的条件写在**图边函数**里,ReactiveGraph 写在**事件选择**里。二者都表达"运行时决定下一批任务"。无 `add_conditional_edges` 同名 API 是**有意设计** |
| **流式** | `stream_mode="messages"` / `"updates"` / `"custom"`(streaming 页确认) | `stream()` 事件 `values` / `messages` / `custom`;`agent.stream()` 逐 token;任务函数写成**生成器**即自动 token 流 | 事件族一一对应;token 级流双方都支持。ReactiveGraph 额外允许"生成器回调"在 RGP/1 远程透明透出 |
| **持久化 / checkpointer** | `checkpointer`、`MemorySaver`(graph-api 页出现 12/6 次) | `CheckpointSaver` 接口:sqlite / postgres / redis 后端 + 加密序列化 + 事务日志 | 等价;ReactiveGraph 的 checkpoint 带 parent 链与**乐观派生检查**(多 Driver 防丢更新) |
| **time travel** | 官方 time-travel 概念页;`update_state` / `get_state_history` / fork(细节未提取,以官方为准) | `CHECKPOINT_OP restore`:读 checkpoint → **新事务回滚**(不改写历史)→ 继续执行 = 分支 | 方向一致:都能回到过去并重新分支。实现不同:langgraph 改状态后从该点重放;ReactiveGraph 用事务回滚产生新版本,历史 checkpoint 只读不可变 |
| **recursion limit** | `recursion_limit` 编译参数(graph-api 页出现 7 次) | `Scheduler({recursionLimit})`;超限抛 `RecursionLimitError`(带 Hint、不重试) | 等价,均为"超步数即中止";ReactiveGraph 默认无限(向后兼容) |

## 设计主张(为何不镜像 langgraph 的 API 形状)

- **条件边 = 事件选择,不是图边函数**:ReactiveGraph 的执行体是"事件→任务"映射。
  条件分支、扇出(一个事件多个监听)、扇入(多事件一个任务)是同一机制的三个面,
  不需要第四种图元(`add_conditional_edges`)。详见 `docs/spec/native-api.md` §5。
- **流式 = 同一条事件通道**:`messages` / `custom` 是 transient 事件,
  与 `values` 走同一 `stream()`;模型 token、自定义遥测、状态快照不分家。
- **持久化 = checkpoint 链 + 事务**:time travel 不"改写历史",而是派生出新版本,
  与乐观并发(多 Driver)自然兼容。

## 结论

对已对照的六项能力,**能力等价、形状不同**是常态;没有任何一项是
ReactiveGraph 缺失而 langgraph 具备的(在已实现范围内)。差异是设计选择,
不是功能落差。
