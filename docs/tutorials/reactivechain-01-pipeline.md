# ReactiveChain 01 · 管道入门

**目标**：用 `A | B | C` 声明式管道跑通第一个链，理解显式 read/write 与段级选择性。

## 核心

- `RunnableLambda(fn, reads={...}, writes={...})`：普通函数包装为管道段
- `a | b | c` → `Pipeline`：按序执行，每段从状态取 `reads` 子集、把返回值（写键 dict）合并回状态
- **段级选择性**：同段输入指纹不变（如 `{"k": 1}` 再次出现）→ 整段跳过，不调用函数
  ——ReactiveChain 的差异化卖点（LangChain 同输入也会全链重跑）
- `prompt | llm | parser`：`ChatPromptTemplate` → `FakeLLM`/`OpenAICompatChatModel` → `StrOutputParser`

## 运行

```bash
uv run --directory python/reactivechain python docs/tutorials/code/reactchain_01_pipeline.py
# 1) 管道输出： {'len': 8}
# 2) 相同输入两次 invoke，段实际执行 1 次（期望 1）
# 3) 提示链输出： 你好！
```

## 进阶

- 段输入提取：`reads` 为空 = 接收完整状态（宽松模式）；缺失键抛 `ReactiveChainError`（Hint 风格）
- 图集成：`chain.to_graph(graph_id=...)` 把整条管道包装为 ReactiveGraph 图（单 effect task）
- stats：`instrument_pipeline(chain)` 后可读 `chain._stats.summary()`（耗时/跳过段数）