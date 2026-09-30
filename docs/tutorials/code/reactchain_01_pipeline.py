"""ReactiveChain 教程 01：管道入门 —— `A | B | C` 声明式管道。

运行：uv run --directory python/reactivechain python docs/tutorials/code/reactchain_01_pipeline.py
"""
import os
import sys

# 引导：把 reactivechain 源码包加入 sys.path（教程脚本独立可跑）
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "python", "reactivechain"))
)

from reactivechain import (
    ChatPromptTemplate,
    FakeLLM,
    Pipeline,
    RunnableLambda,
    StrOutputParser,
)


# --- 1. 普通函数做管道段：reads 声明输入键、writes 声明输出键 ---
def to_upper(state: dict) -> dict:
    return {"text": str(state["text"]).upper()}


def count_len(state: dict) -> dict:
    return {"len": len(state["text"])}


chain = RunnableLambda(to_upper, reads={"text"}, writes={"text"}) | RunnableLambda(
    count_len, reads={"text"}, writes={"len"}
)
out = chain.invoke({"text": "reactive"})
print("1) 管道输出：", out)  # {'len': 8}
assert out == {"len": 8}


# --- 2. 段级选择性：相同输入重跑时跳过函数调用（差异卖点）---
calls: list[int] = []


def counted(state: dict) -> dict:
    calls.append(1)
    return {"v": state["k"] + 1}


selective = Pipeline([RunnableLambda(counted, reads={"k"}, writes={"v"}, pure=True)])
selective.invoke({"k": 1})
selective.invoke({"k": 1})  # 指纹命中 → 跳过
print(f"2) 相同输入两次 invoke，段实际执行 {len(calls)} 次（期望 1）")
assert len(calls) == 1


# --- 3. 提示 → 模型 → 解析（M2+M3 组件链）---
prompt = ChatPromptTemplate([("system", "你是{lang}助手"), ("human", "{q}")])
llm = FakeLLM(["你好！"], name="demo")
pipe = prompt | llm | StrOutputParser()
answer = pipe.invoke({"lang": "中文", "q": "打个招呼"})
print("3) 提示链输出：", answer["output"])
assert answer["output"] == "你好！"

print("教程 01 OK")