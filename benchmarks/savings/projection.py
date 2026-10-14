"""把 demo 的实测节省投影到真实规模（诚实版：区分"实测"与"线性外推"）。"""
from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
rg = json.loads((HERE / "results" / "reactivegraph.json").read_text())
lg = json.loads((HERE / "results" / "langgraph.json").read_text())

rg_in = sum(c["tokens_in"] or 0 for c in rg["calls"])
rg_out = sum(c["tokens_out"] or 0 for c in rg["calls"])
lg_in, lg_out = lg["tokens_in_total"], lg["tokens_out_total"]

print("=== 实测（2 次 run，同一份文档，仅 tone 不同）===")
print(f"ReactiveGraph: {rg['llm_calls_total']} 次调用, {rg_in} in / {rg_out} out tokens")
print(f"LangGraph    : {lg['llm_calls_total']} 次调用, {lg_in} in / {lg_out} out tokens")
print(f"节省         : {lg['llm_calls_total'] - rg['llm_calls_total']} 次调用, "
      f"{lg_in - rg_in} in / {lg_out - rg_out} out tokens")
print()
print("重要限定：")
print("  1. 省的是**确定性抽取任务**（纯函数语义）。agent 主链（model/tools）")
print("     在 fork 里注册为 effect，**不省**。")
print("  2. 只有当同一抽取输入被重复触达时才省。首次运行不省。")
print("  3. 投影依赖『输入重复率』假设，必须按实际 workload 校准，不能当承诺。")
print()
print("=== 线性外推（仅示意真实规模，不是承诺）===")
# 真实场景：N 次 run 共享同一批文档（例如重试、多风格改写、多轮追问）
per_repeat_rg_in = rg_in - (rg_in - lg_in) if False else rg_in
saved_in = lg_in - rg_in
saved_out = lg_out - rg_out
for n in (10, 100, 1000):
    print(f"  {n:>5} 次重跑（文档不变）：fork 省 {saved_in * (n-1):>9} in / "
          f"{saved_out * (n-1):>7} out tokens vs LangGraph")
