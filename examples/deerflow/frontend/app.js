/* DeerFlow-style workbench frontend: chat tab (token typewriter) + capability lab. */
"use strict";

// ---------- tabs ----------
const $ = (sel) => document.querySelector(sel);

$("#tab-chat").addEventListener("click", () => switchTab("chat"));
$("#tab-lab").addEventListener("click", () => switchTab("lab"));

function switchTab(name) {
  $("#tab-chat").classList.toggle("active", name === "chat");
  $("#tab-lab").classList.toggle("active", name === "lab");
  $("#pane-chat").classList.toggle("active", name === "chat");
  $("#pane-lab").classList.toggle("active", name === "lab");
}

// ---------- chat: token typewriter ----------
const log = $("#chat-log");

function addMsg(cls, text) {
  const el = document.createElement("div");
  el.className = "msg " + cls;
  el.textContent = text;
  log.appendChild(el);
  log.scrollTop = log.scrollHeight;
  return el;
}

$("#chat-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const q = $("#chat-input").value.trim();
  if (!q) return;
  $("#chat-input").value = "";
  addMsg("user", q);

  const reply = addMsg("sys");
  const cursor = document.createElement("span");
  cursor.className = "token cursor";
  reply.appendChild(cursor);

  const btn = $("#chat-form button");
  btn.disabled = true;
  try {
    if ($("#chat-real").checked) {
      // Real LLM via AGNES gateway, TRUE token streaming over SSE: every
      // data frame is one model token rendered as it arrives.
      addMsg("meta", "agnes · true token stream (SSE)");
      const es = new EventSource("/api/chat-stream?q=" + encodeURIComponent(q));
      es.onmessage = (ev) => {
        if (ev.data === "[DONE]") {
          cursor.remove();
          es.close();
          return;
        }
        let d;
        try {
          d = JSON.parse(ev.data);
        } catch {
          return;
        }
        if (d.error) {
          cursor.remove();
          reply.textContent = "✗ " + d.error;
          es.close();
          return;
        }
        if (d.token) {
          const span = document.createElement("span");
          span.className = "token";
          span.textContent = d.token;
          reply.insertBefore(span, cursor);
          log.scrollTop = log.scrollHeight;
        }
      };
      es.onerror = () => {
        cursor.remove();
        es.close();
      };
      return;
    }
    const res = await fetch("/api/stream?q=" + encodeURIComponent(q));
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    let tokenCount = 0;
    for (const ev of data.events || []) {
      if (ev.eventType === "messages") {
        const t = document.createElement("span");
        t.className = "token";
        t.textContent = String((ev.payload && ev.payload.message && ev.payload.message.content) || "");
        reply.insertBefore(t, cursor);
        tokenCount++;
        log.scrollTop = log.scrollHeight;
      } else if (ev.eventType === "values") {
        addMsg("meta", "values → " + JSON.stringify((ev.payload && ev.payload.state) || {}));
      }
    }
    cursor.remove();
    if (tokenCount === 0) {
      reply.textContent = JSON.stringify(data, null, 2);
    }
  } catch (err) {
    cursor.remove();
    reply.textContent = "✗ " + err.message;
  } finally {
    btn.disabled = false;
    $("#chat-input").focus();
  }
});

// ---------- capability lab ----------
const CAPS = [
  { id: "invoke", method: "POST /api/invoke", title: "事件路由 + 并行任务", desc: "一个事件并行派发到多个任务,输出合并成 state(真实 Driver)。", run: () => api("/api/invoke", { q: "hello world" }) },
  { id: "stream", method: "GET /api/stream", title: "LLM token 级流", desc: "生成器任务逐 token 产出 messages 事件,展示完整事件序列。", run: () => api("/api/stream?q=" + encodeURIComponent("reactive token flow")) },
  { id: "computed", method: "GET /api/computed", title: "computed 派生值", desc: "computed 按读集缓存/失效,派生值进入 state。", run: () => api("/api/computed") },
  { id: "scope", method: "GET /api/scope", title: "scope 多租户隔离", desc: "tenantA / tenantB 写同一 key 互不碰撞。", run: () => api("/api/scope") },
  { id: "checkpoint", method: "POST /api/checkpoint", title: "checkpoint 持久化", desc: "run 写入 sqlite 持久化 checkpoint(REACTIVEGRAPH_DB)。", run: () => api("/api/checkpoint") },
  { id: "checkpoints", method: "GET /api/checkpoints", title: "checkpoint 列表", desc: "列出线程历史 checkpoint(时间线)。", run: () => api("/api/checkpoints") },
  { id: "restore", method: "POST /api/restore", title: "time travel 回滚", desc: "取最新 checkpoint 回滚 → 派生新版本(历史只读不可变)。", run: async () => { const cps = await api("/api/checkpoints"); const list = (cps.checkpoints || []); if (!list.length) return { error: "先运行 checkpoint 卡片" }; return api("/api/restore", { checkpoint_id: list[list.length - 1].checkpointId }); } },
  { id: "resume", method: "POST /api/resume", title: "human-in-the-loop 中断恢复", desc: "run 停在 Interrupt,resume 携带人类答复继续(finalize)。", run: () => api("/api/resume", { q: "deploy to prod?", answer: "approved" }) },
  { id: "agent", method: "POST /api/agent", title: "prebuilt ReAct agent", desc: "create_react_agent + ToolNode:模型发 tool_call → 工具执行 → 收尾(fake model,机制演示)。", run: () => api("/api/agent", { question: "what is the weather in Paris?" }) },
  { id: "agent-real", method: "POST /api/agent-real", title: "真实 LLM Agent", desc: "AGNES(agnes-2.5-flash)真实模型 + 真实工具调用:user → tool_call → 工具执行 → 最终回答。", run: () => api("/api/agent-real", { question: "What is the weather in Paris?" }) },
  { id: "store", method: "POST+GET /api/store", title: "long-term store", desc: "store_put / store_get 持久化键值(跨重启)。", run: async () => { await api("/api/store", { key: "demo", value: { savedAt: new Date().toISOString() } }); return api("/api/store?key=demo"); } },
  { id: "dot", method: "GET /api/dot", title: "dot 导出", desc: "EXPORT_DOT:运行时把编译图渲染成 Graphviz DOT(graph-as-code)。", run: () => api("/api/dot") },
  { id: "vector", method: "GET /api/vector", title: "向量检索", desc: "VECTOR_UPSERT/SEARCH:upsert 三点后 cosine 最近邻返回排序结果。", run: () => api("/api/vector") },
  { id: "recursion", method: "GET /api/recursion", title: "recursion limit", desc: "10 任务图 + config.recursionLimit=3 → 真实 RecursionLimitError(带 Hint、不重试)。", run: () => api("/api/recursion") },
];

async function api(url, body) {
  const res = await fetch(url, {
    method: body === undefined ? "GET" : "POST",
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return res.json();
}

function fmt(v) {
  if (typeof v === "string") return v;
  return JSON.stringify(v, null, 2);
}

const grid = $("#lab-grid");
for (const cap of CAPS) {
  const card = document.createElement("div");
  card.className = "card";
  card.innerHTML = `
    <h3>${cap.title} <span class="method">${cap.method}</span>
      ${cap.native ? '<span class="badge warn">native·绑定待办</span>' : '<span class="badge ok">真实引擎</span>'}</h3>
    <p>${cap.desc}</p>
    <button class="run">执行</button>
    <pre style="display:none"></pre>`;
  const pre = card.querySelector("pre");
  const btn = card.querySelector(".run");
  btn.addEventListener("click", async () => {
    btn.disabled = true;
    pre.style.display = "block";
    pre.textContent = "… 执行中(真实 Driver)";
    pre.className = "";
    try {
      const out = await cap.run();
      pre.textContent = fmt(out);
      pre.className = "ok";
    } catch (err) {
      pre.textContent = "✗ " + err.message;
      pre.className = "err";
    } finally {
      btn.disabled = false;
    }
  });
  grid.appendChild(card);
}
