# Reactive Agent Cookbook

## Native ReAct agent

```python
from reactivegraph.prebuilt import ToolSpec, create_react_agent

def weather(city: str) -> dict:
    return {"city": city, "temp_c": 20}

agent = create_react_agent(
    model=model_callable,
    tools=[ToolSpec(name="weather", fn=weather)],
)
out = agent.invoke([{"role": "user", "content": "weather in Paris?"}])
```

## Parallel tool calls

`ToolNode` can execute a batch of independent tool calls with bounded
concurrency while preserving the model's tool-call order:

```python
tool_node(tool_calls, concurrency=4)
```

One tool failure becomes an error message; sibling calls still return.

## Agent state

Use the versioned native schema instead of LangGraph channels/reducers:

```python
from reactivegraph.prebuilt import AgentState

state = AgentState(messages=({"role": "user", "content": "go"},))
```

`AgentState.to_dict()` and `AgentState.from_dict()` are RGP/1-safe and validate
the schema version.

## Human in the loop

```python
agent = ReactiveAgent(..., interrupt_before=("charge",), interrupt_after=("charge",))
state = agent.invoke_state(state)
state = agent.resume_state(state, {"id": state.interrupts[0]["id"], "response": "approve"})
```

Multiple pending interrupts have distinct ids and can be approved individually.

## External tools

OpenAPI:

```python
from reactivechain import from_openapi
tools = from_openapi(spec, base_url="https://api.example.com", headers=headers)
```

MCP:

```python
from reactivechain import from_mcp
tools = from_mcp("https://mcp.example.com", headers=headers)
```

Both return native `BaseTool` objects with schemas and optional reactive
metadata.
