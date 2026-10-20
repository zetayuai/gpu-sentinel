"""Incident Response Copilot — agentic loop with ground truth and a human approval gate.

Flow (per the architecture):
  1. Analyst prompt (Streamlit)
  2. Senso.ai ground truth: retrieve the org's actual playbooks/SOPs
  3. LLM drafts SQL  ->  HUMAN APPROVAL GATE
  4. Approved SQL runs on ClickHouse (ms latency over millions of rows)
  5. Verdict grounded in the playbook, with the SOP's own response steps
Sidebar tracks tokens/cost per LLM step.
"""
import os, re, time
import pandas as pd
import streamlit as st
from openai import OpenAI
from senso import get_playbook_context

BACKEND = os.environ.get("BACKEND", "clickhouse")
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
# Empty OPENAI_BASE_URL means "use the default api.openai.com" (compose always sets the var)
# SDK treats an empty OPENAI_BASE_URL env var as a literal URL, and compose always
# sets the var — so pass an explicit base_url: custom endpoint or the real default.
client = OpenAI(base_url=os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1")

CH_SCHEMA = """
ClickHouse database: telemetry. Dialect: ClickHouse SQL (toStartOfMinute, now(), INTERVAL).
Table telemetry.gpu_metrics (raw 5s samples, millions of rows):
  event_time DateTime64(3), host_id String ('host-0007'), rack String, gpu_index UInt8,
  gpu_uuid String, gpu_util Float32 (0-100), sm_active Float32 (0-100),
  mem_used_gb Float32, temp_c Float32, power_w Float32, energy_j_total Float64,
  ecc_dbe UInt8, xid_error UInt8
Table telemetry.alerts (insert-time detections):
  detected_at DateTime, rule String ('cryptojack_suspect'|'hw_fault'|'thermal'),
  severity String, host_id String, gpu_index UInt8, detail String, event_time DateTime64(3)
Notes: high gpu_util with LOW sm_active = fake load / cryptomining signature.
"""
TRINO_SCHEMA = """
Trino catalog.schema: iceberg.telemetry. Dialect: Trino SQL.
Table gpu_metrics_1m: window_start/window_end TIMESTAMP, host_id, rack, gpu_index,
  avg_util, max_util, avg_sm_active, avg_temp, max_temp, avg_power, ecc_dbe, xid_errors, samples
"""
SCHEMA = CH_SCHEMA if BACKEND == "clickhouse" else TRINO_SCHEMA
SQL_PROMPT = f"""You translate incident-response questions into ONE SQL query.
{SCHEMA}
Rules: SELECT-only. Always include a LIMIT (<=200). Return ONLY the SQL, no markdown, no commentary."""

PRICE_IN = float(os.environ.get("PRICE_PER_MTOK_IN", "0.15"))
PRICE_OUT = float(os.environ.get("PRICE_PER_MTOK_OUT", "0.60"))

def track(step, usage):
    log = st.session_state.setdefault("agent_log", [])
    cost = (usage.prompt_tokens * PRICE_IN + usage.completion_tokens * PRICE_OUT) / 1e6
    log.append({"step": step, "in_tok": usage.prompt_tokens,
                "out_tok": usage.completion_tokens, "cost_usd": round(cost, 6)})

def llm(step, messages, temperature=0.0):
    r = client.chat.completions.create(model=MODEL, temperature=temperature, messages=messages)
    if r.usage: track(step, r.usage)
    return r.choices[0].message.content

def gen_sql(question: str) -> str:
    sql = llm("generate_sql", [{"role": "system", "content": SQL_PROMPT},
                               {"role": "user", "content": question}]).strip()
    return re.sub(r"^```(sql)?|```$", "", sql, flags=re.M).strip().rstrip(";")

def guard(sql: str) -> str:
    low = sql.lower()
    if not (low.startswith("select") or low.startswith("with")):
        raise ValueError("Only SELECT queries are allowed")
    for bad in ["insert", "update", "delete", "drop", "alter", "create",
                "merge", "call", "grant", ";"]:
        if bad in low:
            raise ValueError(f"Blocked keyword: {bad}")
    if "limit" not in low:
        sql += " LIMIT 200"
    return sql

def run_sql(sql: str):
    t0 = time.perf_counter()
    if BACKEND == "clickhouse":
        import clickhouse_connect
        ch = clickhouse_connect.get_client(
            host=os.environ.get("CH_HOST", "clickhouse"), port=8123,
            username="default", password=os.environ.get("CH_PASSWORD", ""))
        res = ch.query(sql)
        df = pd.DataFrame(res.result_rows, columns=res.column_names)
    else:
        import trino
        conn = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "trino"),
                                   port=8080, user="copilot", catalog="iceberg",
                                   schema="telemetry")
        cur = conn.cursor(); cur.execute(sql)
        df = pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])
    return df, (time.perf_counter() - t0) * 1000

def analyze(question, sql, df, playbook_ctx, pb_source):
    return llm("analyst_verdict", temperature=0.2, messages=[
        {"role": "system", "content":
         "You are a senior SOC/infrastructure incident analyst. You MUST ground your "
         "recommended actions in the organization's playbooks provided below — cite the "
         "playbook id (e.g. PB-001) and follow ITS steps, not generic advice. Give: "
         "1) direct answer, 2) notable findings, 3) recommended next action per the playbook. "
         "Be concise. Empty rows: say so plainly.\n\n=== ORG PLAYBOOKS (ground truth via "
         f"{pb_source}) ===\n{playbook_ctx}"},
        {"role": "user", "content":
         f"Question: {question}\nSQL executed: {sql}\n"
         f"Rows (CSV, truncated):\n{df.head(50).to_csv(index=False)}"}])

# ---------------- UI ----------------
st.set_page_config(page_title="IR Copilot", layout="wide")
st.title("Incident Response Copilot")
st.caption(f"Backend: **{BACKEND}** · Model: **{MODEL}** · ground truth: Senso · "
           "control plane pattern: draft → human approval → execute")

with st.sidebar:
    st.subheader("Agent control plane")
    log = st.session_state.get("agent_log", [])
    if log:
        ldf = pd.DataFrame(log)
        st.dataframe(ldf, use_container_width=True, hide_index=True)
        st.metric("Total cost (USD)", f"${ldf.cost_usd.sum():.5f}")
        st.metric("Total tokens", int(ldf.in_tok.sum() + ldf.out_tok.sum()))
    else:
        st.caption("Token/cost telemetry appears here per step.")

examples = [
    "Show the latest 20 alerts with their rule and severity",
    "Which hosts triggered cryptojack_suspect alerts in the last 15 minutes?",
    "Reconstruct a minute-by-minute timeline of gpu_util, sm_active, power_w and temp_c for host-0007 over the last 30 minutes",
    "Show hosts with any ECC or XID errors today — what should I do about them?",
    "Which rack draws the most average power right now?",
]
q = st.selectbox("Examples", ["(type your own below)"] + examples)
question = st.text_input("Question", value="" if q.startswith("(") else q)

if st.button("Draft investigation", type="primary") and question:
    try:
        with st.spinner("Retrieving org playbooks (ground truth)..."):
            ctx, src = get_playbook_context(question)
        with st.spinner("Drafting SQL..."):
            sql = guard(gen_sql(question))
        st.session_state.update(pending={"q": question, "sql": sql, "ctx": ctx, "src": src})
    except Exception as e:
        st.error(str(e))

p = st.session_state.get("pending")
if p:
    st.markdown("#### Proposed query — review before it touches the data")
    st.caption(f"playbook context source: **{p['src']}**")
    sql_edited = st.text_area("SQL (editable)", p["sql"], height=110)
    c1, c2 = st.columns([1, 1])
    approved = c1.button("✅ Approve & run")
    if c2.button("✕ Reject"):
        st.session_state.pop("pending"); st.rerun()
    if approved:
        try:
            sql = guard(sql_edited.strip().rstrip(";"))
            with st.spinner("Querying..."):
                df, ms = run_sql(sql)
            st.caption(f"query latency: **{ms:.0f} ms** · rows: {len(df)}")
            st.dataframe(df, use_container_width=True)
            num = df.select_dtypes("number").columns
            tcol = next((c for c in df.columns if "time" in c.lower() or c in ("m", "minute")), None)
            if tcol is not None and len(num):
                try: st.line_chart(df.set_index(tcol)[num[0]])
                except Exception: pass
            with st.spinner("Analyst verdict (playbook-grounded)..."):
                st.markdown("### Analyst verdict")
                st.markdown(analyze(p["q"], sql, df, p["ctx"], p["src"]))
            st.session_state.pop("pending")
        except Exception as e:
            st.error(str(e))
