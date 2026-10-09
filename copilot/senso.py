"""Senso.ai ground-truth layer: retrieve the org's real playbook for an incident type.
Uses the Senso REST API when SENSO_API_KEY is set; falls back to local ./playbooks
markdown (same content you upload to Senso as ground truth) so the demo never breaks.
"""
import os, glob

SENSO_BASE = os.environ.get("SENSO_BASE_URL", "https://sdk.senso.ai/api/v1")
SENSO_KEY = os.environ.get("SENSO_API_KEY", "")
PLAYBOOK_DIR = os.environ.get("PLAYBOOK_DIR", "/app/playbooks")

def _local_context(question: str) -> str:
    docs = []
    for f in sorted(glob.glob(f"{PLAYBOOK_DIR}/*.md")):
        docs.append(open(f).read())
    return "\n\n---\n\n".join(docs) if docs else ""

def _senso_context(question: str) -> str:
    import requests
    r = requests.post(f"{SENSO_BASE}/search",
                      headers={"X-API-Key": SENSO_KEY, "Content-Type": "application/json"},
                      json={"query": question, "max_results": 3}, timeout=10)
    r.raise_for_status()
    hits = r.json().get("results", [])
    return "\n\n---\n\n".join(h.get("content", h.get("text", "")) for h in hits)

def get_playbook_context(question: str) -> tuple[str, str]:
    """Returns (context, source) where source is 'senso' or 'local'."""
    if SENSO_KEY:
        try:
            ctx = _senso_context(question)
            if ctx:
                return ctx, "senso"
        except Exception:
            pass
    return _local_context(question), "local"
