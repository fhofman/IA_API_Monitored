"""Los agentes construyen ChatOpenAI al importarse y eso exige una API key,
aunque los tests nunca llaman al LLM. Un valor ficticio alcanza."""

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy-no-network")
os.environ.setdefault("PHOENIX_TRACING", "false")
