"""LangChain chat model shared by standard agents."""

import os

from ocs_ci.agents.mcp.registry import _load_auth_config


def get_chat_model():
    """
    Return the chat model used by agent graphs.

    The default provider is OpenAI. The API key is
    agents_credentials.openai.api_key in data/auth.yaml, with OPENAI_API_KEY
    as a fallback. OCS_AGENT_PROVIDER=claude selects the Claude CLI.
    OCS_AGENT_MODEL overrides the model name. The OpenAI default is gpt-4o.

    Returns:
        object: A LangChain chat model with tool binding support.

    Raises:
        NotImplementedError: The selected provider's package is not installed.
        ValueError: OpenAI was selected and no API key is configured.
    """
    provider = os.environ.get("OCS_AGENT_PROVIDER", "").strip().lower()
    if provider == "claude":
        return _claude_model()
    return _openai_model()


def _openai_model():
    """
    Return a ChatOpenAI model.

    Returns:
        ChatOpenAI: Model named by OCS_AGENT_MODEL, or gpt-4o.

    Raises:
        NotImplementedError: langchain-openai is not installed.
        ValueError: No OpenAI API key is configured.
    """
    try:
        from langchain_openai import ChatOpenAI
    except ImportError as error:
        raise NotImplementedError(
            "Chat model loading requires langchain-openai in the agents extra"
        ) from error
    api_key = _openai_api_key()
    if not api_key:
        raise ValueError(
            "Set agents_credentials.openai.api_key in data/auth.yaml "
            "or OPENAI_API_KEY. OCS_AGENT_MODEL overrides the default model gpt-4o."
        )
    model_name = os.environ.get("OCS_AGENT_MODEL") or "gpt-4o"
    return ChatOpenAI(model=model_name, api_key=api_key, temperature=0)


def _openai_api_key():
    """
    Return the OpenAI API key for agent runs.

    agents_credentials.openai.api_key in data/auth.yaml is preferred so a
    shell OPENAI_API_KEY does not replace the key checked in for the agent.

    Returns:
        str: API key, or None when neither the file nor the environment has one.
    """
    auth = _load_auth_config()
    credentials = auth.get("agents_credentials") or {}
    openai = credentials.get("openai") if isinstance(credentials, dict) else None
    if isinstance(openai, dict):
        key = openai.get("api_key")
        if isinstance(key, str) and key.strip():
            return key.strip()
    env_key = os.environ.get("OPENAI_API_KEY")
    if isinstance(env_key, str) and env_key.strip():
        return env_key.strip()
    return None


def _claude_model():
    """
    Return the Claude CLI chat model.

    Returns:
        ClaudeCliChat: Print-mode Claude model.
    """
    from ocs_ci.agents.runtime.claude_cli import ClaudeCliChat

    model_name = os.environ.get("OCS_AGENT_MODEL") or ""
    if model_name.startswith("gpt-"):
        model_name = ""
    return ClaudeCliChat(model_name=model_name)
