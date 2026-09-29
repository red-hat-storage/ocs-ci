"""LangChain chat model shared by standard agents."""


def get_chat_model():
    """
    Return the chat model used by agent graphs.

    Returns:
        object: A LangChain chat model with tool binding support.

    Raises:
        NotImplementedError: The LangChain model package is not connected yet.
    """
    raise NotImplementedError(
        "Chat model loading requires langchain in the agents extra"
    )
