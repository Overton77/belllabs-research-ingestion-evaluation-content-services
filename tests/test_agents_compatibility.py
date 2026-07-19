from agents import RunContextWrapper


def test_agents_usage_context_constructs() -> None:
    """Catch OpenAI client usage-schema changes before a Temporal workflow can retry forever."""
    wrapper = RunContextWrapper(context=None)
    assert wrapper.usage.total_tokens == 0
