from app.config import get_settings


def test_preemptive_settings_contract() -> None:
    settings = get_settings()
    assert settings.mongodb_database == "belllabsbiotech"
    assert settings.openai_model == "gpt-5.4-nano"
    assert settings.temporal_task_queue
    assert settings.postgres_dsn
