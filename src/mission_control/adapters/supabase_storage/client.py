from supabase import AsyncClient, acreate_client

from mission_control.bootstrap.settings import IntegrationConfigurationError, Settings


async def create_supabase(settings: Settings, *, privileged: bool = False) -> AsyncClient:
    """Create an async Supabase client; default to the publishable key for least privilege."""
    key = settings.supabase_secret_key if privileged else settings.supabase_publishable_key
    if (
        not settings.supabase_url
        or not settings.supabase_url.strip()
        or key is None
        or not key.get_secret_value().strip()
    ):
        key_name = "SUPABASE_SECRET_KEY" if privileged else "SUPABASE_PUBLISHABLE_KEY"
        raise IntegrationConfigurationError(
            f"SUPABASE_URL and {key_name} are required for Supabase"
        )
    return await acreate_client(settings.supabase_url, key.get_secret_value())
