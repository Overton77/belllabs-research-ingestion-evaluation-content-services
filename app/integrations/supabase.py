from supabase import AsyncClient, acreate_client

from app.config import Settings


async def create_supabase(settings: Settings, *, privileged: bool = False) -> AsyncClient:
    """Create an async Supabase client; default to the publishable key for least privilege."""
    key = settings.supabase_secret_key if privileged else settings.supabase_publishable_key
    return await acreate_client(settings.supabase_url, key.get_secret_value())
