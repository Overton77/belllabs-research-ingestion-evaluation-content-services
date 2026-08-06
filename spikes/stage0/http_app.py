"""Non-colliding custom route used only by the Stage 0 Agent Server spike."""

from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException

from local_identity import authenticate_local

app = FastAPI()


async def require_local_scope(
    stage0_token: Annotated[str | None, Header(alias="X-Stage0-Token")] = None,
    request_scope: Annotated[str | None, Header(alias="X-Request-Scope")] = None,
) -> str:
    """Close the observed custom-route auth gap in local Agent Server 0.12.0."""

    identity = authenticate_local(
        token=stage0_token or "",
        claimed_request_scope=request_scope or "",
    )
    if identity is None:
        raise HTTPException(status_code=401, detail="invalid local spike identity")
    return identity.request_scope


@app.get("/stage0/qualification")
async def qualification_probe(
    request_scope: Annotated[str, Depends(require_local_scope)],
) -> dict[str, str]:
    return {"status": "local-spike-only", "request_scope": request_scope}
