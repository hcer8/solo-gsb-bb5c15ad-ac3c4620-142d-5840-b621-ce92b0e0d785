"""租约协调服务：多工作进程间的独占任务协调。

所有租约状态转换均由 Redis Lua 脚本原子完成（见 app/lua.py），
过期判断基于 Redis 服务器时间（TIME 命令）。
"""
from __future__ import annotations

import math
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .config import Settings, get_settings
from .lua import (
    ACQUIRE_BATCH_LUA,
    ACQUIRE_LUA,
    LIMIT_CHECK_LUA,
    RELEASE_LUA,
    RENEW_LUA,
    VALIDATE_LUA,
)
from .schemas import (
    AcquireBatchRequest,
    AcquireBatchResponse,
    AcquiredLease,
    AcquireRequest,
    AcquireResponse,
    LeaseStateResponse,
    LimitCheckRequest,
    LimitCheckResponse,
    ReleaseRequest,
    ReleaseResponse,
    RenewRequest,
    RenewResponse,
    ValidateRequest,
    ValidateResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.redis = redis
    # 启动时注册 Lua 脚本（EVALSHA 执行，脚本不存在时客户端自动回退 EVAL）
    app.state.scripts = SimpleNamespace(
        acquire=redis.register_script(ACQUIRE_LUA),
        acquire_batch=redis.register_script(ACQUIRE_BATCH_LUA),
        renew=redis.register_script(RENEW_LUA),
        release=redis.register_script(RELEASE_LUA),
        validate=redis.register_script(VALIDATE_LUA),
        limit_check=redis.register_script(LIMIT_CHECK_LUA),
    )
    yield
    await redis.aclose()


app = FastAPI(
    title="Lease & Rate Limit API",
    version="1.1.0",
    description=(
        "供多个工作进程协调独占任务的租约服务，以及基于 Redis Lua 的"
        "原子滑动窗口成本限流（FastAPI + Redis Lua）"
    ),
    lifespan=lifespan,
)


@app.exception_handler(RedisError)
async def redis_error_handler(request: Request, exc: RedisError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "redis unavailable"},
    )


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def get_scripts(request: Request) -> SimpleNamespace:
    return request.app.state.scripts


RedisDep = Annotated[Redis, Depends(get_redis)]
ScriptsDep = Annotated[SimpleNamespace, Depends(get_scripts)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def lease_key(settings: Settings, task_key: str) -> str:
    return f"{settings.key_prefix}:lease:{task_key}"


def fencing_key(settings: Settings, task_key: str) -> str:
    return f"{settings.key_prefix}:fencing:{task_key}"


def resolve_ttl(ttl_ms: int | None, settings: Settings) -> int:
    ttl = ttl_ms if ttl_ms is not None else settings.default_ttl_ms
    if not settings.min_ttl_ms <= ttl <= settings.max_ttl_ms:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"ttl_ms must be within "
                f"[{settings.min_ttl_ms}, {settings.max_ttl_ms}]"
            ),
        )
    return ttl


def limit_key(settings: Settings, client_key: str) -> str:
    return f"{settings.limit_key_prefix}:{client_key}"


def idempotency_key(settings: Settings, client_key: str, request_id: str) -> str:
    # client_key 长度定界（4 位十六进制）后再拼接，保证不同 client_key
    # 即使内容包含分隔符也不可能映射到同一个幂等键
    length_tag = f"{len(client_key):04x}"
    return (
        f"{settings.limit_idem_key_prefix}:{length_tag}:{client_key}:{request_id}"
    )


def resolve_window(window_ms: int, settings: Settings) -> int:
    if not settings.limits_min_window_ms <= window_ms <= settings.limits_max_window_ms:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"window_ms must be within "
                f"[{settings.limits_min_window_ms}, {settings.limits_max_window_ms}]"
            ),
        )
    return window_ms


@app.get("/healthz")
async def healthz(redis: RedisDep) -> dict:
    await redis.ping()
    return {"status": "ok"}


@app.get("/")
async def root() -> dict:
    return {"service": "lease-rate-limit-api", "docs": "/docs"}


@app.post("/v1/leases/acquire", response_model=AcquireResponse)
async def acquire(
    req: AcquireRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> AcquireResponse:
    """申请租约。仅当任务未被占用或旧租约已过期时成功。

    成功返回 200 及所有权令牌 + 防栅序号；被占用返回 409。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    token = uuid.uuid4().hex
    ok, fencing, expires_at = await scripts.acquire(
        keys=[lease_key(settings, req.task_key), fencing_key(settings, req.task_key)],
        args=[token, ttl],
    )
    if ok:
        return AcquireResponse(
            acquired=True,
            task_key=req.task_key,
            token=token,
            fencing_token=fencing,
            expires_at_ms=expires_at,
        )
    response.status_code = status.HTTP_409_CONFLICT
    return AcquireResponse(
        acquired=False,
        task_key=req.task_key,
        fencing_token=fencing,
        expires_at_ms=expires_at,
    )


@app.post(
    "/v1/leases/acquire-batch",
    response_model=AcquireBatchResponse,
)
async def acquire_batch(
    req: AcquireBatchRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> AcquireBatchResponse:
    """批量申请租约（2～32 个不重复任务键，统一 TTL），全成全败。

    由单个 Lua 脚本、单次服务器 TIME 原子完成：任一任务仍被有效租约占用时
    返回 409 及全部冲突键，不创建任何租约、不递增任何防栅计数器；
    全部可用（含旧租约已过期可接管）时，按输入顺序返回每个任务独立的
    token、递增的 fencing_token 与同一基准计算的 expires_at_ms。
    所得凭据可直接用于现有的续租 / 释放 / 校验接口。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    tokens = [uuid.uuid4().hex for _ in req.task_keys]
    lease_keys = [lease_key(settings, k) for k in req.task_keys]
    fencer_keys = [fencing_key(settings, k) for k in req.task_keys]

    result = await scripts.acquire_batch(
        keys=lease_keys + fencer_keys,
        args=[str(len(req.task_keys)), *tokens, str(ttl)],
    )

    if result[0]:
        leases = [
            AcquiredLease(
                task_key=req.task_keys[i],
                token=tokens[i],
                fencing_token=result[1 + 2 * i],
                expires_at_ms=result[2 + 2 * i],
            )
            for i in range(len(req.task_keys))
        ]
        return AcquireBatchResponse(acquired=True, leases=leases)

    conflicting = [req.task_keys[idx - 1] for idx in result[1:]]
    response.status_code = status.HTTP_409_CONFLICT
    return AcquireBatchResponse(
        acquired=False, conflicting_task_keys=conflicting
    )


@app.post("/v1/leases/renew", response_model=RenewResponse)
async def renew(
    req: RenewRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> RenewResponse:
    """续租。必须同时匹配令牌与防栅序号，且租约尚未过期。

    失败返回 409，reason 取值：not_found / conflict / expired。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    ok, reason, expires_at = await scripts.renew(
        keys=[lease_key(settings, req.task_key)],
        args=[req.token, str(req.fencing_token), ttl],
    )
    if ok:
        return RenewResponse(renewed=True, expires_at_ms=expires_at)
    response.status_code = status.HTTP_409_CONFLICT
    return RenewResponse(renewed=False, reason=reason)


@app.post("/v1/leases/release", response_model=ReleaseResponse)
async def release(
    req: ReleaseRequest, scripts: ScriptsDep, settings: SettingsDep
) -> ReleaseResponse:
    """释放租约。令牌与防栅序号同时匹配才删除，否则幂等忽略。

    旧持有者在超时后调用本接口不会影响新租约。
    """
    released = await scripts.release(
        keys=[lease_key(settings, req.task_key)],
        args=[req.token, str(req.fencing_token)],
    )
    return ReleaseResponse(released=bool(released))


@app.post("/v1/leases/validate", response_model=ValidateResponse)
async def validate(
    req: ValidateRequest, scripts: ScriptsDep, settings: SettingsDep
) -> ValidateResponse:
    """校验 (令牌, 防栅序号) 是否仍对应当前有效租约，供下游判断操作归属。"""
    valid, remaining_ms = await scripts.validate(
        keys=[lease_key(settings, req.task_key)],
        args=[req.token, str(req.fencing_token)],
    )
    return ValidateResponse(valid=bool(valid), remaining_ms=remaining_ms)


@app.get("/v1/leases/{task_key:path}", response_model=LeaseStateResponse)
async def inspect(
    task_key: str, redis: RedisDep, settings: SettingsDep
) -> LeaseStateResponse:
    """查看任务键当前租约状态（只读，不暴露所有权令牌）。"""
    data = await redis.hgetall(lease_key(settings, task_key))
    if not data:
        return LeaseStateResponse(held=False)
    sec, usec = await redis.time()
    now = int(sec) * 1000 + int(usec) // 1000
    expires_at = int(data["expires_at"])
    held = expires_at > now
    return LeaseStateResponse(
        held=held,
        fencing_token=int(data["fencing"]),
        expires_at_ms=expires_at,
        remaining_ms=max(0, expires_at - now),
    )


@app.post(
    "/v1/limits/check",
    response_model=LimitCheckResponse,
    tags=["rate-limits"],
    summary="滑动窗口成本限流检查（通过即扣减，支持 request_id 幂等）",
    responses={
        status.HTTP_429_TOO_MANY_REQUESTS: {"model": LimitCheckResponse},
    },
)
async def check_limit(
    req: LimitCheckRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> LimitCheckResponse:
    """按 Redis 服务器时间统计滑动窗口内的累计成本并原子扣减。

    - 未超限（`used + cost <= limit`）：原子记入本次成本，返回 **200**、
      `remaining` 与 `reset_at_ms`（窗口内最老一笔成本滑出的时刻）。
    - 超限：不扣减，返回 **429** 与 `retry_after_ms`（等待最老一笔成本滑出后重试），
      同时携带标准 `Retry-After` 响应头（秒）。

    **可选幂等（request_id）**：请求体携带 `request_id` 时，同一
    `client_key` + `request_id` 在其首次请求的窗口内只判定 / 扣减一次，
    首次结果（无论 200 还是 429）完整记录在 Redis 中；随后携带相同
    `request_id` 的并发或重试请求直接回放首次结果，响应中
    `idempotent_replay=true` 并携带 `Idempotent-Replay: true` 响应头，
    **不会重复扣减、不刷新任何 TTL**。幂等记录的 TTL 等于首次请求的
    `window_ms`，窗口结束后 `request_id` 自动失效、可再次使用。
    重试请求以首次结果为准，不会重新判定参数与额度；需要发起新的逻辑
    请求时请使用新的 `request_id`。不传 `request_id` 时行为与旧版本一致。

    "幂等判定 → 清理旧记录 → 累计 → 判定 → 扣减 → 记录响应"由单个 Lua
    脚本基于 Redis 服务器时间原子执行，同一 `client_key` 的并发请求不会
    超额，相同 `request_id` 的并发请求只有首个完成扣减；窗口数据随滑动
    自动清理，键在窗口清空后按 `PEXPIRE` 自动删除。
    """
    window = resolve_window(req.window_ms, settings)
    keys = [limit_key(settings, req.client_key)]
    if req.request_id is not None:
        keys.append(idempotency_key(settings, req.client_key, req.request_id))
    result = await scripts.limit_check(
        keys=keys,
        args=[req.limit, window, req.cost, uuid.uuid4().hex],
    )

    kind = result[0]
    if kind == 2:
        # 幂等命中：回放首次请求记录的完整结果
        _, allowed_flag, remaining, reset_at_ms, retry_after_ms, stored_limit, stored_window = result
        allowed = bool(allowed_flag)
        replay = LimitCheckResponse(
            allowed=allowed,
            client_key=req.client_key,
            request_id=req.request_id,
            idempotent_replay=True,
            limit=stored_limit,
            window_ms=stored_window,
            remaining=remaining,
            reset_at_ms=reset_at_ms,
            retry_after_ms=retry_after_ms,
        )
        response.headers["Idempotent-Replay"] = "true"
        if not allowed:
            response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
            # 标准 Retry-After 以秒为单位，向上取整避免客户端过早重试
            response.headers["Retry-After"] = str(
                max(1, math.ceil(retry_after_ms / 1000))
            )
        return replay

    if kind == 1:
        return LimitCheckResponse(
            allowed=True,
            client_key=req.client_key,
            request_id=req.request_id,
            limit=req.limit,
            window_ms=window,
            remaining=result[1],
            reset_at_ms=result[2],
        )

    remaining, reset_at_ms, retry_after_ms = result[1], result[2], result[3]
    response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
    # 标准 Retry-After 以秒为单位，向上取整避免客户端过早重试
    response.headers["Retry-After"] = str(max(1, math.ceil(retry_after_ms / 1000)))
    return LimitCheckResponse(
        allowed=False,
        client_key=req.client_key,
        request_id=req.request_id,
        limit=req.limit,
        window_ms=window,
        remaining=remaining,
        reset_at_ms=reset_at_ms,
        retry_after_ms=retry_after_ms,
    )
