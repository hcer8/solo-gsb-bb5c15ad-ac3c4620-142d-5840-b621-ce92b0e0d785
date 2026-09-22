"""API 请求 / 响应模型。"""
from typing import Annotated

from pydantic import BaseModel, Field, field_validator, model_validator

BATCH_MIN_SIZE = 2
BATCH_MAX_SIZE = 32

TaskKey = Annotated[str, Field(min_length=1, max_length=512)]

# 幂等键：仅允许 URL/Header 安全字符，便于客户端通过日志 / 链路追踪传递
REQUEST_ID_PATTERN = r"^[A-Za-z0-9._:-]+$"
RequestId = Annotated[
    str,
    Field(
        min_length=1,
        max_length=128,
        pattern=REQUEST_ID_PATTERN,
        examples=["req-7f3a2c91"],
    ),
]


class AcquireRequest(BaseModel):
    task_key: str = Field(
        min_length=1, max_length=512, examples=["video-transcode:order-123"]
    )
    ttl_ms: int | None = Field(
        default=None, gt=0, description="租约 TTL（毫秒），缺省使用服务端默认值"
    )


class AcquireResponse(BaseModel):
    acquired: bool
    task_key: str
    token: str | None = Field(default=None, description="所有权令牌，仅成功时返回")
    fencing_token: int | None = Field(
        default=None, description="防栅序号，单调递增"
    )
    expires_at_ms: int | None = Field(
        default=None, description="过期时间（Redis 服务器时钟，Unix 毫秒）"
    )


class AcquireBatchRequest(BaseModel):
    task_keys: list[TaskKey] = Field(
        min_length=BATCH_MIN_SIZE,
        max_length=BATCH_MAX_SIZE,
        examples=[["video-transcode:order-123", "video-transcode:order-456"]],
        description=(
            f"一批任务键，{BATCH_MIN_SIZE}～{BATCH_MAX_SIZE} 个且不允许重复"
        ),
    )
    ttl_ms: int | None = Field(
        default=None, gt=0, description="统一租约 TTL（毫秒），缺省使用服务端默认值"
    )

    @field_validator("task_keys")
    @classmethod
    def reject_duplicates(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("task_keys must be unique")
        return v


class AcquiredLease(BaseModel):
    """批量申请结果中的单个任务凭据，顺序与请求的 task_keys 一致。"""

    task_key: str
    token: str = Field(description="所有权令牌")
    fencing_token: int = Field(description="防栅序号，单调递增")
    expires_at_ms: int = Field(
        description="过期时间（Redis 服务器时钟，Unix 毫秒），整批同一基准"
    )


class AcquireBatchResponse(BaseModel):
    acquired: bool
    leases: list[AcquiredLease] = Field(
        default_factory=list,
        description="全部成功时每个任务的独立凭据，按输入顺序返回",
    )
    conflicting_task_keys: list[str] = Field(
        default_factory=list,
        description="冲突时仍被占用的任务键（全部冲突键，按输入顺序）",
    )


class RenewRequest(BaseModel):
    task_key: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1)
    fencing_token: int = Field(ge=0)
    ttl_ms: int | None = Field(
        default=None, gt=0, description="新的 TTL（毫秒），缺省使用服务端默认值"
    )


class RenewResponse(BaseModel):
    renewed: bool
    expires_at_ms: int | None = None
    reason: str | None = Field(
        default=None, description="失败原因：not_found / conflict / expired"
    )


class ReleaseRequest(BaseModel):
    task_key: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1)
    fencing_token: int = Field(ge=0)


class ReleaseResponse(BaseModel):
    released: bool = Field(description="是否真正删除了租约；不匹配时为 false（幂等）")


class ValidateRequest(BaseModel):
    task_key: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1)
    fencing_token: int = Field(ge=0)


class ValidateResponse(BaseModel):
    valid: bool
    remaining_ms: int = Field(default=0, description="租约剩余毫秒数，无效时为 0")


class LeaseStateResponse(BaseModel):
    """租约状态查询（不暴露所有权令牌）。"""

    held: bool
    fencing_token: int | None = None
    expires_at_ms: int | None = None
    remaining_ms: int = 0


class LimitCheckRequest(BaseModel):
    client_key: str = Field(
        min_length=1,
        max_length=512,
        examples=["tenant-42:send-sms"],
        description="限流对象标识（租户 / 用户 / API 等任意维度）",
    )
    request_id: RequestId | None = Field(
        default=None,
        description=(
            "可选幂等标识。同一 client_key + 同一 request_id 在其首次请求的"
            "窗口内只扣减一次：后续携带该标识的并发 / 重试请求直接返回首次"
            "判定结果（200 或 429）。窗口结束后标识自动失效，可再次使用。"
            "不传时行为与旧版本完全一致（每次请求正常判定扣减）。"
        ),
    )
    limit: int = Field(
        ge=1,
        le=1_000_000_000,
        examples=[10],
        description="单个窗口内允许累计的成本上限",
    )
    window_ms: int = Field(
        ge=1,
        examples=[1000],
        description="滑动窗口长度（毫秒）",
    )
    cost: int = Field(
        default=1,
        ge=1,
        le=1_000_000_000,
        examples=[1],
        description="本次请求消耗的成本；不得大于 limit",
    )

    @model_validator(mode="after")
    def cost_within_limit(self) -> "LimitCheckRequest":
        if self.cost > self.limit:
            raise ValueError("cost must not exceed limit")
        return self


class LimitCheckResponse(BaseModel):
    allowed: bool = Field(description="是否放行（false 时 HTTP 状态码为 429）")
    client_key: str
    request_id: str | None = Field(
        default=None,
        description="回显请求携带的幂等标识；未携带时为 null",
    )
    idempotent_replay: bool = Field(
        default=False,
        description=(
            "true 表示本次响应直接取自同一 request_id 的首次判定结果"
            "（未重复扣减）；响应头同时给出 Idempotent-Replay: true"
        ),
    )
    limit: int = Field(description="窗口内允许的成本上限")
    window_ms: int = Field(description="本次判定使用的滑动窗口长度（毫秒）")
    remaining: int = Field(
        ge=0,
        description="放行后窗口内剩余可用成本；超限时为不扣减情况下的剩余值",
    )
    reset_at_ms: int = Field(
        description=(
            "窗口内最老一笔成本滑出、可再获得配额的时刻"
            "（Redis 服务器时钟，Unix 毫秒）"
        )
    )
    retry_after_ms: int = Field(
        default=0,
        ge=0,
        description="仅超限时返回：建议等待毫秒数（至最老一笔成本滑出窗口）",
    )
