# Lease & Rate Limit API — 分布式独占任务租约与滑动窗口限流服务

供多个工作进程协调独占任务的租约 API。客户端按任务键（`task_key`）申请带 TTL 的
租约；仅当任务未被占用或旧租约已过期时申请成功，并返回**所有权令牌**与**单调递增
的防栅序号（fencing token）**。续租与释放必须同时校验令牌与防栅序号，旧持有者在
超时后无法影响新租约；另提供校验接口，供下游判断一次操作是否仍属于当前租约。

另提供 **Redis Lua 原子滑动窗口（成本）限流 API**：`POST /v1/limits/check`
按 Redis 服务器时间累计滑动窗口内的请求成本，未超限则原子扣减并返回剩余额度，
超限返回 `429` 与建议重试时间；同一 `client_key` 的并发请求不会超额，过期窗口
数据自动清理。请求还可携带可选 `request_id` 实现**窗口内请求幂等**：相同
`client_key` + `request_id` 的并发或重试请求直接返回首次判定结果且不重复扣减，
`request_id` 仅在对应窗口内存活，跨窗口可再次使用。

技术栈：Python 3.12 · FastAPI · Redis 7（Lua 脚本保证原子性）

## 快速开始（一条命令）

```bash
docker compose up --build
```

启动后：

| 组件 | 地址 |
| --- | --- |
| API | http://localhost:8000 |
| 交互式文档 (Swagger UI) | http://localhost:8000/docs |
| 健康检查 | http://localhost:8000/healthz |
| Redis | localhost:6379 |

Compose 会同时拉起 Redis 与 API 两个容器，API 等待 Redis 健康检查通过后启动。

## 配置

全部通过环境变量配置（见 `docker-compose.yml` 中的示例）：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis 连接串 |
| `LEASE_KEY_PREFIX` | `lease` | Redis 键前缀，便于多环境共用实例 |
| `LEASE_MIN_TTL_MS` | `100` | 允许的最小 TTL（毫秒） |
| `LEASE_MAX_TTL_MS` | `300000` | 允许的最大 TTL（毫秒） |
| `LEASE_DEFAULT_TTL_MS` | `10000` | 请求未指定 `ttl_ms` 时的默认值 |
| `LIMIT_KEY_PREFIX` | `ratelimit` | 限流 Redis 键前缀，与租约键隔离 |
| `LIMIT_IDEM_KEY_PREFIX` | `ratelimit-idem` | 限流幂等记录 Redis 键前缀，与限流 ZSet 隔离 |
| `LIMITS_MIN_WINDOW_MS` | `1` | 允许的最小限流窗口（毫秒） |
| `LIMITS_MAX_WINDOW_MS` | `86400000` | 允许的最大限流窗口（毫秒，默认 1 天） |

服务端口固定为 **8000**（容器内），可在 `docker-compose.yml` 的 `ports` 中调整宿主机映射。

## API

所有端点位于 `/v1` 前缀下，请求与响应均为 JSON。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/leases/acquire` | 申请租约，成功 200 / 被占用 409 |
| POST | `/v1/leases/acquire-batch` | 批量申请 2～32 个不重复任务键（统一 TTL），全成全败 |
| POST | `/v1/leases/renew` | 续租，成功 200 / 失败 409（reason: `not_found` / `conflict` / `expired`） |
| POST | `/v1/leases/release` | 释放租约（幂等，始终 200） |
| POST | `/v1/leases/validate` | 校验 (令牌, 防栅序号) 是否仍属当前租约 |
| GET | `/v1/leases/{task_key}` | 查看任务键租约状态（不暴露令牌） |
| POST | `/v1/limits/check` | 滑动窗口成本限流检查（通过即扣减，可选 `request_id` 幂等），放行 200 / 超限 429 |
| GET | `/healthz` | 健康检查 |

### 示例

```bash
# 1. 申请租约（TTL 5 秒）
curl -s -X POST localhost:8000/v1/leases/acquire \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "ttl_ms": 5000}'
# => {"acquired":true,"task_key":"report:daily","token":"9f3c...","fencing_token":1,"expires_at_ms":1758...}

# 占用期间他人申请 => 409 {"acquired":false,...}

# 2. 批量申请（2～32 个不重复 task_key，统一 TTL，全成全败）
curl -s -X POST localhost:8000/v1/leases/acquire-batch \
  -H 'Content-Type: application/json' \
  -d '{"task_keys": ["report:daily", "report:weekly", "report:monthly"], "ttl_ms": 5000}'
# 成功 200（leases 顺序与请求一致；expires_at_ms 为同一次服务器 TIME 的基准）：
# => {"acquired":true,"leases":[
#   {"task_key":"report:daily","token":"a1b2...","fencing_token":1,"expires_at_ms":1758...},
#   {"task_key":"report:weekly","token":"c3d4...","fencing_token":1,"expires_at_ms":1758...},
#   {"task_key":"report:monthly","token":"e5f6...","fencing_token":1,"expires_at_ms":1758...}
# ],"conflicting_task_keys":[]}
#
# 任一任务仍被占用 => 409，返回全部冲突键，不创建任何租约、不递增任何防栅计数器：
# => {"acquired":false,"leases":[],"conflicting_task_keys":["report:weekly"]}
#
# 过期租约与不存在的键同样视为可用，可被批量接管。
# 批量所得 (token, fencing_token) 与单个申请完全等价，
# 可直接用于下面的续租 / 校验 / 释放接口（每个任务键独立调用）。

# 3. 续租（必须同时带上令牌与防栅序号）
curl -s -X POST localhost:8000/v1/leases/renew \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "9f3c...", "fencing_token": 1, "ttl_ms": 5000}'

# 4. 下游校验某次操作是否仍属当前租约
curl -s -X POST localhost:8000/v1/leases/validate \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "9f3c...", "fencing_token": 1}'
# => {"valid":true,"remaining_ms":4210}

# 5. 释放租约
curl -s -X POST localhost:8000/v1/leases/release \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "9f3c...", "fencing_token": 1}'
```

### 限流示例

```bash
# 窗口 1 秒、成本上限 3、本次成本 1：放行并返回剩余额度
curl -s -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:send-sms", "limit": 3, "window_ms": 1000, "cost": 1}'
# => 200
# {"allowed":true,"client_key":"tenant-42:send-sms","limit":3,"window_ms":1000,
#  "remaining":2,"reset_at_ms":1758...,"retry_after_ms":0}

# 连续打满窗口后再次请求 => 429（不扣减任何成本）
curl -i -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:send-sms", "limit": 3, "window_ms": 1000, "cost": 1}'
# => HTTP/1.1 429 Too Many Requests
#    Retry-After: 1
# {"allowed":false,"client_key":"tenant-42:send-sms","limit":3,"window_ms":1000,
#  "remaining":0,"reset_at_ms":1758...,"retry_after_ms":812}

# cost 可表达加权成本（如批量条数、令牌数）；cost 不得大于 limit，否则 422
curl -s -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:bulk", "limit": 100, "window_ms": 60000, "cost": 30}'
# => 200 {"allowed":true,...,"remaining":70,...}

# request_id 幂等：相同 client_key + request_id 在窗口内只扣减一次。
# 首次请求正常判定（假设通过）：
curl -s -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:send-sms", "request_id": "req-7f3a2c91",
       "limit": 3, "window_ms": 1000, "cost": 1}'
# => 200
# {"allowed":true,...,"request_id":"req-7f3a2c91","idempotent_replay":false,
#  "remaining":2,...}

# 网络超时 / 客户端重试，携带相同 request_id 再发一次：
# 直接回放首次结果，不再扣减（即使窗口额度已被其他请求占满）：
curl -i -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:send-sms", "request_id": "req-7f3a2c91",
       "limit": 3, "window_ms": 1000, "cost": 1}'
# => HTTP/1.1 200 OK
#    Idempotent-Replay: true
# {"allowed":true,...,"request_id":"req-7f3a2c91","idempotent_replay":true,
#  "remaining":2,...}            # 与首次完全一致，窗口内实际只扣了一笔

# 首次判定为 429 时同样会记录：重试得到相同的 429 快照（同 reset_at_ms /
# retry_after_ms），不会因为时间推移或参数变化重新判定；
# 待窗口结束、request_id 自动失效后，同一 request_id 可再次用于新请求。
```

字段说明：

| 字段 | 方向 | 说明 |
| --- | --- | --- |
| `client_key` | 请求 | 限流对象标识（1～512 字符），任意维度（租户 / 用户 / API） |
| `request_id` | 请求 | 可选幂等标识（1～128 字符，仅允许字母、数字、`._:-`）。同一 `client_key` + `request_id` 在首次请求的窗口内只扣减一次，重试 / 并发请求回放首次结果；窗口结束后自动失效、可再次使用。不传时行为与旧版本一致 |
| `limit` | 请求 | 单个窗口内允许累计的成本上限（≥1） |
| `window_ms` | 请求 | 滑动窗口长度（毫秒，服务端限定 1～86400000），同时决定幂等记录的存活时长 |
| `cost` | 请求 | 本次成本（≥1，默认 1，且不得大于 `limit`） |
| `request_id` | 响应 | 原样回显请求携带的幂等标识；未携带时为 `null` |
| `idempotent_replay` | 响应 | `true` 表示本次为幂等回放（未重复扣减），响应头同时带 `Idempotent-Replay: true`；默认 `false` |
| `remaining` | 响应 | 放行后窗口内剩余成本；超限时为未扣减情况下的剩余值；回放时为首次结果快照 |
| `reset_at_ms` | 响应 | 窗口内最老一笔成本滑出、可再获配额的时刻（Redis 服务器时钟，Unix 毫秒） |
| `retry_after_ms` | 响应 | 仅 429 时非 0：建议等待毫秒数；响应头同时给出标准 `Retry-After`（秒，向上取整） |

## 设计要点

- **原子性**：申请 / 批量申请 / 续租 / 释放 / 校验全部状态转换均由 Redis Lua 脚本
  （`app/lua.py`）在服务端原子执行，不存在"检查后修改"的竞态窗口。批量申请在同一
  脚本内先检查全部任务、再统一写入，任一任务被占用则整体失败——不创建任何租约，
  也不递增任何防栅计数器（全成全败）；过期租约可被接管。
- **过期判断使用 Redis 服务器时间**：Lua 内通过 `TIME` 命令取 Redis 服务器时钟，
  不依赖各客户端本地时间，避免时钟漂移导致的误判。批量脚本只调用一次 `TIME`，
  整批租约的 `expires_at_ms` 基于同一时刻计算。
- **防栅序号单调递增**：每个任务键对应一个独立的 `INCR` 计数器键且**永不过期**，
  即使租约键被 TTL 清除，序号也不会回退或复用。下游系统可用它拒绝迟到的旧操作
  （fencing token 经典用法）。
- **旧持有者隔离**：续租与释放要求令牌和防栅序号**同时**匹配；租约过期后不允许
  续租（必须重新申请）。因此旧持有者在超时后的任何写操作都不会影响新租约。
- **双重过期保障**：租约 Hash 内记录 `expires_at`（判断依据），同时设置
  `PEXPIRE`（自动清理残留键）；判断以 `expires_at` 与服务器时间的比较为准。

### 限流设计要点

- **原子滑动窗口**：每次检查由单个 Lua 脚本依次完成
  `ZREMRANGEBYSCORE` 清旧 → 遍历 member 累计成本 → 判定 → `ZADD` 扣减，
  Redis 单线程执行脚本，同一 `client_key` 的并发请求严格串行判定，
  不会出现"检查后扣减"的竞态，**并发请求绝不超额**（超限时不写入任何成本）。
- **成本加权**：每笔请求携带整数 `cost`（批量条数、令牌数等），
  member 形如 `<uuid>:<cost>`，cost 编码在 member 尾部由脚本解析累计；
  uuid 保证同一毫秒并发请求的 member 不冲突。
- **Redis 服务器时间**：窗口边界由 Lua 内 `TIME` 命令的服务器时钟确定，
  不依赖调用方本地时间；`reset_at_ms` / `retry_after_ms` 均基于该时钟计算。
- **过期数据自动清理**：滑出窗口的记录在下次检查时被 `ZREMRANGEBYSCORE`
  即时删除，键同时设置 `PEXPIRE window_ms`，窗口结束后无流量时键也会自动消失，
  不残留空窗口数据。
- **超限语义**：超限时返回 `429`，响应体给出 `remaining`、`reset_at_ms`、
  `retry_after_ms`，并携带标准 `Retry-After` 头（秒，向上取整）。
- **request_id 幂等**：携带 `request_id` 时，"幂等命中判定 → 清旧 → 累计 →
  判定 → 扣减 → 记录完整响应"在**同一个 Lua 脚本**内完成，因此相同
  `client_key` + `request_id` 的并发请求也只有首个完成判定与扣减，其余直接
  回放首次结果（200 或 429 均会记录），回放响应带 `idempotent_replay=true`
  与 `Idempotent-Replay: true` 响应头，且**不刷新任何 TTL**。幂等记录的 TTL
  等于首次请求的 `window_ms`，窗口结束后自动删除——`request_id` 仅在对应窗口
  内存活，跨窗口可再次使用。重试请求以首次结果为准，不重新判定额度或参数
  （`limit` / `window_ms` 也回放首次值），发起新的逻辑请求请使用新的
  `request_id`。幂等键以 `client_key` 长度定界，不同 `client_key` 间不可能
  碰撞；`request_id` 仅允许字母、数字与 `._:-`，不携带 `request_id` 的请求
  行为与旧版本完全一致。

### Redis 数据模型

| 键 | 类型 | 说明 |
| --- | --- | --- |
| `{prefix}:lease:{task_key}` | Hash（`token` / `fencing` / `expires_at`），带 PEXPIRE | 当前租约 |
| `{prefix}:fencing:{task_key}` | String 计数器，无 TTL | 防栅序号，只增不减 |
| `{limit_prefix}:{client_key}` | ZSet，score=请求毫秒时间戳，member=`<uuid>:<cost>`，带 PEXPIRE | 滑动窗口内各笔成本 |
| `{idem_prefix}:<len>:{client_key}:{request_id}` | Hash（`allowed` / `remaining` / `reset_at` / `retry_after` / `limit` / `window`），PEXPIRE=首次 `window_ms` | request_id 首次判定的完整响应；窗口内回放、窗口结束自动删除 |

## 冒烟测试

服务启动后执行（仅依赖 Python 标准库）：

```bash
python3 scripts/smoke_test.py            # 默认打 http://localhost:8000
```

覆盖：获取冲突、错误令牌/序号被拒绝、防栅序号单调递增、旧持有者超时后无法
续租/释放/干扰新租约等场景；以及限流的剩余额度递减、成本加权、超限 429
（含 `Retry-After` 头与不扣减语义）、窗口滑动后恢复、**20 并发恰好放行
limit 笔不超额**、参数校验和过期数据自动清理；以及 `request_id` 幂等：
重试回放首次结果且不重复扣减、回放响应带 `Idempotent-Replay` 头、
**20 个携带相同 request_id 的并发请求只扣减一次**、429 结果原样回放、
`request_id` 跨窗口可再次使用、幂等作用域按 `client_key` 隔离、
非法 `request_id` 返回 422。

## 本地开发（不用 Docker）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# 需要本地或远程有可用的 Redis，并通过 REDIS_URL 指向它
uvicorn app.main:app --reload --port 8000
```

## 项目结构

```
app/
  config.py    # 环境变量配置（pydantic-settings）
  lua.py       # 申请/批量申请/续租/释放/校验五个租约 Lua 脚本 + 滑动窗口限流（含 request_id 幂等）Lua 脚本
  schemas.py   # 请求 / 响应模型
  main.py      # FastAPI 应用、路由、Redis 生命周期
scripts/
  smoke_test.py
Dockerfile
docker-compose.yml
requirements.txt
```
