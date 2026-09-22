"""租约状态机与滑动窗口限流的全部写/判操作都在 Redis Lua 中原子完成。

租约约定：
- 租约键（KEYS[1] 或 lease key）是一个 Hash：
    token       所有权令牌（UUID）
    fencing     防栅序号（单调递增）
    expires_at  过期时间，Unix 毫秒，基于 Redis 服务器时钟
- 防栅计数器键独立存放、永不过期，保证同一任务键的 fencing 只增不减。
- 过期判断一律使用 redis.call('TIME')，即 Redis 服务器时间，
  避免各客户端时钟不一致导致的问题。

限流约定：
- 限流键是一个 ZSet：score 为请求时刻（Redis 服务器毫秒），
  member 形如 "<uuid>:<cost>"，cost 编码在 member 尾部，
  uuid 保证同毫秒并发请求的 member 不冲突。
- 幂等键（可选 request_id）是一个 Hash，保存首次判定的完整响应，
  TTL 等于首次请求的 window_ms：窗口内同键请求直接回放首次结果、
  不重复扣减；窗口结束后 Hash 自动删除，request_id 可再次使用。
- 窗口判断、累计、扣减与幂等判定全部基于 redis.call('TIME') 的
  服务器时间，单脚本原子执行，并发请求不会超额、不会重复扣减。
"""

# 申请租约：仅当租约不存在或已过期时成功。
# KEYS[1] = 租约键, KEYS[2] = 防栅计数器键
# ARGV[1] = 新令牌, ARGV[2] = ttl_ms
# 返回 {是否成功(1/0), fencing, expires_at}
ACQUIRE_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ttl = tonumber(ARGV[2])

local expires_at = redis.call('HGET', KEYS[1], 'expires_at')
if expires_at and tonumber(expires_at) > now then
  return {0, tonumber(redis.call('HGET', KEYS[1], 'fencing')), tonumber(expires_at)}
end

local fencing = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1],
  'token', ARGV[1],
  'fencing', fencing,
  'expires_at', now + ttl)
redis.call('PEXPIRE', KEYS[1], ttl)
return {1, fencing, now + ttl}
"""

# 批量申请（2～32 个任务键，统一 TTL），全成全败：
# 先统一检查所有任务，任一仍被有效租约占用即整体失败，
# 不创建任何租约、不递增任何防栅计数器；
# 全部可用（不存在或已过期，过期可被接管）时再统一写入。
# 全程只调用一次 TIME，所有成功租约的 expires_at 基于同一基准时刻。
# KEYS[1..n]    = 租约键
# KEYS[n+1..2n] = 防栅计数器键（与租约键同序）
# ARGV[1]       = 任务数 n
# ARGV[2..n+1]  = 各任务的新令牌（与输入同序）
# ARGV[n+2]     = ttl_ms
# 成功返回 {1, fencing_1, expires_1, ..., fencing_n, expires_n}
# 冲突返回 {0, 冲突下标_1, ...}（1 基下标，按输入顺序）
ACQUIRE_BATCH_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local n = tonumber(ARGV[1])
local ttl = tonumber(ARGV[n + 2])

local conflicts = {}
for i = 1, n do
  local expires_at = redis.call('HGET', KEYS[i], 'expires_at')
  if expires_at and tonumber(expires_at) > now then
    conflicts[#conflicts + 1] = i
  end
end
if #conflicts > 0 then
  local reply = {0}
  for _, i in ipairs(conflicts) do
    reply[#reply + 1] = i
  end
  return reply
end

local expires = now + ttl
local reply = {1}
for i = 1, n do
  local fencing = redis.call('INCR', KEYS[n + i])
  redis.call('HSET', KEYS[i],
    'token', ARGV[i + 1],
    'fencing', fencing,
    'expires_at', expires)
  redis.call('PEXPIRE', KEYS[i], ttl)
  reply[#reply + 1] = fencing
  reply[#reply + 1] = expires
end
return reply
"""

# 续租：必须同时匹配令牌与防栅序号，且租约尚未过期。
# 已过期的租约不允许续租（持有者必须重新申请），
# 防止旧持有者在超时后"复活"自己的租约。
# KEYS[1] = 租约键
# ARGV[1] = 令牌, ARGV[2] = fencing, ARGV[3] = ttl_ms
# 返回 {是否成功(1/0), 原因, 新 expires_at}
RENEW_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  return {0, 'not_found', 0}
end
if token ~= ARGV[1] or redis.call('HGET', KEYS[1], 'fencing') ~= ARGV[2] then
  return {0, 'conflict', 0}
end

local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at'))
if expires_at <= now then
  return {0, 'expired', 0}
end

local ttl = tonumber(ARGV[3])
local new_expires = now + ttl
redis.call('HSET', KEYS[1], 'expires_at', new_expires)
redis.call('PEXPIRE', KEYS[1], ttl)
return {1, 'ok', new_expires}
"""

# 释放租约：令牌与防栅序号同时匹配才删除，否则静默忽略。
# 旧持有者超时后的释放请求不会误删新持有者的租约。
# KEYS[1] = 租约键
# ARGV[1] = 令牌, ARGV[2] = fencing
# 返回 1（已释放）或 0（无操作）
RELEASE_LUA = """
local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  return 0
end
if token ~= ARGV[1] or redis.call('HGET', KEYS[1], 'fencing') ~= ARGV[2] then
  return 0
end
redis.call('DEL', KEYS[1])
return 1
"""

# 校验：判断 (令牌, 防栅序号) 是否仍对应当前有效租约。
# KEYS[1] = 租约键
# ARGV[1] = 令牌, ARGV[2] = fencing
# 返回 {是否有效(1/0), 剩余毫秒}
VALIDATE_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  return {0, 0}
end
if token ~= ARGV[1] or redis.call('HGET', KEYS[1], 'fencing') ~= ARGV[2] then
  return {0, 0}
end

local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at'))
if expires_at <= now then
  return {0, 0}
end
return {1, expires_at - now}
"""

# 滑动窗口限流（成本累计）+ 可选请求幂等：单脚本原子完成
# "幂等命中判定 -> 清旧 -> 累计 -> 判定 -> 扣减 -> 记录完整响应"。
# 限流键是一个 ZSet：score 为请求时刻（Redis 服务器毫秒），
# member 形如 "<唯一标识>:<cost>"，同一毫秒内的并发请求也不会互相覆盖；
# 每笔成本直接编码在 member 尾部，无需额外的键。
# 幂等键是一个 Hash（仅携带 request_id 时传入 KEYS[2]）：
#   allowed / remaining / reset_at / retry_after / limit / window
# 保存首次请求的完整判定结果，PEXPIRE = 首次请求的 window_ms，
# 因此 request_id 仅在对应窗口内存活，窗口结束后自动删除、可被再次使用；
# Redis 单线程执行脚本，携带相同 request_id 的并发请求也只有一个完成扣减。
# KEYS[1] = 限流 ZSet 键
# KEYS[2] = 幂等 Hash 键（可选；未携带 request_id 时不传入）
# ARGV[1] = limit（窗口内允许的成本上限）
# ARGV[2] = window_ms
# ARGV[3] = cost（本次请求成本）
# ARGV[4] = 唯一标识（uuid hex，保证 member 不冲突）
# 幂等命中返回 {2, allowed, remaining, reset_at, retry_after, limit, window}
#   （直接回传首次结果，不清理 / 不判定 / 不扣减 / 不刷新任何 TTL）
# 首次放行返回 {1, remaining, reset_at_ms}
# 首次超限返回 {0, remaining, reset_at_ms, retry_after_ms}
# reset_at_ms 为窗口内最老一笔成本滑出的时刻；窗口为空时取 now + window_ms。
LIMIT_CHECK_LUA = """
local key = KEYS[1]

-- 幂等命中：原样返回首次请求记录的完整判定结果
if #KEYS >= 2 then
  local stored = redis.call('HMGET', KEYS[2],
    'allowed', 'remaining', 'reset_at', 'retry_after', 'limit', 'window')
  if stored[1] then
    return {2,
      tonumber(stored[1]), tonumber(stored[2]), tonumber(stored[3]),
      tonumber(stored[4]), tonumber(stored[5]), tonumber(stored[6])}
  end
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local window_start = now - window

-- 清理已滑出窗口的旧记录，ZSet 中只保留当前窗口内的成本
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- 从 member 尾部解析每笔成本并累计
local used = 0
local members = redis.call('ZRANGE', key, 0, -1)
for _, m in ipairs(members) do
  used = used + tonumber(string.match(m, ':(%d+)$'))
end

local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local reset_at
if #oldest == 0 then
  reset_at = now + window
else
  reset_at = tonumber(oldest[2]) + window
end

local allowed, remaining, retry_after
if used + cost > limit then
  allowed = 0
  remaining = limit - used
  retry_after = reset_at - now
else
  allowed = 1
  remaining = limit - used - cost
  retry_after = 0
  redis.call('ZADD', key, now, ARGV[4] .. ':' .. cost)
end
redis.call('PEXPIRE', key, window)

-- 首次请求：连同 limit / window 一起记录完整响应，TTL = window_ms，
-- 使 request_id 恰好存活到该窗口结束
if #KEYS >= 2 then
  redis.call('HSET', KEYS[2],
    'allowed', allowed,
    'remaining', remaining,
    'reset_at', reset_at,
    'retry_after', retry_after,
    'limit', limit,
    'window', window)
  redis.call('PEXPIRE', KEYS[2], window)
end

if allowed == 1 then
  return {1, remaining, reset_at}
end
return {0, remaining, reset_at, retry_after}
"""

