"""租约状态机与滑动窗口限流的全部写/判操作都在 Redis Lua 中原子完成。

租约约定：
- 租约键（KEYS[1] 或 lease key）是一个 Hash：
    token       所有权令牌（UUID）
    fencing     防栅序号（单调递增）
    expires_at  过期时间，Unix 毫秒，基于 Redis 服务器时钟
- 防栅计数器键独立存放、永不过期，保证同一任务键的 fencing 只增不减。
- 过期判断一律使用 redis.call('TIME')，即 Redis 服务器时间，
  避免各客户端时钟不一致导致的问题。

租约审计约定：
- 每个任务键对应一个 Redis Stream（事件流键），申请（成功 / 被拒）、
  续租、释放以及批量申请的逐任务结果都在状态转换的同一脚本内 XADD 落账，
  与状态转换原子提交，不可能出现"操作成功但事件丢失"。
- 条目字段：op（acquire / acquire_batch / renew / release）、
  result（操作结果）、fencing（相关防栅序号，无则空串）、
  time_ms（Redis 服务器毫秒，与状态判定同一时钟）、
  batch_size（仅批量申请携带）。
- retention > 0 时以 XADD MAXLEN 精确裁剪，每个流只保留最新的 retention 条，
  超出部分从最旧事件开始丢弃；retention = 0 表示不限制。

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

# 租约审计公共函数：拼接到每个需要落账的租约脚本前。
# 条目 ID 由 XADD '*' 按 Redis 服务器时间生成（毫秒-序号），
# 同时在字段里显式记录 time_ms，便于消费方直接读取。
# retention > 0 时精确保留最新 retention 条（裁剪最旧事件）；= 0 不裁剪。
_AUDIT_LUA = """
local function audit_add(stream, retention, fields)
  local keep = tonumber(retention)
  if keep and keep > 0 then
    return redis.call('XADD', stream, 'MAXLEN', keep, '*', unpack(fields))
  end
  return redis.call('XADD', stream, '*', unpack(fields))
end
"""

# 申请租约：仅当租约不存在或已过期时成功。
# KEYS[1] = 租约键, KEYS[2] = 防栅计数器键, KEYS[3] = 审计事件流键
# ARGV[1] = 新令牌, ARGV[2] = ttl_ms, ARGV[3] = 事件保留条数
# 返回 {是否成功(1/0), fencing, expires_at}
ACQUIRE_LUA = _AUDIT_LUA + """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ttl = tonumber(ARGV[2])
local retention = ARGV[3]

local expires_at = redis.call('HGET', KEYS[1], 'expires_at')
if expires_at and tonumber(expires_at) > now then
  local fencing = tonumber(redis.call('HGET', KEYS[1], 'fencing'))
  audit_add(KEYS[3], retention,
    {'op', 'acquire', 'result', 'conflict',
     'fencing', tostring(fencing), 'time_ms', tostring(now)})
  return {0, fencing, tonumber(expires_at)}
end

local fencing = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1],
  'token', ARGV[1],
  'fencing', fencing,
  'expires_at', now + ttl)
redis.call('PEXPIRE', KEYS[1], ttl)
audit_add(KEYS[3], retention,
  {'op', 'acquire', 'result', 'acquired',
   'fencing', tostring(fencing), 'time_ms', tostring(now)})
return {1, fencing, now + ttl}
"""

# 批量申请（2～32 个任务键，统一 TTL），全成全败：
# 先统一检查所有任务，任一仍被有效租约占用即整体失败，
# 不创建任何租约、不递增任何防栅计数器；
# 全部可用（不存在或已过期，过期可被接管）时再统一写入。
# 全程只调用一次 TIME，所有成功租约的 expires_at 基于同一基准时刻。
# 审计事件逐任务写入各自的事件流：
#   成功 -> 每个任务一条 acquired；冲突 -> 仅每个被占用任务一条 conflict
#   （未冲突任务在失败批次中没有任何状态变化，与"不创建不递增"一致）。
# KEYS[1..n]       = 租约键
# KEYS[n+1..2n]    = 防栅计数器键（与租约键同序）
# KEYS[2n+1..3n]   = 审计事件流键（与租约键同序）
# ARGV[1]          = 任务数 n
# ARGV[2..n+1]     = 各任务的新令牌（与输入同序）
# ARGV[n+2]        = ttl_ms
# ARGV[n+3]        = 事件保留条数
# 成功返回 {1, fencing_1, expires_1, ..., fencing_n, expires_n}
# 冲突返回 {0, 冲突下标_1, ...}（1 基下标，按输入顺序）
ACQUIRE_BATCH_LUA = _AUDIT_LUA + """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local n = tonumber(ARGV[1])
local ttl = tonumber(ARGV[n + 2])
local retention = ARGV[n + 3]

local conflicts = {}
for i = 1, n do
  local expires_at = redis.call('HGET', KEYS[i], 'expires_at')
  if expires_at and tonumber(expires_at) > now then
    conflicts[#conflicts + 1] = i
  end
end
if #conflicts > 0 then
  for _, i in ipairs(conflicts) do
    local fencing = tonumber(redis.call('HGET', KEYS[i], 'fencing'))
    audit_add(KEYS[2 * n + i], retention,
      {'op', 'acquire_batch', 'result', 'conflict',
       'fencing', tostring(fencing), 'time_ms', tostring(now),
       'batch_size', tostring(n)})
  end
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
  audit_add(KEYS[2 * n + i], retention,
    {'op', 'acquire_batch', 'result', 'acquired',
     'fencing', tostring(fencing), 'time_ms', tostring(now),
     'batch_size', tostring(n)})
  reply[#reply + 1] = fencing
  reply[#reply + 1] = expires
end
return reply
"""

# 续租：必须同时匹配令牌与防栅序号，且租约尚未过期。
# 已过期的租约不允许续租（持有者必须重新申请），
# 防止旧持有者在超时后"复活"自己的租约。
# KEYS[1] = 租约键, KEYS[2] = 审计事件流键
# ARGV[1] = 令牌, ARGV[2] = fencing, ARGV[3] = ttl_ms
# ARGV[4] = 事件保留条数
# 返回 {是否成功(1/0), 原因, 新 expires_at}
RENEW_LUA = _AUDIT_LUA + """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local retention = ARGV[4]

local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  audit_add(KEYS[2], retention,
    {'op', 'renew', 'result', 'not_found',
     'fencing', '', 'time_ms', tostring(now)})
  return {0, 'not_found', 0}
end
local current_fencing = redis.call('HGET', KEYS[1], 'fencing')
if token ~= ARGV[1] or current_fencing ~= ARGV[2] then
  local f = tonumber(current_fencing)
  audit_add(KEYS[2], retention,
    {'op', 'renew', 'result', 'conflict',
     'fencing', f and tostring(f) or '', 'time_ms', tostring(now)})
  return {0, 'conflict', 0}
end

local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at'))
if expires_at <= now then
  audit_add(KEYS[2], retention,
    {'op', 'renew', 'result', 'expired',
     'fencing', current_fencing, 'time_ms', tostring(now)})
  return {0, 'expired', 0}
end

local ttl = tonumber(ARGV[3])
local new_expires = now + ttl
redis.call('HSET', KEYS[1], 'expires_at', new_expires)
redis.call('PEXPIRE', KEYS[1], ttl)
audit_add(KEYS[2], retention,
  {'op', 'renew', 'result', 'renewed',
   'fencing', ARGV[2], 'time_ms', tostring(now)})
return {1, 'ok', new_expires}
"""

# 释放租约：令牌与防栅序号同时匹配才删除，否则静默忽略。
# 旧持有者超时后的释放请求不会误删新持有者的租约。
# KEYS[1] = 租约键, KEYS[2] = 审计事件流键
# ARGV[1] = 令牌, ARGV[2] = fencing, ARGV[3] = 事件保留条数
# 返回 1（已释放）或 0（无操作）
RELEASE_LUA = _AUDIT_LUA + """
local token = redis.call('HGET', KEYS[1], 'token')
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local retention = ARGV[3]

if not token then
  audit_add(KEYS[2], retention,
    {'op', 'release', 'result', 'ignored',
     'fencing', '', 'time_ms', tostring(now)})
  return 0
end
local current_fencing = redis.call('HGET', KEYS[1], 'fencing')
if token ~= ARGV[1] or current_fencing ~= ARGV[2] then
  local f = tonumber(current_fencing)
  audit_add(KEYS[2], retention,
    {'op', 'release', 'result', 'ignored',
     'fencing', f and tostring(f) or '', 'time_ms', tostring(now)})
  return 0
end
redis.call('DEL', KEYS[1])
audit_add(KEYS[2], retention,
  {'op', 'release', 'result', 'released',
   'fencing', ARGV[2], 'time_ms', tostring(now)})
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
