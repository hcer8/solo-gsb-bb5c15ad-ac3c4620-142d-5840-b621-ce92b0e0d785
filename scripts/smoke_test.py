#!/usr/bin/env python3
"""端到端冒烟测试（仅依赖 Python 标准库）。

先启动服务（docker compose up --build），然后执行：
    python3 scripts/smoke_test.py [base_url]

覆盖场景：获取 / 冲突 / 校验 / 续租 / 释放 / 防栅序号单调递增 /
旧持有者在超时后无法影响新租约，以及批量申请的全成全败、
冲突时不写不递增、过期接管与凭据兼容；以及滑动窗口成本限流的
剩余额度 / 成本累计 / 超限 429 / 窗口滑动恢复 / 并发不超额 /
参数校验 / 过期数据自动清理，以及 request_id 幂等：
重试回放不重复扣减 / 并发相同 request_id 只扣一次 / 429 结果回放 /
request_id 跨窗口可复用 / 非法 request_id 拒绝。
"""
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
FAILED = []


def call(method, path, body=None):
    status_code, headers, data = call_ex(method, path, body)
    return status_code, data


def call_ex(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        method=method,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.headers, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, e.headers, json.loads(e.read())


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f"  -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def main():
    key = f"smoke:{int(time.time() * 1000)}"

    code, body = call("GET", "/healthz")
    check("healthz", code == 200 and body.get("status") == "ok", f"{code} {body}")

    # 1. 申请租约
    code, a = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 1500})
    check("acquire #1", code == 200 and a["acquired"] and a["token"], f"{code} {a}")
    token_a, fence_a = a["token"], a["fencing_token"]

    # 2. 占用期间再次申请 -> 409
    code, b = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 1500})
    check("acquire conflict -> 409", code == 409 and not b["acquired"], f"{code} {b}")

    # 3. 校验当前持有者
    code, v = call(
        "POST",
        "/v1/leases/validate",
        {"task_key": key, "token": token_a, "fencing_token": fence_a},
    )
    check("validate holder", code == 200 and v["valid"] and v["remaining_ms"] > 0,
          f"{code} {v}")

    # 4. 错误令牌续租 -> 409
    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": "wrong", "fencing_token": fence_a, "ttl_ms": 1500},
    )
    check("renew with wrong token -> 409", code == 409 and not r["renewed"],
          f"{code} {r}")

    # 5. 正确续租
    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": token_a, "fencing_token": fence_a, "ttl_ms": 1500},
    )
    check("renew ok", code == 200 and r["renewed"], f"{code} {r}")

    # 6. 错误防栅序号释放 -> 幂等忽略
    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_a, "fencing_token": fence_a + 99},
    )
    check("release with wrong fencing ignored", code == 200 and not rel["released"],
          f"{code} {rel}")

    # 7. 正确释放
    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_a, "fencing_token": fence_a},
    )
    check("release ok", code == 200 and rel["released"], f"{code} {rel}")

    # 8. 重新申请，防栅序号必须单调递增
    code, b = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 800})
    check("re-acquire after release", code == 200 and b["acquired"], f"{code} {b}")
    check("fencing monotonic", b["fencing_token"] > fence_a,
          f"{b['fencing_token']} <= {fence_a}")
    token_b, fence_b = b["token"], b["fencing_token"]

    # 9. 等租约过期，新持有者可接管；旧持有者此后无法干扰
    time.sleep(1.0)
    code, c = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 5000})
    check("acquire after expiry", code == 200 and c["acquired"], f"{code} {c}")
    token_c, fence_c = c["token"], c["fencing_token"]

    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": token_b, "fencing_token": fence_b, "ttl_ms": 5000},
    )
    check("stale holder renew rejected", code == 409 and not r["renewed"],
          f"{code} {r}")

    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_b, "fencing_token": fence_b},
    )
    check("stale holder release ignored", code == 200 and not rel["released"],
          f"{code} {rel}")

    code, v = call(
        "POST",
        "/v1/leases/validate",
        {"task_key": key, "token": token_c, "fencing_token": fence_c},
    )
    check("new lease unaffected", code == 200 and v["valid"], f"{code} {v}")

    # 10. 状态查询
    code, st = call("GET", f"/v1/leases/{key}")
    check("inspect", code == 200 and st["held"] and st["fencing_token"] == fence_c,
          f"{code} {st}")

    batch_checks()
    limit_checks()

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All checks passed.")


def batch_checks():
    """acquire-batch：参数校验 / 全成 / 全败不写不递增 / 过期接管 / 凭据兼容。"""
    base = f"smoke-batch:{int(time.time() * 1000)}"
    k1, k2, k3, k4, k5 = [f"{base}:{i}" for i in range(5)]

    # 参数校验：少于 2 个 / 多于 32 个 / 有重复 -> 422
    code, _ = call("POST", "/v1/leases/acquire-batch", {"task_keys": [k1]})
    check("batch size < 2 -> 422", code == 422, f"code={code}")
    code, _ = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [f"{base}:x{i}" for i in range(33)]})
    check("batch size > 32 -> 422", code == 422, f"code={code}")
    code, _ = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k1, k1], "ttl_ms": 5000})
    check("batch duplicate keys -> 422", code == 422, f"code={code}")

    # 全成：顺序一致、token 独立、fencing 独立递增、expires_at 同一基准
    code, b = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k1, k2, k3], "ttl_ms": 5000})
    check("batch acquire ok", code == 200 and b["acquired"], f"{code} {b}")
    leases = b.get("leases", [])
    check("batch leases order/count",
          [l["task_key"] for l in leases] == [k1, k2, k3], f"{leases}")
    check("batch distinct tokens",
          len({l["token"] for l in leases}) == 3, f"{leases}")
    check("batch same expires baseline",
          len({l["expires_at_ms"] for l in leases}) == 1, f"{leases}")

    # 凭据兼容现有 validate 接口
    for l in leases:
        code, v = call("POST", "/v1/leases/validate",
                       {"task_key": l["task_key"], "token": l["token"],
                        "fencing_token": l["fencing_token"]})
        check(f"batch credential validate {l['task_key']}",
              code == 200 and v["valid"], f"{code} {v}")

    # 冲突：k3 仍被占用，k5 从未申请；应 409、仅报 k3，且 k5 不得被创建、计数器不得递增
    code, c = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k3, k5], "ttl_ms": 5000})
    check("batch conflict -> 409", code == 409 and not c.get("acquired"),
          f"{code} {c}")
    check("batch conflict keys",
          c.get("conflicting_task_keys") == [k3], f"{c}")
    code, st5 = call("GET", f"/v1/leases/{k5}")
    check("batch conflict created nothing",
          code == 200 and not st5["held"], f"{code} {st5}")
    # k5 从未申请成功；其第一次成功申请的 fencing 必须为 1，证明批量失败未 INCR
    code, a5 = call("POST", "/v1/leases/acquire", {"task_key": k5, "ttl_ms": 5000})
    check("batch conflict no fencing increment",
          code == 200 and a5["fencing_token"] == 1, f"{code} {a5}")

    # 全败时冲突方自己也不受影响：k3 的旧凭据仍可续租
    l3 = next(l for l in leases if l["task_key"] == k3)
    code, r = call("POST", "/v1/leases/renew",
                   {"task_key": k3, "token": l3["token"],
                    "fencing_token": l3["fencing_token"], "ttl_ms": 5000})
    check("batch conflict holder untouched", code == 200 and r["renewed"],
          f"{code} {r}")

    # 释放整批后，另起一批；过期租约可被接管，全新键与过期键混在一起
    for l in leases:
        code, rel = call("POST", "/v1/leases/release",
                         {"task_key": l["task_key"], "token": l["token"],
                          "fencing_token": l["fencing_token"]})
        check(f"batch release {l['task_key']}",
              code == 200 and rel["released"], f"{code} {rel}")
    code, rel = call("POST", "/v1/leases/release",
                     {"task_key": k5, "token": a5["token"],
                      "fencing_token": a5["fencing_token"]})
    check("batch setup release k5", code == 200 and rel["released"], f"{code} {rel}")

    # k1 用短 TTL 单独占用，等待过期后与全新的 k4 一起批量接管
    code, old = call("POST", "/v1/leases/acquire", {"task_key": k1, "ttl_ms": 300})
    check("batch takeover setup", code == 200 and old["acquired"], f"{code} {old}")
    time.sleep(0.7)
    code, t = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k1, k4], "ttl_ms": 5000})
    check("batch acquire after expiry", code == 200 and t["acquired"], f"{code} {t}")
    taken = {l["task_key"]: l for l in t.get("leases", [])}
    check("batch takeover fencing advanced",
          k1 in taken and taken[k1]["fencing_token"] > old["fencing_token"],
          f"{t.get('leases')}")
    check("batch free key fencing starts 1",
          taken.get(k4, {}).get("fencing_token") == 1, f"{t.get('leases')}")

    # 旧持有者释放不得影响批量接管后的新租约
    code, rel = call("POST", "/v1/leases/release",
                     {"task_key": k1, "token": old["token"],
                      "fencing_token": old["fencing_token"]})
    check("batch stale release ignored", code == 200 and not rel["released"],
          f"{code} {rel}")
    code, v = call("POST", "/v1/leases/validate",
                   {"task_key": k1, "token": taken[k1]["token"],
                    "fencing_token": taken[k1]["fencing_token"]})
    check("batch new lease valid", code == 200 and v["valid"], f"{code} {v}")


def limit_checks():
    """/v1/limits/check：剩余额度 / 成本累计 / 429 / 窗口滑动 / 并发不超额 / 自动清理。"""
    key = f"smoke-limit:{int(time.time() * 1000)}"
    path = "/v1/limits/check"

    # 参数校验：limit=0 / cost=0 / cost>limit / 缺 window_ms / window 超界
    code, b = call("POST", path, {"client_key": key, "limit": 0,
                                  "window_ms": 1000, "cost": 1})
    check("limit zero -> 422", code == 422, f"code={code} {b}")
    code, b = call("POST", path, {"client_key": key, "limit": 10,
                                  "window_ms": 1000, "cost": 0})
    check("cost zero -> 422", code == 422, f"code={code} {b}")
    code, b = call("POST", path, {"client_key": key, "limit": 5,
                                  "window_ms": 1000, "cost": 6})
    check("cost greater than limit -> 422", code == 422, f"code={code} {b}")
    code, b = call("POST", path, {"client_key": key, "limit": 10, "cost": 1})
    check("missing window_ms -> 422", code == 422, f"code={code} {b}")
    code, b = call("POST", path, {"client_key": key, "limit": 10,
                                  "window_ms": 90_000_000, "cost": 1})
    check("window_ms out of range -> 400", code == 400, f"code={code} {b}")

    # 逐笔扣减，remaining 递减到 0；空窗口的 reset_at_ms ≈ now + window
    now_ms = int(time.time() * 1000)
    code, b = call("POST", path, {"client_key": key, "limit": 3,
                                  "window_ms": 10_000, "cost": 1})
    check("limit allow #1", code == 200 and b["allowed"]
          and b["remaining"] == 2, f"{code} {b}")
    check("limit fresh reset_at near now+window",
          abs(b["reset_at_ms"] - (now_ms + 10_000)) < 2000, f"{b}")
    code, b = call("POST", path, {"client_key": key, "limit": 3,
                                  "window_ms": 10_000, "cost": 1})
    check("limit allow #2 remaining 1",
          code == 200 and b["allowed"] and b["remaining"] == 1, f"{code} {b}")
    code, b = call("POST", path, {"client_key": key, "limit": 3,
                                  "window_ms": 10_000, "cost": 1})
    check("limit allow #3 remaining 0",
          code == 200 and b["allowed"] and b["remaining"] == 0, f"{code} {b}")

    # 超限：429、不扣减、retry_after_ms 在 (0, window] 内，带 Retry-After 头
    code, headers, b = call_ex("POST", path, {"client_key": key, "limit": 3,
                                              "window_ms": 10_000, "cost": 1})
    check("limit exceed -> 429",
          code == 429 and not b["allowed"] and b["remaining"] == 0,
          f"{code} {b}")
    check("limit exceed retry_after_ms",
          0 < b["retry_after_ms"] <= 10_000, f"{b}")
    retry_after_hdr = headers.get("Retry-After")
    check("limit exceed Retry-After header",
          retry_after_hdr is not None and int(retry_after_hdr) >= 1,
          f"Retry-After={retry_after_hdr}")
    # 超限请求不扣减：再来一次结果完全一致
    code, b2 = call("POST", path, {"client_key": key, "limit": 3,
                                   "window_ms": 10_000, "cost": 1})
    check("limit denied not charged",
          code == 429 and b2["remaining"] == b["remaining"]
          and b2["reset_at_ms"] == b["reset_at_ms"], f"{b} vs {b2}")

    # 成本加权：独立键，limit=10：cost 6 放行剩 4，cost 5 拒绝，cost 4 放行剩 0
    wkey = f"{key}:weighted"
    code, b = call("POST", path, {"client_key": wkey, "limit": 10,
                                  "window_ms": 10_000, "cost": 6})
    check("weighted allow cost 6",
          code == 200 and b["allowed"] and b["remaining"] == 4, f"{code} {b}")
    code, b = call("POST", path, {"client_key": wkey, "limit": 10,
                                  "window_ms": 10_000, "cost": 5})
    check("weighted deny cost 5", code == 429 and not b["allowed"],
          f"{code} {b}")
    code, b = call("POST", path, {"client_key": wkey, "limit": 10,
                                  "window_ms": 10_000, "cost": 4})
    check("weighted allow cost 4 remaining 0",
          code == 200 and b["allowed"] and b["remaining"] == 0, f"{code} {b}")

    # 滑动恢复 + 旧数据自动清理：窗口 800ms 打满后等待滑出，应恢复全部额度
    skey = f"{key}:sliding"
    for _ in range(3):
        code, b = call("POST", path, {"client_key": skey, "limit": 3,
                                      "window_ms": 800, "cost": 1})
        check("sliding fill", code == 200 and b["allowed"], f"{code} {b}")
    code, b = call("POST", path, {"client_key": skey, "limit": 3,
                                  "window_ms": 800, "cost": 1})
    check("sliding full -> 429", code == 429, f"{code} {b}")
    time.sleep(1.0)
    now_ms = int(time.time() * 1000)
    code, b = call("POST", path, {"client_key": skey, "limit": 3,
                                  "window_ms": 800, "cost": 1})
    check("sliding recovered full quota",
          code == 200 and b["allowed"] and b["remaining"] == 2, f"{code} {b}")
    # 旧记录已被清理：新窗口第一笔的 reset_at 应基于当前时刻而非最老旧记录
    check("sliding old data cleaned (reset renewed)",
          abs(b["reset_at_ms"] - (now_ms + 800)) < 1500, f"{b}")

    # 并发不超额：20 个同键并发成本 1，limit=10，恰好 10 个放行
    ckey = f"{key}:concurrent"

    def one_hit(_):
        return call("POST", path, {"client_key": ckey, "limit": 10,
                                   "window_ms": 10_000, "cost": 1})

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(one_hit, range(20)))
    allowed = [r for code, r in results if code == 200 and r["allowed"]]
    denied = [r for code, r in results if code == 429 and not r["allowed"]]
    check("concurrent no over-admit",
          len(allowed) == 10 and len(denied) == 10,
          f"allowed={len(allowed)} denied={len(denied)}")
    if allowed:
        remainings = sorted(r["remaining"] for r in allowed)
        check("concurrent remaining consistent",
              remainings == list(range(0, 10)), f"{remainings}")
        check("concurrent denied retry_after present",
              all(r["retry_after_ms"] > 0 for r in denied), f"{denied[:2]}")

    idempotency_checks()


def idempotency_checks():
    """request_id 幂等：回放不重复扣减 / 并发只扣一次 / 429 回放 / 跨窗口复用 / 校验。"""
    base = f"smoke-idem:{int(time.time() * 1000)}"
    path = "/v1/limits/check"

    # 参数校验：空串 / 过长 / 含非法字符 -> 422
    code, _ = call("POST", path, {"client_key": base, "limit": 10,
                                  "window_ms": 10_000, "cost": 1,
                                  "request_id": ""})
    check("idem empty request_id -> 422", code == 422, f"code={code}")
    code, _ = call("POST", path, {"client_key": base, "limit": 10,
                                  "window_ms": 10_000, "cost": 1,
                                  "request_id": "x" * 129})
    check("idem long request_id -> 422", code == 422, f"code={code}")
    code, _ = call("POST", path, {"client_key": base, "limit": 10,
                                  "window_ms": 10_000, "cost": 1,
                                  "request_id": "bad id/中"})
    check("idem illegal chars request_id -> 422", code == 422, f"code={code}")

    # 首次请求放行并回显 request_id；非回放不带 Idempotent-Replay
    key = f"{base}:basic"
    rid = "req-0001"
    code, headers, b = call_ex("POST", path,
                               {"client_key": key, "limit": 3, "window_ms": 10_000,
                                "cost": 1, "request_id": rid})
    check("idem first allow", code == 200 and b["allowed"]
          and b["remaining"] == 2 and b["request_id"] == rid
          and b["idempotent_replay"] is False, f"{code} {b}")
    check("idem first no replay header",
          headers.get("Idempotent-Replay") is None,
          f"Idempotent-Replay={headers.get('Idempotent-Replay')}")

    # 窗口内其他请求把额度打满（与 rid 无关，验证回放不受后续状态影响）
    call("POST", path, {"client_key": key, "limit": 3,
                        "window_ms": 10_000, "cost": 2})
    # 此时真实额度已满；重试相同 request_id 必须回放首次结果（remaining=2）且不扣减
    code, headers, b2 = call_ex("POST", path,
                                {"client_key": key, "limit": 3, "window_ms": 10_000,
                                 "cost": 1, "request_id": rid})
    check("idem replay returns first result",
          code == 200 and b2["allowed"] and b2["remaining"] == 2
          and b2["reset_at_ms"] == b["reset_at_ms"]
          and b2["request_id"] == rid and b2["idempotent_replay"] is True,
          f"{code} first={b} replay={b2}")
    check("idem replay header",
          headers.get("Idempotent-Replay") == "true",
          f"Idempotent-Replay={headers.get('Idempotent-Replay')}")

    # 回放期间的重试参数不重新判定（limit/window 仍以首次记录为准）
    code, b3 = call("POST", path,
                    {"client_key": key, "limit": 99, "window_ms": 10_000,
                     "cost": 1, "request_id": rid})
    check("idem replay echoes first limit/window",
          code == 200 and b3["idempotent_replay"] and b3["limit"] == 3
          and b3["window_ms"] == 10_000, f"{code} {b3}")

    # 不带 request_id 的普通请求不受影响：额度已满 -> 429
    code, b4 = call("POST", path, {"client_key": key, "limit": 3,
                                   "window_ms": 10_000, "cost": 1})
    check("idem non-idem path still charged",
          code == 429 and not b4["allowed"] and b4["request_id"] is None,
          f"{code} {b4}")

    # 429 结果同样被记录：先在新键打满窗口，再携带 request_id 请求被拒
    dkey = f"{base}:denied"
    call("POST", path, {"client_key": dkey, "limit": 2,
                        "window_ms": 10_000, "cost": 2})
    drid = "req-deny-1"
    code, headers1, d1 = call_ex("POST", path,
                                 {"client_key": dkey, "limit": 2, "window_ms": 10_000,
                                  "cost": 1, "request_id": drid})
    check("idem first deny -> 429",
          code == 429 and not d1["allowed"] and d1["retry_after_ms"] > 0
          and d1["idempotent_replay"] is False, f"{code} {d1}")
    check("idem first deny Retry-After",
          headers1.get("Retry-After") is not None,
          f"Retry-After={headers1.get('Retry-After')}")
    # 窗口滑出部分后重试：仍回放首次的 429（同一 reset_at_ms / retry_after_ms 快照）
    code, headers2, d2 = call_ex("POST", path,
                                 {"client_key": dkey, "limit": 2, "window_ms": 10_000,
                                  "cost": 1, "request_id": drid})
    check("idem denied replay",
          code == 429 and d2["idempotent_replay"] is True
          and d2["allowed"] is False
          and d2["reset_at_ms"] == d1["reset_at_ms"]
          and d2["retry_after_ms"] == d1["retry_after_ms"],
          f"{code} first={d1} replay={d2}")
    check("idem denied replay headers",
          headers2.get("Idempotent-Replay") == "true"
          and headers2.get("Retry-After") == headers1.get("Retry-After"),
          f"h1={dict(headers1)} h2={dict(headers2)}")

    # 并发相同 request_id：20 个并发只有 1 个完成扣减，其余全部回放首次结果
    ckey = f"{base}:concurrent"
    crid = "req-conc-1"
    body = {"client_key": ckey, "limit": 10, "window_ms": 10_000,
            "cost": 1, "request_id": crid}

    def one_idem_hit(_):
        return call_ex("POST", path, body)

    with ThreadPoolExecutor(max_workers=20) as pool:
        cresults = list(pool.map(one_idem_hit, range(20)))
    firsts = [d for _, h, d in cresults if d.get("idempotent_replay") is False]
    replays = [d for _, h, d in cresults if d.get("idempotent_replay") is True]
    check("idem concurrent single deduction",
          len(firsts) == 1 and len(replays) == 19
          and all(d["allowed"] and d["remaining"] == 9
                  for _, _, d in cresults)
          and all(d["reset_at_ms"] == firsts[0]["reset_at_ms"]
                  for d in replays),
          f"firsts={len(firsts)} replays={len(replays)}")
    check("idem concurrent replay headers",
          sum(1 for _, h, _ in cresults if h.get("Idempotent-Replay") == "true") == 19,
          f"{[h.get('Idempotent-Replay') for _, h, _ in cresults]}")
    # 实际只扣了一笔：不带 request_id 再请求一笔应放行且 remaining=8
    code, after = call("POST", path, {"client_key": ckey, "limit": 10,
                                      "window_ms": 10_000, "cost": 1})
    check("idem concurrent total charged once",
          code == 200 and after["allowed"] and after["remaining"] == 8,
          f"{code} {after}")

    # request_id 仅在对应窗口存活：800ms 窗口结束后同一 request_id 可再次使用，
    # 且按新请求重新判定扣减
    skey = f"{base}:sliding"
    srid = "req-window-1"
    code, s1 = call("POST", path,
                    {"client_key": skey, "limit": 3, "window_ms": 800,
                     "cost": 1, "request_id": srid})
    check("idem short window first", code == 200 and s1["allowed"], f"{code} {s1}")
    code, s2 = call("POST", path,
                    {"client_key": skey, "limit": 3, "window_ms": 800,
                     "cost": 1, "request_id": srid})
    check("idem short window replay",
          code == 200 and s2["idempotent_replay"] is True, f"{code} {s2}")
    time.sleep(1.1)
    code, s3 = call("POST", path,
                    {"client_key": skey, "limit": 3, "window_ms": 800,
                     "cost": 1, "request_id": srid})
    check("idem request_id reusable across window",
          code == 200 and s3["allowed"] and s3["idempotent_replay"] is False
          and s3["remaining"] == 2
          and s3["reset_at_ms"] != s1["reset_at_ms"], f"{code} {s3}")

    # request_id 作用域为 client_key：不同 client_key 使用同一 request_id 互不影响
    xkey, ykey = f"{base}:scope-a", f"{base}:scope-b"
    shared_rid = "req-shared"
    code, x1 = call("POST", path,
                    {"client_key": xkey, "limit": 1, "window_ms": 10_000,
                     "cost": 1, "request_id": shared_rid})
    code, y1 = call("POST", path,
                    {"client_key": ykey, "limit": 1, "window_ms": 10_000,
                     "cost": 1, "request_id": shared_rid})
    check("idem request_id scoped per client_key",
          x1["allowed"] and y1["allowed"]
          and x1["idempotent_replay"] is False
          and y1["idempotent_replay"] is False, f"{x1} {y1}")


if __name__ == "__main__":
    main()
