-- Batch-GCRA quota leasing -- atomic Redis implementation.
--
-- Grants (leases) up to `requested` units of quota in one call by advancing
-- the SAME GCRA Theoretical Arrival Time (TAT) that gcra.lua uses, by
-- `granted * emission_interval` in a single step -- instead of one
-- emission_interval per admitted request. This advancing-the-TAT step IS the
-- distributed coordination mechanism: quota is reserved by moving the shared
-- TAT forward, NOT by decrementing a plain integer counter. That preserves
-- GCRA's pacing and self-expiry, and makes a batch of size 1 reduce EXACTLY to
-- gcra.lua's per-request decision (proven by GcraQuotaAllocatorTest).
--
-- Operates on the SAME key as gcra.lua (ratelimit:<id>:gcra) by design: one
-- TAT per key, whether the endpoint is served by direct GCRA or by leasing.
--
-- KEYS[1]: gcra state key -- the TAT (epoch ms) as a string. Identical to gcra.lua.
-- ARGV[1]: period_ms  - the rate limiting window, in milliseconds
-- ARGV[2]: limit      - max requests per period_ms (also the idle burst capacity)
-- ARGV[3]: requested  - how many units the caller wants to lease this call
-- ARGV[4]: unused     - units from the caller's PREVIOUS lease that went unused,
--                       refunded (credited back) before this grant. Pass 0 when
--                       there is nothing to refund. A refund can never rewind the
--                       TAT earlier than "now" -- you can never bank more than a
--                       single fully-idle burst of capacity.
--
-- Returns a 4-element array (mirrors gcra.lua's 4-element shape):
--   [1] granted            units actually leased this call (0 <= granted <= requested)
--   [2] new_tat_ms         resulting stored TAT (observability/debugging only; note
--                          Redis truncates Lua numbers to integers on return, so
--                          read the stored key for fractional-exact TAT assertions)
--   [3] now_ms             Redis' own clock at decision time (epoch ms)
--   [4] next_available_ms  ms until the NEXT unit beyond `granted` becomes
--                          grantable: 0 if more quota is immediately available,
--                          > 0 if the key is now saturated

local key = KEYS[1]
local period_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local unused = tonumber(ARGV[4]) or 0

if not period_ms or not limit or not requested
        or limit <= 0 or period_ms <= 0 or requested <= 0 then
    return {0, 0, 0, 0}
end
if unused < 0 then unused = 0 end

-- Use Redis' own clock (see gcra.lua): one shared clock for every app
-- instance, so no cross-node clock skew, and no extra network round trip since
-- TIME runs inside this already-atomic script.
local time_parts = redis.call('TIME')
local now_ms = math.floor(tonumber(time_parts[1]) * 1000 + tonumber(time_parts[2]) / 1000)

local emission_interval = period_ms / limit
local dvt = period_ms

local tat_raw = redis.call('GET', key)
local tat
if tat_raw then
    tat = tonumber(tat_raw)
else
    tat = now_ms
end

-- Credit any unused quota from the caller's previous lease by rewinding the
-- TAT, but never earlier than "now": a fully idle key already offers exactly
-- one full burst, and no refund may bank more than that. With unused = 0 this
-- line is a no-op and the whole script is a batch generalisation of gcra.lua.
tat = tat - unused * emission_interval

-- Drain stale debt, exactly like gcra.lua: once the queue has fully drained,
-- the old TAT is meaningless, so start accounting from "now".
if tat < now_ms then
    tat = now_ms
end
local base_tat = tat

-- Virtual time available to grant against before the TAT would run past
-- now + DVT (the conformance ceiling). floor(available / emission) is how many
-- whole units fit; clamp to what was requested and to >= 0.
local available_virtual_time = now_ms + dvt - base_tat
local granted = math.floor(available_virtual_time / emission_interval)
if granted < 0 then granted = 0 end
if granted > requested then granted = requested end

local new_tat = base_tat + granted * emission_interval

if granted > 0 then
    redis.call('SET', key, new_tat)
    -- Same self-expiry rule as gcra.lua: the key is only meaningful while the
    -- TAT is ahead of "now"; let it expire once the leased virtual time drains,
    -- to bound Redis memory.
    local ttl_ms = math.ceil(new_tat - now_ms)
    if ttl_ms < 1 then ttl_ms = 1 end
    redis.call('PEXPIRE', key, ttl_ms)
end

-- When does the NEXT unit (granted + 1) become grantable? 0 if immediately
-- (the key still has slack), > 0 if the key is now saturated and a caller must
-- wait. Mirrors gcra.lua's retry_after computation.
local next_available_ms = math.ceil(new_tat + emission_interval - dvt - now_ms)
if next_available_ms < 0 then next_available_ms = 0 end

return {granted, new_tat, now_ms, next_available_ms}
