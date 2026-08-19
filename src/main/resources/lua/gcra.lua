-- GCRA (Generic Cell Rate Algorithm) rate limiter -- atomic Redis implementation.
--
-- KEYS[1]: gcra state key. Stores a single value: the Theoretical Arrival
--          Time (TAT), in epoch milliseconds, as a string. No per-request
--          timestamps are stored (unlike Sliding Window's sorted set).
-- ARGV[1]: period_ms - the rate limiting window, in milliseconds
-- ARGV[2]: limit     - max requests allowed per period_ms. This also
--                       defines the burst capacity: this many requests may
--                       be admitted back-to-back against a fully idle key.
--
-- Returns a 4-element array:
--   [1] allowed   1 or 0
--   [2] remaining requests still permitted right now, if allowed (>= 0)
--   [3] time_ms   if allowed = 1: ms until the bucket is fully idle again
--                 (informational "reset", NOT a wait requirement)
--                 if allowed = 0: ms to wait before retrying (retry_after)
--   [4] tat_ms    the resulting (or, if rejected, unchanged) stored TAT --
--                 exposed for observability/debugging only

local key = KEYS[1]
local period_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

if not period_ms or not limit or limit <= 0 or period_ms <= 0 then
    return {0, 0, 0, 0}
end

-- Use Redis' own clock (TIME) rather than each application server's local
-- clock. All app instances share one Redis, so reading the time from
-- inside this atomic script guarantees every instance computes admission
-- decisions against the exact same clock, with no cross-node clock-skew
-- risk. TIME executes as part of the already-atomic script, so this adds
-- no extra network round trip.
local time_parts = redis.call('TIME')
local now_ms = math.floor(tonumber(time_parts[1]) * 1000 + tonumber(time_parts[2]) / 1000)

-- emission_interval: the ideal spacing, in ms, between successive
-- conforming requests in order to sustain exactly `limit` requests per
-- `period_ms`.
local emission_interval = period_ms / limit

-- delay_variation_tolerance (DVT): how far TAT may run ahead of "now"
-- while a request still conforms. Setting DVT = period_ms gives a burst
-- capacity of exactly `limit` requests against a fully idle key (i.e. the
-- same working definition of "N per window" that Sliding Window uses for
-- an idle key), which keeps the two algorithms directly comparable.
local dvt = period_ms

local tat_raw = redis.call('GET', key)
local tat
if tat_raw then
    tat = tonumber(tat_raw)
else
    -- No prior state: treat the key as idle, i.e. as if its last
    -- theoretical arrival time was "now".
    tat = now_ms
end

if tat < now_ms then
    -- The queue has already fully drained since the last request; forget
    -- the stale debt rather than letting it compound. This is standard
    -- GCRA behavior and is what lets GCRA recover capacity faster than a
    -- full window after a partial burst (see design notes).
    tat = now_ms
end

local new_tat = tat + emission_interval
local allow_at = new_tat - dvt

if allow_at <= now_ms then
    -- Conforming: commit the new TAT.
    redis.call('SET', key, new_tat)

    -- The key's state is only meaningful while TAT is ahead of "now";
    -- once TAT decays back to (or below) "now" the bucket is
    -- indistinguishable from a fresh/idle key, so it is safe -- and
    -- desirable, to bound Redis memory -- to let it expire at that point.
    local ttl_ms = math.ceil(new_tat - now_ms)
    if ttl_ms < 1 then ttl_ms = 1 end
    redis.call('PEXPIRE', key, ttl_ms)

    local remaining_ms = dvt - (new_tat - now_ms)
    local remaining = math.floor(remaining_ms / emission_interval)
    if remaining < 0 then remaining = 0 end

    local reset_after_ms = math.ceil(new_tat - now_ms)
    return {1, remaining, reset_after_ms, new_tat}
else
    -- Non-conforming: stored state is left untouched -- only a real
    -- admission may advance the TAT.
    local retry_after_ms = math.ceil(allow_at - now_ms)
    return {0, 0, retry_after_ms, tat}
end