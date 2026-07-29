-- Sliding Window Rate Limiter using Redis Sorted Sets
-- KEYS[1]: rate limit key
-- ARGV[1]: current timestamp in milliseconds
-- ARGV[2]: window size in milliseconds
-- ARGV[3]: maximum requests allowed
-- ARGV[4]: unique request ID

local key = KEYS[1]
local now = tonumber(ARGV[1])
local window_size = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local request_id = ARGV[4]

-- Remove expired entries
local window_start = now - window_size
redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

-- Count current requests
local current_count = redis.call('ZCARD', key)

if current_count < limit then
    -- Allow the request
    redis.call('ZADD', key, now, request_id)
    redis.call('PEXPIRE', key, window_size + 1000)
    local remaining = limit - current_count - 1
    local reset_time = now + window_size
    return {1, remaining, reset_time}
else
    -- Deny the request
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local reset_time = now + window_size
    if #oldest > 0 then
        reset_time = tonumber(oldest[2]) + window_size
    end
    return {0, 0, reset_time}
end