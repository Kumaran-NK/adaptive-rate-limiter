-- Token Bucket Rate Limiter using Redis Hash
-- KEYS[1]: bucket key
-- ARGV[1]: current timestamp in milliseconds
-- ARGV[2]: bucket capacity (max tokens)
-- ARGV[3]: refill rate (tokens per millisecond)

local key = KEYS[1]
local now = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local refill_rate = tonumber(ARGV[3])

-- Get current bucket state
local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1]) or capacity
local last_refill = tonumber(bucket[2]) or now

-- Calculate refill
local elapsed = now - last_refill
local new_tokens = elapsed * refill_rate
tokens = math.min(capacity, tokens + new_tokens)

-- Try to consume
if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('PEXPIRE', key, 60000)
    return {1, math.floor(tokens), 0}
else
    local next_refill = now + math.ceil((1 - tokens) / refill_rate)
    return {0, 0, next_refill}
end