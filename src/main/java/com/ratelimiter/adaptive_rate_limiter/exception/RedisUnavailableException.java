package com.ratelimiter.adaptive_rate_limiter.exception;

public class RedisUnavailableException extends RuntimeException {
    public RedisUnavailableException(String message) {
        super(message);
    }
}