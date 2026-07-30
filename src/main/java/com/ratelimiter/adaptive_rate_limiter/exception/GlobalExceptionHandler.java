package com.ratelimiter.adaptive_rate_limiter.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.time.Instant;
import java.util.Map;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<Map<String, Object>> handleRateLimitExceeded(RateLimitExceededException e) {
        return ResponseEntity.status(429).body(Map.of(
                "error", "Rate limit exceeded",
                "message", e.getMessage(),
                "timestamp", Instant.now().toString()
        ));
    }

    @ExceptionHandler(RedisUnavailableException.class)
    public ResponseEntity<Map<String, Object>> handleRedisUnavailable(RedisUnavailableException e) {
        return ResponseEntity.status(503).body(Map.of(
                "error", "Service temporarily degraded",
                "message", e.getMessage(),
                "timestamp", Instant.now().toString()
        ));
    }
}