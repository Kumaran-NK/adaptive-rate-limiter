package com.ratelimiter.adaptive_rate_limiter.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

@ControllerAdvice
public class RedisExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(RedisExceptionHandler.class);

    @ExceptionHandler({RedisConnectionFailureException.class, RedisSystemException.class})
    public void handleRedisConnectionFailure(Exception e, 
                                              HttpServletResponse response) throws IOException {
        log.warn("Redis unavailable: {}", e.getMessage());
        response.setStatus(HttpStatus.SERVICE_UNAVAILABLE.value());
        response.setContentType("application/json");
        response.getWriter().write(
            "{\"error\":\"Service temporarily degraded\",\"message\":\"Redis unavailable\"}"
        );
    }
}