package com.ratelimiter.adaptive_rate_limiter.interceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import com.ratelimiter.adaptive_rate_limiter.model.RateLimitDecision;
import com.ratelimiter.adaptive_rate_limiter.service.RateLimiterService;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Component
public class RateLimitInterceptor implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(RateLimitInterceptor.class);

    private final RateLimiterService rateLimiterService;

    public RateLimitInterceptor(RateLimiterService rateLimiterService) {
        this.rateLimiterService = rateLimiterService;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response,
                              Object handler) throws Exception {
        String apiKey = request.getHeader("X-API-Key");
        if (apiKey == null || apiKey.isEmpty()) {
            apiKey = request.getRemoteAddr();
        }

        String endpoint = request.getRequestURI();

        RateLimitDecision decision = rateLimiterService.isAllowed(apiKey, endpoint);

        response.setHeader("X-RateLimit-Remaining", String.valueOf(decision.remaining()));
        response.setHeader("X-RateLimit-Reset", String.valueOf(decision.resetTimeMillis()));
        response.setHeader("X-System-Health", decision.systemHealth().name());
        response.setHeader("X-Algorithm-Used", decision.algorithmUsed());

        if (!decision.allowed()) {
            response.setStatus(429);
            response.setHeader("Retry-After", 
                String.valueOf((decision.resetTimeMillis() - System.currentTimeMillis()) / 1000));
            response.getWriter().write("{\"error\":\"Rate limit exceeded\"}");
            response.setContentType("application/json");
            return false;
        }

        return true;
    }
}