package com.ratelimiter.adaptive_rate_limiter.service.strategy.support;

import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.ratelimiter.adaptive_rate_limiter.redis.LuaScriptLoader;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.GcraStrategy;
import com.ratelimiter.adaptive_rate_limiter.service.strategy.SlidingWindowStrategy;

/**
 * Shared real-Redis test fixture for GCRA vs. Sliding Window comparison
 * tests. Test support only -- not part of production code.
 *
 * <p>Boots a single, shared (per-JVM) Redis container via Testcontainers
 * and exposes ready-to-use {@link SlidingWindowStrategy} / {@link GcraStrategy}
 * instances backed by the real Lua scripts running against it, so every
 * test in this suite exercises the exact same atomic Redis code path
 * production would use.
 */
@Testcontainers
public abstract class RedisStrategyTestBase {

    protected static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    protected RedisTemplate<String, String> redisTemplate;
    protected SlidingWindowStrategy slidingWindowStrategy;
    protected GcraStrategy gcraStrategy;

    @BeforeAll
    static void startContainer() {
        if (!REDIS.isRunning()) {
            REDIS.start();
        }
    }

    @BeforeEach
    void setUpRedis() {
        redisTemplate = newRedisTemplate();

        LuaScriptLoader scriptLoader = new LuaScriptLoader();
        slidingWindowStrategy = new SlidingWindowStrategy(redisTemplate, scriptLoader);
        gcraStrategy = new GcraStrategy(redisTemplate, scriptLoader);
    }

    @AfterEach
    void flushRedis() {
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    /**
     * Builds a fresh RedisTemplate pointed at the shared container. Exposed
     * so multi-instance tests can simulate several independent application
     * pods (each with their own connection factory) talking to the same
     * Redis, rather than all sharing one Java-side connection pool.
     */
    protected RedisTemplate<String, String> newRedisTemplate() {
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379)));
        connectionFactory.afterPropertiesSet();

        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    protected String uniqueKey(String prefix) {
        return prefix + ":" + UUID.randomUUID();
    }
}