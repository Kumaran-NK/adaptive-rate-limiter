package com.ratelimiter.adaptive_rate_limiter.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class LuaScriptLoader {

    private static final Logger log = LoggerFactory.getLogger(LuaScriptLoader.class);

    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> slidingWindowScript;
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> tokenBucketScript;

    public LuaScriptLoader() {
        this.slidingWindowScript = loadScript("lua/sliding_window.lua");
        this.tokenBucketScript = loadScript("lua/token_bucket.lua");
        log.info("Lua scripts loaded successfully");
    }

    @SuppressWarnings("rawtypes")
    private DefaultRedisScript<List> loadScript(String path) {
        try {
            ClassPathResource resource = new ClassPathResource(path);
            String scriptContent = resource.getContentAsString(StandardCharsets.UTF_8);

            DefaultRedisScript<List> script = new DefaultRedisScript<>();
            script.setScriptText(scriptContent);
            script.setResultType(List.class);

            return script;
        } catch (IOException e) {
            log.error("Failed to load Lua script: {}", path, e);
            throw new RuntimeException("Failed to load Lua script: " + path, e);
        }
    }

    public DefaultRedisScript<List> getSlidingWindowScript() {
        return slidingWindowScript;
    }

    public DefaultRedisScript<List> getTokenBucketScript() {
        return tokenBucketScript;
    }
}