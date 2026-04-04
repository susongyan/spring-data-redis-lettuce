package com.example.redis.async.config;

import com.example.redis.async.api.AsyncRedisTemplate;
import com.example.redis.async.connection.AsyncRedisConnectionProvider;
import com.example.redis.async.connection.LettuceAsyncRedisConnectionProvider;
import com.example.redis.async.exception.AsyncRedisExceptionTranslator;
import com.example.redis.async.executor.AsyncCommandExecutor;
import com.example.redis.async.executor.LettuceAsyncCommandExecutor;
import com.example.redis.async.metrics.AsyncRedisMetricsRecorder;
import com.example.redis.async.metrics.MicrometerAsyncRedisMetricsRecorder;
import com.example.redis.async.metrics.NoopAsyncRedisMetricsRecorder;
import com.example.redis.async.support.AsyncRedisTemplateOptions;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

@AutoConfiguration(after = RedisAutoConfiguration.class)
public class AsyncRedisConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public AsyncRedisTemplateOptions asyncRedisTemplateOptions() {
        return AsyncRedisTemplateOptions.builder().build();
    }

    @Bean
    @ConditionalOnMissingBean
    public AsyncRedisMetricsRecorder asyncRedisMetricsRecorder(ObjectProvider<MeterRegistry> meterRegistryProvider) {
        MeterRegistry meterRegistry = meterRegistryProvider.getIfAvailable();
        if (meterRegistry == null) {
            return NoopAsyncRedisMetricsRecorder.INSTANCE;
        }
        return new MicrometerAsyncRedisMetricsRecorder(meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public AsyncRedisExceptionTranslator asyncRedisExceptionTranslator() {
        return new AsyncRedisExceptionTranslator();
    }

    @Bean
    @ConditionalOnMissingBean
    public AsyncRedisConnectionProvider asyncRedisConnectionProvider(LettuceConnectionFactory connectionFactory) {
        return new LettuceAsyncRedisConnectionProvider(connectionFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    public AsyncCommandExecutor asyncCommandExecutor(
            AsyncRedisExceptionTranslator exceptionTranslator,
            AsyncRedisMetricsRecorder metricsRecorder,
            AsyncRedisTemplateOptions options
    ) {
        return new LettuceAsyncCommandExecutor(exceptionTranslator, metricsRecorder, options);
    }

    @Bean
    @ConditionalOnMissingBean
    public AsyncRedisTemplateFactory asyncRedisTemplateFactory(
            AsyncRedisConnectionProvider connectionProvider,
            AsyncCommandExecutor commandExecutor
    ) {
        return new AsyncRedisTemplateFactory(connectionProvider, commandExecutor);
    }

    @Bean("asyncRedisTemplate")
    @ConditionalOnBean(name = "redisTemplate")
    @ConditionalOnMissingBean(name = "asyncRedisTemplate")
    @SuppressWarnings("unchecked")
    public AsyncRedisTemplate<Object, Object> asyncRedisTemplate(
            AsyncRedisTemplateFactory factory,
            @Qualifier("redisTemplate") RedisTemplate<?, ?> redisTemplate
    ) {
        return factory.create((RedisTemplate<Object, Object>) redisTemplate);
    }

    @Bean("asyncStringRedisTemplate")
    @ConditionalOnBean(StringRedisTemplate.class)
    @ConditionalOnMissingBean(name = "asyncStringRedisTemplate")
    public AsyncRedisTemplate<String, String> asyncStringRedisTemplate(
            AsyncRedisTemplateFactory factory,
            StringRedisTemplate stringRedisTemplate
    ) {
        return factory.create(stringRedisTemplate);
    }
}
