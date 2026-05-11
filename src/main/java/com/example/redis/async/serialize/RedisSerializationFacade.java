package com.example.redis.async.serialize;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.lettuce.core.KeyValue;
import io.lettuce.core.ScoredValue;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.core.ZSetOperations;

public final class RedisSerializationFacade {

    private final RedisSerializer<Object> keySerializer;
    private final RedisSerializer<Object> valueSerializer;
    private final RedisSerializer<Object> hashKeySerializer;
    private final RedisSerializer<Object> hashValueSerializer;

    public RedisSerializationFacade(RedisTemplateSerializationContext context) {
        Objects.requireNonNull(context, "context must not be null");
        this.keySerializer = context.keySerializer();
        this.valueSerializer = context.valueSerializer();
        this.hashKeySerializer = context.hashKeySerializer();
        this.hashValueSerializer = context.hashValueSerializer();
    }

    public byte[] serializeKey(Object key) {
        return serialize(keySerializer, key);
    }

    public byte[] serializeValue(Object value) {
        return serialize(valueSerializer, value);
    }

    public byte[] serializeHashKey(Object hashKey) {
        return serialize(hashKeySerializer, hashKey);
    }

    public byte[] serializeHashValue(Object hashValue) {
        return serialize(hashValueSerializer, hashValue);
    }

    public byte[][] serializeKeys(Collection<?> keys) {
        return serializeCollection(keys, this::serializeKey);
    }

    public byte[][] serializeValues(Collection<?> values) {
        return serializeCollection(values, this::serializeValue);
    }

    public byte[][] serializeHashKeys(Collection<?> hashKeys) {
        return serializeCollection(hashKeys, this::serializeHashKey);
    }

    public Map<byte[], byte[]> serializeValueMap(Map<?, ?> values) {
        return serializeMap(values, this::serializeKey, this::serializeValue);
    }

    public Map<byte[], byte[]> serializeHashMap(Map<?, ?> values) {
        return serializeMap(values, this::serializeHashKey, this::serializeHashValue);
    }

    public <T> T deserializeValue(byte[] value) {
        return deserialize(valueSerializer, value);
    }

    public <T> T deserializeKey(byte[] value) {
        return deserialize(keySerializer, value);
    }

    public <T> T deserializeHashKey(byte[] value) {
        return deserialize(hashKeySerializer, value);
    }

    public <T> T deserializeHashValue(byte[] value) {
        return deserialize(hashValueSerializer, value);
    }

    public <T> List<T> deserializeValueList(List<KeyValue<byte[], byte[]>> source) {
        List<T> values = new ArrayList<>(source.size());
        for (KeyValue<byte[], byte[]> value : source) {
            values.add(deserializeValue(value == null ? null : value.getValue()));
        }
        return values;
    }

    public <T> List<T> deserializeHashValueList(List<KeyValue<byte[], byte[]>> source) {
        List<T> values = new ArrayList<>(source.size());
        for (KeyValue<byte[], byte[]> value : source) {
            values.add(deserializeHashValue(value == null ? null : value.getValue()));
        }
        return values;
    }

    public <T> List<T> deserializeValueBytes(List<byte[]> source) {
        List<T> values = new ArrayList<>(source.size());
        for (byte[] bytes : source) {
            values.add(deserializeValue(bytes));
        }
        return values;
    }

    public <T> Set<T> deserializeValueSet(Set<byte[]> source) {
        Set<T> values = new LinkedHashSet<>(source.size());
        for (byte[] bytes : source) {
            values.add(deserializeValue(bytes));
        }
        return values;
    }

    public <T> List<T> deserializeHashValues(List<byte[]> source) {
        List<T> values = new ArrayList<>(source.size());
        for (byte[] bytes : source) {
            values.add(deserializeHashValue(bytes));
        }
        return values;
    }

    public <T> Set<T> deserializeHashKeySet(List<byte[]> source) {
        Set<T> keys = new LinkedHashSet<>(source.size());
        for (byte[] bytes : source) {
            keys.add(deserializeHashKey(bytes));
        }
        return keys;
    }

    public <HK, HV> Map<HK, HV> deserializeHashEntries(Map<byte[], byte[]> source) {
        Map<HK, HV> values = new LinkedHashMap<>(source.size());
        source.forEach((rawKey, rawValue) -> values.put(deserializeHashKey(rawKey), deserializeHashValue(rawValue)));
        return values;
    }

    public <V> Set<ZSetOperations.TypedTuple<V>> deserializeTypedTuples(List<ScoredValue<byte[]>> source) {
        Set<ZSetOperations.TypedTuple<V>> tuples = new LinkedHashSet<>(source.size());
        for (ScoredValue<byte[]> scoredValue : source) {
            V value = scoredValue == null ? null : deserializeValue(scoredValue.getValue());
            Double score = scoredValue == null ? null : scoredValue.getScore();
            tuples.add(ZSetOperations.TypedTuple.of(value, score));
        }
        return tuples;
    }

    public ScoredValue<byte[]> serializeTypedTuple(ZSetOperations.TypedTuple<?> tuple) {
        Object value = Objects.requireNonNull(tuple, "tuple must not be null").getValue();
        Double score = tuple.getScore();
        if (score == null) {
            throw new IllegalArgumentException("tuple score must not be null");
        }
        return ScoredValue.just(score, serializeValue(value));
    }

    public List<ScoredValue<byte[]>> serializeTypedTuples(Collection<? extends ZSetOperations.TypedTuple<?>> tuples) {
        List<ScoredValue<byte[]>> values = new ArrayList<>(tuples.size());
        for (ZSetOperations.TypedTuple<?> tuple : tuples) {
            values.add(serializeTypedTuple(tuple));
        }
        return values;
    }

    public byte[] serializeString(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    private byte[][] serializeCollection(Collection<?> values, ThrowingSerializer serializer) {
        byte[][] result = new byte[values.size()][];
        int index = 0;
        for (Object value : values) {
            result[index++] = serializer.serialize(value);
        }
        return result;
    }

    private Map<byte[], byte[]> serializeMap(
            Map<?, ?> source,
            ThrowingSerializer keyWriter,
            ThrowingSerializer valueWriter
    ) {
        Map<byte[], byte[]> result = new LinkedHashMap<>(source.size());
        source.forEach((key, value) -> result.put(keyWriter.serialize(key), valueWriter.serialize(value)));
        return result;
    }

    private byte[] serialize(RedisSerializer<Object> serializer, Object value) {
        return serializer.serialize(value);
    }

    @SuppressWarnings("unchecked")
    private <T> T deserialize(RedisSerializer<Object> serializer, byte[] value) {
        return (T) serializer.deserialize(value);
    }

    @FunctionalInterface
    private interface ThrowingSerializer {
        byte[] serialize(Object value);
    }
}
