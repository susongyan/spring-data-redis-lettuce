package com.example.redis.async.api;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface AsyncStreamOperations<K, HK, HV> {

    CompletionStage<String> add(K key, Map<? extends HK, ? extends HV> body);

    CompletionStage<Long> acknowledge(K key, String group, String... recordIds);

    CompletionStage<String> createGroup(K key, String group);

    CompletionStage<Boolean> destroyGroup(K key, String group);

    CompletionStage<List<StreamMessage<K, HK, HV>>> range(K key, String start, String end);

    CompletionStage<List<StreamMessage<K, HK, HV>>> read(ReadOptions options, StreamOffset<K>... streams);

    CompletionStage<List<StreamMessage<K, HK, HV>>> read(Consumer consumer, ReadOptions options, StreamOffset<K>... streams);

    CompletionStage<Long> trim(K key, long count);

    record StreamMessage<K, HK, HV>(K stream, String id, Map<HK, HV> body) {
    }

    record Consumer(String group, String name) {

        public static Consumer from(String group, String name) {
            return new Consumer(group, name);
        }
    }

    record StreamOffset<K>(K key, String offset) {

        public static <K> StreamOffset<K> latest(K key) {
            return new StreamOffset<>(key, "$");
        }

        public static <K> StreamOffset<K> lastConsumed(K key) {
            return new StreamOffset<>(key, ">");
        }

        public static <K> StreamOffset<K> from(K key, String offset) {
            return new StreamOffset<>(key, offset);
        }
    }

    final class ReadOptions {

        private final Duration block;
        private final Long count;
        private final boolean noack;

        private ReadOptions(Builder builder) {
            this.block = builder.block;
            this.count = builder.count;
            this.noack = builder.noack;
        }

        public static Builder builder() {
            return new Builder();
        }

        public Duration block() {
            return block;
        }

        public Long count() {
            return count;
        }

        public boolean noack() {
            return noack;
        }

        public static final class Builder {

            private Duration block;
            private Long count;
            private boolean noack;

            private Builder() {
            }

            public Builder block(Duration block) {
                this.block = block;
                return this;
            }

            public Builder count(long count) {
                this.count = count;
                return this;
            }

            public Builder noack() {
                this.noack = true;
                return this;
            }

            public ReadOptions build() {
                return new ReadOptions(this);
            }
        }
    }
}
