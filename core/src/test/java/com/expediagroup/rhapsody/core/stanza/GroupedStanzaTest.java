/**
 * Copyright 2019 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.rhapsody.core.stanza;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;
import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.api.SubscriberFactory;
import com.expediagroup.rhapsody.core.adapter.Adapters;
import com.expediagroup.rhapsody.core.transformer.MetricsConfig;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.core.publisher.Flux;

import static org.junit.Assert.assertEquals;

public class GroupedStanzaTest {

    @Test
    public void groupedStanzaHasAllResourcesWired() {
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        Metrics.addRegistry(meterRegistry);

        List<String> results = new ArrayList<>();

        SimpleGroupedStanzaConfig config = new SimpleGroupedStanzaConfig(meterRegistry, results::add);

        // We know that this will complete synchronously due to "immediate" scheduling
        new SimpleGroupedStanza().start(config);

        assertEquals(3, results.size());
        assertEquals("B,C", results.get(0));
        assertEquals("A,E", results.get(1));
        assertEquals("D", results.get(2));

        assertEquals(5D, meterRegistry.find("simple.items").tag("flow", "inbound").counter().count(), 0D);
        assertEquals(3D, meterRegistry.find("simple.items").tag("flow", "outbound").counter().count(), 0D);
    }

    private static final class SimpleGroupedStanza extends GroupedStanza<SimpleGroupedStanzaConfig, Character, List<Character>, String> {

        @Override
        protected Publisher<? extends Publisher<Character>> buildGroupPublisher(SimpleGroupedStanzaConfig config) {
            return Flux.just('A', 'B', 'C', 'D', 'E').groupBy(Arrays.asList('A', 'E', 'I', 'O', 'U')::contains);
        }

        @Override
        protected Function<? super Publisher<Character>, ? extends Publisher<List<Character>>> buildPrescheduler(SimpleGroupedStanzaConfig config) {
            return publisher -> Flux.from(publisher).buffer(2);
        }

        @Override
        protected Function<? super Publisher<List<Character>>, ? extends Publisher<String>> buildTransformer(SimpleGroupedStanzaConfig config) {
            return publisher -> Flux.from(publisher).map(list -> list.stream().map(Object::toString).collect(Collectors.joining(",")));
        }

        @Override
        protected SubscriberFactory<String> buildSubscriberFactory(SimpleGroupedStanzaConfig config) {
            return () -> Adapters.toSubscriber(config.getResultConsumer());
        }
    }

    private static final class SimpleGroupedStanzaConfig implements StanzaConfig {

        private final MeterRegistry meterRegistry;

        private final Consumer<String> resultConsumer;

        private SimpleGroupedStanzaConfig(MeterRegistry meterRegistry, Consumer<String> resultConsumer) {
            this.meterRegistry = meterRegistry;
            this.resultConsumer = resultConsumer;
        }

        @Override
        public Optional<MeterRegistry> meterRegistry() {
            return Optional.of(meterRegistry);
        }

        @Override
        public MetricsConfig metrics() {
            return new MetricsConfig(name(), Tags.empty());
        }

        @Override
        public String name() {
            return "simple";
        }

        public Consumer<String> getResultConsumer() {
            return resultConsumer;
        }
    }
}