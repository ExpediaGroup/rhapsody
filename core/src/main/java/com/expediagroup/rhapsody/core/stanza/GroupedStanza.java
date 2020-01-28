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

import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.api.SubscriberFactory;
import com.expediagroup.rhapsody.core.transformer.ListeningTransformer;
import com.expediagroup.rhapsody.core.transformer.MetricsConfig;
import com.expediagroup.rhapsody.core.transformer.MetricsTransformer;
import com.expediagroup.rhapsody.core.transformer.SchedulingTransformer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * A Stanza that provides a framework for "grouped" streaming, typically used to implement parallel
 * processing of data.
 *
 * @param <C> The type of StanzaConfig used to configure this Stanza
 * @param <U> The type of Upstream messages being consumed
 * @param <V> The type of Values that will be scheduled for processing
 * @param <R> The type of Results produced by processing
 */
public abstract class GroupedStanza<C extends StanzaConfig, U, V, R> extends Stanza<C> {

    protected final Disposable startDisposable(C config) {
        MeterRegistry meterRegistry = config.meterRegistry().orElse(Metrics.globalRegistry);
        MetricsConfig metricsConfig = config.metrics();
        MetricsTransformer<U> inboundMetrics = new MetricsTransformer<>(metricsConfig.withTags(Tags.of("flow", "inbound")), meterRegistry);
        MetricsTransformer<R> outboundMetrics = new MetricsTransformer<>(metricsConfig.withTags(Tags.of("flow", "outbound")), meterRegistry);

        Function<? super Publisher<U>, ? extends Publisher<V>> prescheduler = buildPrescheduler(config);

        SchedulingTransformer<V> schedulingTransformer = new SchedulingTransformer<>(config.scheduling(), config.name());

        Function<? super Publisher<V>, ? extends Publisher<R>> transformer = buildTransformer(config);

        SubscriberFactory<R> subscriberFactory = buildSubscriberFactory(config);

        return Flux.from(buildGroupPublisher(config))
            .transform(new ListeningTransformer<>(config.streamListeners()))
            .subscribe(groupedPublisher -> Flux.from(groupedPublisher)
                .transform(inboundMetrics)
                .transform(prescheduler)
                .transform(schedulingTransformer)
                .transform(transformer)
                .transform(outboundMetrics)
                .subscribe(subscriberFactory.create()));
    }

    protected abstract Publisher<? extends Publisher<U>> buildGroupPublisher(C config);

    protected abstract Function<? super Publisher<U>, ? extends Publisher<V>> buildPrescheduler(C config);

    protected abstract Function<? super Publisher<V>, ? extends Publisher<R>> buildTransformer(C config);

    protected abstract SubscriberFactory<R> buildSubscriberFactory(C config);
}