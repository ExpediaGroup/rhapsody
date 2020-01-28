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
package com.expediagroup.rhapsody.core.transformer;

import java.util.function.Function;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;

public final class QosTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private final QosConfig config;

    private final String name;

    public QosTransformer(QosConfig config, String name) {
        this.config = config;
        this.name = name;
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        return Flux.from(publisher)
            .transformDeferred(new ActivityEnforcingTransformer<>(config.activityEnforcement(name)))
            .transform(new ResubscribingTransformer<>(config.resubscription(name)))
            .transform(new RateLimitingTransformer<>(config.rateLimiting(name)));
    }
}
