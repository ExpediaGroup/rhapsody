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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

public final class ResubscribingTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResubscribingTransformer.class);

    private final ResubscriptionConfig config;

    public ResubscribingTransformer(ResubscriptionConfig config) {
        this.config = config;
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        return config.isEnabled() ? applyResubscription(publisher) : publisher;
    }

    private Flux<T> applyResubscription(Publisher<T> publisher) {
        return Flux.from(publisher).retryWhen(Retry.from(this::scheduleResubscription));
    }

    private Flux<?> scheduleResubscription(Flux<Retry.RetrySignal> signals) {
        return signals.map(Retry.RetrySignal::failure)
            .doOnNext(error -> LOGGER.warn("An Error has occurred! Scheduling resubscription: name={} delay={}", config.getName(), config.getDelay(), error))
            .delayElements(config.getDelay())
            .doOnNext(error -> LOGGER.info("Attempting resubscription from Error: name={}", config.getName(), error));
    }
}
