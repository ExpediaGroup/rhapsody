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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.core.transformer.ActivityEnforcingTransformer;
import com.expediagroup.rhapsody.core.transformer.ListeningTransformer;
import com.expediagroup.rhapsody.core.transformer.RateLimitingTransformer;
import com.expediagroup.rhapsody.core.transformer.ResubscribingTransformer;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * A Stanza is a stream process that can be started and stopped
 *
 * @param <C> The type modeling configuration information for this Stanza
 */
public abstract class Stanza<C extends StanzaConfig> {

    private static final Disposable EMPTY = () -> {};

    private static final Disposable STARTING = () -> {};

    private final AtomicReference<Disposable> disposableReference = new AtomicReference<>(EMPTY);

    public final void start(C config) {
        synchronized (disposableReference) {
            if (!disposableReference.compareAndSet(EMPTY, STARTING) && !disposableReference.get().isDisposed()) {
                throw new UnsupportedOperationException("Cannot start Stanza that is already starting/started");
            }
            disposableReference.set(startDisposable(config));
        }
    }

    public final void stop() {
        synchronized (disposableReference) {
            disposableReference.getAndSet(EMPTY).dispose();
        }
    }

    protected abstract Disposable startDisposable(C config);

    protected <T> UnaryOperator<Publisher<T>> buildQosTransformer(C config) {
        return publisher -> Flux.from(publisher)
            .transform(new ListeningTransformer<>(config.streamListeners()))
            .transformDeferred(new ActivityEnforcingTransformer<>(config.activityEnforcement()))
            .transform(new ResubscribingTransformer<>(config.resubscription()))
            .transform(new RateLimitingTransformer<>(config.rateLimiting()));
    }
}
