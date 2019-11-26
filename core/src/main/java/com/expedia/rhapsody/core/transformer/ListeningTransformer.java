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
package com.expedia.rhapsody.core.transformer;

import java.util.Collection;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.expedia.rhapsody.api.StreamListener;
import com.expedia.rhapsody.api.StreamState;

import reactor.core.publisher.Flux;

public final class ListeningTransformer<T> implements Function<Publisher<T>, Flux<T>> {

    private final Collection<StreamListener> listeners;

    public ListeningTransformer(Collection<StreamListener> listeners) {
        this.listeners = listeners;
    }

    @Override
    public Flux<T> apply(Publisher<T> publisher) {
        return Flux.from(publisher)
            .doOnNext(this::notifyListenersOfNext)
            .doOnSubscribe(subscription -> notifyListenersOfState(StreamState.SUBSCRIBED))
            .doOnCancel(() -> notifyListenersOfState(StreamState.CANCELED))
            .doOnError(error -> notifyListenersOfState(StreamState.ERRORED, error.toString()))
            .doOnComplete(() -> notifyListenersOfState(StreamState.COMPLETED));
    }

    private void notifyListenersOfNext(Object object) {
        listeners.forEach(listener -> listener.next(object));
    }

    private void notifyListenersOfState(StreamState state) {
        notifyListenersOfState(state, state.name());
    }

    private void notifyListenersOfState(StreamState state, String metadata) {
        listeners.forEach(listener -> listener.stateChanged(state, metadata));
    }
}
