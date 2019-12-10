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
package com.expediagroup.rhapsody.api;

import java.util.function.Consumer;

/**
 * The most simple implementation of Acknowledgable. This implementation is fully composed of its
 * data item, acknowledger, and nacknowledger. Propagation includes no extra information.
 *
 * @param <T> The type of data item
 */
public final class ComposedAcknowledgeable<T> extends AbstractAcknowledgeable<T> {

    private final T t;

    private final Runnable acknowledger;

    private final Consumer<? super Throwable> nacknowledger;

    public ComposedAcknowledgeable(T t, Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        this.t = t;
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
    }

    @Override
    public T get() {
        return t;
    }

    @Override
    public Runnable getAcknowledger() {
        return acknowledger;
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return nacknowledger;
    }

    @Override
    protected <R> AcknowledgeableFactory<R> createPropagator() {
        return ComposedAcknowledgeable::new;
    }
}
