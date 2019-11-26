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
package com.expedia.rhapsody.core.adapter;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expedia.rhapsody.api.AbstractAcknowledgeable;
import com.expedia.rhapsody.api.AcknowledgeableFactory;
import com.expedia.rhapsody.api.ComposedAcknowledgeable;

public final class LoggingAcknowledgeable<T> extends AbstractAcknowledgeable<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingAcknowledgeable.class);

    private final T t;

    LoggingAcknowledgeable(T t) {
        this.t = t;
    }

    @Override
    public T get() {
        return t;
    }

    @Override
    public Runnable getAcknowledger() {
        return () -> LOGGER.info("Acknowledged: t=" + t);
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return error -> LOGGER.warn("Nacknowledged: t=" + t, error);
    }

    @Override
    protected <R> AcknowledgeableFactory<R> createPropagator() {
        return ComposedAcknowledgeable::new;
    }
}
