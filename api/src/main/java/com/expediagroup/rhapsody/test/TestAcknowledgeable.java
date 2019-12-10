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
package com.expediagroup.rhapsody.test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.expediagroup.rhapsody.api.AbstractAcknowledgeable;
import com.expediagroup.rhapsody.api.AcknowledgeableFactory;
import com.expediagroup.rhapsody.api.ComposedAcknowledgeable;

public final class TestAcknowledgeable extends AbstractAcknowledgeable<String> {

    private final String data;

    private final Runnable acknowledgerHook;

    private final AtomicBoolean acknowledged = new AtomicBoolean(false);

    private final AtomicReference<Throwable> nacknowledged = new AtomicReference<>();

    public TestAcknowledgeable(String data) {
        this(data, () -> {});
    }

    public TestAcknowledgeable(String data, Runnable acknowledgerHook) {
        this.data = data;
        this.acknowledgerHook = acknowledgerHook;
    }

    @Override
    public String get() {
        return data;
    }

    @Override
    public Runnable getAcknowledger() {
        return () -> {
            acknowledgerHook.run();
            acknowledged.set(true);
        };
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return nacknowledged::set;
    }

    public int length() {
        return data.length();
    }

    public boolean isAcknowledged() {
        return acknowledged.get();
    }

    public boolean isNacknowledged() {
        return getError().isPresent();
    }

    public Optional<Throwable> getError() {
        return Optional.ofNullable(nacknowledged.get());
    }

    @Override
    protected <R> AcknowledgeableFactory<R> createPropagator() {
        return ComposedAcknowledgeable::new;
    }
}
