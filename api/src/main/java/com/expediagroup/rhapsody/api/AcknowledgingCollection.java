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

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

final class AcknowledgingCollection<T> extends AbstractCollection<Acknowledgeable<T>> {

    private final Collection<T> collection;

    private final Collection<T> unacknowledged;

    private final Runnable acknowledger;

    private final Consumer<? super Throwable> nacknowledger;

    private final AcknowledgeableFactory<T> factory;

    public AcknowledgingCollection(Collection<T> collection, Runnable acknowledger, Consumer<? super Throwable> nacknowledger, AcknowledgeableFactory<T> factory) {
        this.collection = collection;
        this.unacknowledged = collection.stream().collect(Collectors.toCollection(() -> Collections.newSetFromMap(new IdentityHashMap<>(collection.size()))));
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
        this.factory = factory;
    }

    @Override
    public String toString() {
        return "AcknowledgingCollection(" + Objects.toString(collection) + ")";
    }

    @Override
    public Iterator<Acknowledgeable<T>> iterator() {
        Iterator<T> iterator = collection.iterator();
        return new Iterator<Acknowledgeable<T>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Acknowledgeable<T> next() {
                return wrap(iterator.next());
            }
        };
    }

    @Override
    public int size() {
        return collection.size();
    }

    private Acknowledgeable<T> wrap(T value) {
        return factory.create(value, () -> {
            synchronized (unacknowledged) {
                if (unacknowledged.remove(value) && unacknowledged.isEmpty()) {
                    acknowledger.run();
                }
            }
        }, error -> {
            synchronized (unacknowledged) {
                if (unacknowledged.contains(value)) {
                    unacknowledged.clear();
                    nacknowledger.accept(error);
                }
            }
        });
    }
}
