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

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A Factory for creating Acknowledgeables of a type of data item. The simplest implementation of
 * this factory is {@link ComposedAcknowledgeable ComposedAcknowledgeable::new}.
 *
 * @param <T> The type of data items for which this factory creates Acknowledgeables
 */
public interface AcknowledgeableFactory<T> {

    /**
     * Functional convenience method that inverts a List of Acknowledgeables in to an
     * Acknowledgeable List of data items
     *
     * @param listToAcknowledgeableFactory Produce Factory from List of Acknowledgeables
     * @param <T> The type of data items in the List of Acknowledgeables
     * @return A Function that inverts a List of Acknowledgeables to Acknowledgeable List
     */
    static <T> Function<List<Acknowledgeable<T>>, Acknowledgeable<List<T>>>
    listInverting(Function<? super List<Acknowledgeable<T>>, ? extends AcknowledgeableFactory<List<T>>> listToAcknowledgeableFactory) {
        return list -> {
            List<T> values = list.stream().map(Acknowledgeable::get).collect(Collectors.toList());
            Collection<Runnable> acknowledgers = list.stream().map(Acknowledgeable::getAcknowledger).collect(Collectors.toList());
            Collection<Consumer<? super Throwable>> nacknowledgers = list.stream().map(Acknowledgeable::getNacknowledger).collect(Collectors.toList());
            return listToAcknowledgeableFactory.apply(list)
                .create(values, () -> acknowledgers.forEach(Runnable::run), error -> nacknowledgers.forEach(nacknowledger -> nacknowledger.accept(error)));
        };
    }

    /**
     * Create a new Acknowledgable from the supplied data item, Acknowledger, and Nacknowledger
     */
    Acknowledgeable<T> create(T t, Runnable acknowledger, Consumer<? super Throwable> nacknowledger);
}
