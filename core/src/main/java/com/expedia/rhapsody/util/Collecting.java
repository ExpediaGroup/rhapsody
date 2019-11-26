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
package com.expedia.rhapsody.util;

import java.util.Arrays;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class Collecting {

    private Collecting() {

    }

    public static <T, K, V, M extends Map<K, V>> Map<K, V> toMap(T[] array,
        Function<? super T, ? extends K> keyMapper,
        Function<? super T, ? extends V> valueMapper,
        Supplier<M> mapSupplier) {
        return Arrays.stream(array).collect(Collectors.toMap(keyMapper, valueMapper, throwingMerger(), mapSupplier));
    }

    public static <T> BinaryOperator<T> throwingMerger() {
        return (t1, t2) -> {
            throw new IllegalStateException(String.format("Duplicate key for values: t1=%s t2=%s", t1, t2));
        };
    }
}
