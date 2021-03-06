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
package com.expediagroup.rhapsody.core.grouping;

import java.util.function.Function;

import com.expediagroup.rhapsody.api.Acknowledgeable;

public class ComposedStringHashGroupExtractor<T> extends StringHashGroupExtractor<T> {

    private final Function<? super T, String> stringExtractor;

    public ComposedStringHashGroupExtractor(int modulus, Function<? super T, String> stringExtractor) {
        super(modulus);
        this.stringExtractor = stringExtractor;
    }

    public static <T> ComposedStringHashGroupExtractor<Acknowledgeable<T>>
    acknowledgeable(int modulus, Function<? super T, String> stringExtractor) {
        return new ComposedStringHashGroupExtractor<>(modulus, stringExtractor.compose(Acknowledgeable::get));
    }

    @Override
    protected String extractString(T t) {
        return stringExtractor.apply(t);
    }
}
