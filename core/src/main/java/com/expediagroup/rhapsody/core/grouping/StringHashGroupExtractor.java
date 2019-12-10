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

import java.util.Objects;

import com.expediagroup.rhapsody.api.GroupExtractor;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public abstract class StringHashGroupExtractor<T> implements GroupExtractor<T> {

    private final HashFunction hashFunction = Hashing.murmur3_32();

    private final int modulus;

    public StringHashGroupExtractor(int modulus) {
        this.modulus = modulus;
    }

    @Override
    public Object apply(T t) {
        String string = Objects.toString(extractString(t));
        HashCode hash = hashFunction.hashUnencodedChars(string);
        return Math.abs(hash.asInt() % modulus);
    }

    protected abstract String extractString(T t);
}
