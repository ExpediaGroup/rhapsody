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
package com.expedia.rhapsody.core.grouping;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.expedia.rhapsody.api.GroupExtractor;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public abstract class UuidHashGroupExtractor<T> implements GroupExtractor<T> {

    private final HashFunction hashFunction = Hashing.murmur3_32();

    private final int modulus;

    public UuidHashGroupExtractor(int modulus) {
        this.modulus = modulus;
    }

    @Override
    public Integer apply(T t) {
        byte[] uuidBytes = extractUuidBytes(t);
        HashCode hash = hashFunction.hashBytes(uuidBytes);
        return Math.abs(hash.asInt() % modulus);
    }

    protected final byte[] extractUuidBytes(T t) {
        UUID uuid = extractUuid(t);
        ByteBuffer uuidBytes = ByteBuffer.allocate(Long.BYTES * 2);
        uuidBytes.putLong(uuid == null ? 0L : uuid.getMostSignificantBits());  // Defend against null UUIDs
        uuidBytes.putLong(uuid == null ? 0L : uuid.getLeastSignificantBits());
        return uuidBytes.array();
    }

    protected abstract UUID extractUuid(T t);
}
