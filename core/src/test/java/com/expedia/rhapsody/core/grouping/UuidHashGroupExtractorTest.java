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

import java.util.UUID;

import org.junit.Test;

import com.expedia.rhapsody.api.GroupExtractor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class UuidHashGroupExtractorTest {

    @Test
    public void uuidsAreAppropriatelyHashed() {
        UUID uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000000");
        UUID uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000001");

        GroupExtractor<UUID> groupExtractor = new TestUuidHashGroupExtractor(2);

        assertEquals(groupExtractor.apply(uuid1), groupExtractor.apply(uuid1));
        assertNotEquals(groupExtractor.apply(uuid1), groupExtractor.apply(uuid2));
    }

    private static final class TestUuidHashGroupExtractor extends UuidHashGroupExtractor<UUID> {

        public TestUuidHashGroupExtractor(int modulus) {
            super(modulus);
        }

        @Override
        protected UUID extractUuid(UUID uuid) {
            return uuid;
        }
    }
}