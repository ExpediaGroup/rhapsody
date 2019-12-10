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
package com.expediagroup.rhapsody.core.metrics;

import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class AuditedMetricRegistryTest {

    private final AuditedMetricRegistry<TestAuditor, Integer, Integer> registry = new AuditedMetricRegistry<>(TestAuditor::evaluate, Integer.MIN_VALUE);

    @Test
    public void auditorCanBeRegistered() {
        MeterKey key = new MeterKey("key", Collections.emptyMap());

        registry.register(key, new TestAuditor(1), 1);

        assertEquals(1, registry.evaluate(key).intValue());
    }

    @Test
    public void auditedMeterKeysAreInterned() {
        MeterKey first = new MeterKey("key", Collections.emptyMap());
        MeterKey second = new MeterKey(first.getName(), first.getTags());

        TestAuditor auditor = new TestAuditor(1);

        assertSame(first, registry.register(first, auditor, 1));
        assertSame(first, registry.register(second, auditor, 1));

        registry.unregister(auditor);

        assertSame(first, registry.register(second, auditor, 1));
    }

    @Test
    public void auditorCanBeUnregistered() {
        MeterKey key1 = new MeterKey("key1", Collections.emptyMap());
        MeterKey key2 = new MeterKey("key2", Collections.emptyMap());
        MeterKey key3 = new MeterKey("key3", Collections.emptyMap());

        TestAuditor auditor = new TestAuditor(1);

        registry.register(key1, auditor, 1);
        registry.register(key2, auditor, 1);
        registry.register(key3, new TestAuditor(1), 1);

        registry.unregister(auditor);

        assertEquals(Integer.MIN_VALUE, registry.evaluate(key1).intValue());
        assertEquals(Integer.MIN_VALUE, registry.evaluate(key2).intValue());
        assertEquals(1, registry.evaluate(key3).intValue());
    }

    @Test
    public void keyAndAuditorCanBeUnregistered() {
        MeterKey key1 = new MeterKey("key1", Collections.emptyMap());
        MeterKey key2 = new MeterKey("key2", Collections.emptyMap());

        TestAuditor auditor = new TestAuditor(1);

        registry.register(key1, auditor, 1);
        registry.register(key2, auditor, 1);

        registry.unregister(key1, auditor);

        assertEquals(Integer.MIN_VALUE, registry.evaluate(key1).intValue());
        assertEquals(1, registry.evaluate(key2).intValue());
    }

    @Test
    public void keyCanBeRegisteredWithNewAuditor() {
        MeterKey key = new MeterKey("key1", Collections.emptyMap());

        registry.register(key, new TestAuditor(1), 1);
        registry.register(key, new TestAuditor(2), 1);

        assertEquals(2, registry.evaluate(key).intValue());
    }

    @Test
    public void unregisteringOldAuditorDoesNotUnregister() {
        MeterKey key = new MeterKey("key1", Collections.emptyMap());

        TestAuditor oldAuditor = new TestAuditor(1);
        registry.register(key, oldAuditor, 1);

        registry.register(key, new TestAuditor(2), 1);
        registry.unregister(key, oldAuditor);

        assertEquals(2, registry.evaluate(key).intValue());
    }

    private static final class TestAuditor {

        private final int multiplier;

        private TestAuditor(int multiplier) {
            this.multiplier = multiplier;
        }

        public int evaluate(Integer integer) {
            return integer * multiplier;
        }
    }
}