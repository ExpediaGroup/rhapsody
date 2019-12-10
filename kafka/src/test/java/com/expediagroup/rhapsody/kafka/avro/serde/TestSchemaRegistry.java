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
package com.expediagroup.rhapsody.kafka.avro.serde;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.UnaryOperator;

import org.apache.avro.Schema;

public final class TestSchemaRegistry {

    private static final NavigableMap<Integer, Schema> SCHEMAS_BY_ID = new ConcurrentSkipListMap<>();

    private UnaryOperator<Schema> registrationHook = UnaryOperator.identity();

    public int register(String subject, Schema schema) {
        synchronized (SCHEMAS_BY_ID) {
            int id = SCHEMAS_BY_ID.isEmpty() ? 1 : SCHEMAS_BY_ID.lastKey() + 1;
            SCHEMAS_BY_ID.put(id, registrationHook.apply(schema));
            return id;
        }
    }

    public Schema getByID(int id) {
        return SCHEMAS_BY_ID.get(id);
    }

    public void setRegistrationHook(UnaryOperator<Schema> registryHook) {
        this.registrationHook = registryHook;
    }
}
