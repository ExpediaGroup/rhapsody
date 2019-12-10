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
package com.expediagroup.rhapsody.util;

import java.lang.reflect.Field;

public final class ValueResolution {

    private ValueResolution() {

    }

    public static Object getFieldValue(Object target, Field field) {
        try {
            ensureFieldAccessibility(field);
            return field.get(target);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Failed to get Field Value: field=%s target=%s e=%s", field, target, e));
        }
    }

    private static void ensureFieldAccessibility(Field field) {
        if (!field.isAccessible()) {
            field.setAccessible(true);
        }
    }
}
