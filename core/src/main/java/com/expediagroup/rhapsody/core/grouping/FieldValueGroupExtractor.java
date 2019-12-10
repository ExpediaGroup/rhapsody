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

import java.lang.reflect.Field;

import com.expediagroup.rhapsody.api.GroupExtractor;
import com.expediagroup.rhapsody.util.FieldResolution;
import com.expediagroup.rhapsody.util.ValueResolution;

public class FieldValueGroupExtractor<T> implements GroupExtractor<T> {

    private final String fieldName;

    public FieldValueGroupExtractor(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Object apply(T t) {
        Field field = FieldResolution.getField(t.getClass(), fieldName);
        return ValueResolution.getFieldValue(t, field);
    }
}
