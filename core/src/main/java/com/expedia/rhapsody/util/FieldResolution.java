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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class FieldResolution {

    private static final Map<Class, Map<String, Field>> FIELDS_BY_CLASS_AND_NAME = new ConcurrentHashMap<>();

    private FieldResolution() {

    }

    public static Field getAccessibleField(Class clazz, String name) {
        Field field = getField(clazz, name);
        field.setAccessible(true);
        return field;
    }

    public static Field getField(Class clazz, String name) {
        Map<String, Field> fieldsByName = FIELDS_BY_CLASS_AND_NAME.computeIfAbsent(clazz, key -> new ConcurrentHashMap<>());
        return fieldsByName.computeIfAbsent(name, key -> findField(clazz, name));
    }

    public static Map<String, Field> getAllFieldsByName(Class clazz) {
        return getAllFields(clazz).stream().collect(Collectors.toMap(Field::getName, Function.identity()));
    }

    public static Collection<Field> getAllFields(Class clazz) {
        return findAllFields(clazz);
    }

    private static Field findField(Class clazz, String name) {
        return findAllFields(clazz).stream()
            .filter(field -> Objects.equals(name, field.getName()))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(String.format("Failed to extract Field on Class: clazz=%s name=%s", clazz, name)));
    }

    private static List<Field> findAllFields(Class clazz) {
        List<Field> fields = new ArrayList<>();
        while (clazz != null) {
            for (Field field : clazz.getDeclaredFields()) {
                fields.add(field);
            }
            clazz = clazz.getSuperclass();
        }
        return fields;
    }
}
