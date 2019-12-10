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
package com.expediagroup.rhapsody.rabbitmq.message;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

public final class RabbitMessageExtraction {

    private static final DateFormat ISO_8601_DATE_FORMAT = createIso8601DateFormat();

    private RabbitMessageExtraction() {

    }

    public static Map<String, String> extractStringifiedHeaders(RabbitMessage rabbitMessage) {
        Map<String, Object> headers = rabbitMessage.getProperties().getHeaders();
        return headers == null ? Collections.emptyMap() : stringifyHeaderValue(headers);
    }

    private static Map<String, String> stringifyHeaderValue(Map<String, Object> headers) {
        return headers.entrySet().stream()
            .filter(entry -> isStringifiable(entry.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> deserializeHeaderValue(entry.getValue())));
    }

    private static boolean isStringifiable(Object value) {
        return value != null && !Collection.class.isInstance(value) && !Map.class.isInstance(value);
    }

    private static String deserializeHeaderValue(Object value) {
        if (value instanceof BigDecimal) {
            return BigDecimal.class.cast(value).toPlainString();
        } else if (value instanceof Date) {
            return ISO_8601_DATE_FORMAT.format(Date.class.cast(value));
        } else if (value instanceof byte[]) {
            return new String (byte[].class.cast(value), StandardCharsets.UTF_8);
        } else {
            return Objects.toString(value);
        }
    }

    private static DateFormat createIso8601DateFormat() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat;
    }
}
