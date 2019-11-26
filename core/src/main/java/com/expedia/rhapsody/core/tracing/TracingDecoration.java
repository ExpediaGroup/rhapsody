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
package com.expedia.rhapsody.core.tracing;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import io.opentracing.Span;
import io.opentracing.log.Fields;
import io.opentracing.tag.Tags;

public final class TracingDecoration {

    private TracingDecoration() {

    }

    public static Runnable decorateAcknowledger(Span span, Runnable acknowledger) {
        return () -> {
            span.log("acknowledged");
            span.setTag(Tags.ERROR.getKey(), false);
            span.finish();
            acknowledger.run();
        };
    }

    public static Consumer<? super Throwable> decorateNacknowledger(Span span, Consumer<? super Throwable> nacknowledger) {
        return error -> {
            span.log("nacknowledged");
            span.setTag(Tags.ERROR.getKey(), true);
            span.log(createErrorFields(error));
            span.finish();
            nacknowledger.accept(error);
        };
    }

    private static Map<String, Object> createErrorFields(Throwable error) {
        Map<String, Object> fields = new HashMap<>();
        fields.put(Fields.EVENT, "error");
        fields.put(Fields.ERROR_KIND, error.getClass().getSimpleName());
        fields.put(Fields.ERROR_OBJECT, error);
        return fields;
    }
}
