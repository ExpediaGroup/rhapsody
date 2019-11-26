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
package com.expedia.rhapsody.kafka.interceptor;

public final class ExceptionTimestamp {

    private final Exception exception;

    private final Long timestamp;

    private final String metadata;

    public ExceptionTimestamp(Exception exception, Long timestamp, String metadata) {
        this.exception = exception;
        this.timestamp = timestamp;
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "ExceptionTimestamp{" +
            "exception=" + exception +
            ", timestamp=" + timestamp +
            ", metadata='" + metadata + '\'' +
            '}';
    }

    public Exception getException() {
        return exception;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getMetadata() {
        return metadata;
    }
}
