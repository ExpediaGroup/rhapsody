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
package com.expediagroup.rhapsody.test;

import java.util.Map;
import java.util.Objects;

import com.expediagroup.rhapsody.api.Headed;
import com.expediagroup.rhapsody.api.Header;

public final class TestHeaded implements Headed {

    private Map<String, String> headers;

    private String data;

    public TestHeaded(Map<String, String> headers, String data) {
        this.headers = headers;
        this.data = data;
    }

    protected TestHeaded() {

    }

    @Override
    public Header header() {
        return Header.fromMap(headers);
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TestHeaded that = (TestHeaded) o;
        return Objects.equals(headers, that.headers) &&
            Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(headers, data);
    }
}
