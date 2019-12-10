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
package com.expediagroup.rhapsody.kafka.serde;

import java.util.Objects;

public class TestData {

    private String data1;

    private String data2;

    public static TestData create() {
        TestData data = new TestData();
        data.setData1("DATA 1");
        data.setData2("DATA 2");
        return data;
    }

    public String getData1() {
        return data1;
    }

    public void setData1(String data1) {
        this.data1 = data1;
    }

    public String getData2() {
        return data2;
    }

    public void setData2(String data2) {
        this.data2 = data2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TestData testData = (TestData) o;
        return Objects.equals(data1, testData.data1) &&
            Objects.equals(data2, testData.data2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data1, data2);
    }
}
