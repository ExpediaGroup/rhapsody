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

import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

public class TestDataWithProblematicTypes {

    private Set<String> dataSet;

    private SortedSet<String> sortedDataSet;

    public Set<String> getDataSet() {
        return dataSet;
    }

    public void setDataSet(Set<String> dataSet) {
        this.dataSet = dataSet;
    }

    public SortedSet<String> getSortedDataSet() {
        return sortedDataSet;
    }

    public void setSortedDataSet(SortedSet<String> sortedDataSet) {
        this.sortedDataSet = sortedDataSet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TestDataWithProblematicTypes that = (TestDataWithProblematicTypes) o;
        return Objects.equals(dataSet, that.dataSet) &&
            Objects.equals(sortedDataSet, that.sortedDataSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSet, sortedDataSet);
    }
}
