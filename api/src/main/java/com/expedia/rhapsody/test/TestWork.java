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
package com.expedia.rhapsody.test;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.expedia.rhapsody.api.Work;
import com.expedia.rhapsody.api.WorkType;

public final class TestWork implements Work {

    private TestWorkHeader workHeader;

    private boolean prepared = false;

    public static TestWork create(WorkType workType, String subject) {
        return create(workType, subject, Instant.now().toEpochMilli());
    }

    public static TestWork create(WorkType workType, String subject, long inceptionEpochMilli) {
        return create(new TestWorkHeader(workType, UUID.randomUUID().toString(), subject, inceptionEpochMilli));
    }

    public static TestWork create(TestWorkHeader workHeader) {
        TestWork testWork = new TestWork();
        testWork.workHeader = workHeader;
        return testWork;
    }

    @Override
    public TestWorkHeader workHeader() {
        return workHeader;
    }

    @Override
    public boolean isPrepared() {
        return prepared;
    }

    public TestWork prepare() {
        this.prepared = true;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TestWork testWork = (TestWork) o;
        return prepared == testWork.prepared &&
            Objects.equals(workHeader, testWork.workHeader);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workHeader, prepared);
    }
}
