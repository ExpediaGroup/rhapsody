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

import java.time.Instant;

import com.expediagroup.rhapsody.api.WorkHeaderBean;
import com.expediagroup.rhapsody.api.WorkType;

public final class TestWorkHeader extends WorkHeaderBean {

    private long recycleCount;

    public TestWorkHeader(WorkType type, String marker, String subject, long inceptionEpochMilli) {
        super(type, marker, subject, inceptionEpochMilli);
    }

    protected TestWorkHeader() {

    }

    public static TestWorkHeader incept(WorkType type, String marker, String subject) {
        return new TestWorkHeader(type, marker, subject, Instant.now().toEpochMilli());
    }

    @Override
    public TestWorkHeader recycle(WorkType workType, String cause) {
        TestWorkHeader recycleHeader = new TestWorkHeader(workType, getMarker(), getSubject(), getInceptionEpochMilli());
        recycleHeader.recycleCount = recycleCount + 1;
        return recycleHeader;
    }

    @Override
    public long recycleCount() {
        return getRecycleCount();
    }

    public long getRecycleCount() {
        return recycleCount;
    }
}
