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
package com.expedia.rhapsody.kafka.partitioning;

import java.time.Instant;
import java.util.Map;
import java.util.function.BinaryOperator;

/**
 * An Assignor that assigns all partitions for any given Topic to one-and-only-one Member. Useful
 * for a desired condition of having a single Producer of data derived from the consumption of a
 * topic. This Assignor makes a best-effort to deterministically choose Members based on:
 * 1) Age of the Member; then
 * 2) Alphabetic ordering of Member ID
 * When either of the above two are non-equal, this Assignor will choose whichever member is
 * "younger", i.e. the one with the latest "born" Instant or with the alphabetically last ID
 */
public abstract class AllOrNothingYoungestWinsPartitionAssignor extends AbstractAllOrNothingPartitionAssignor {

    public AllOrNothingYoungestWinsPartitionAssignor(Instant born) {
        super(born);
    }

    @Override
    protected BinaryOperator<String> bornMemberIdChooser(Map<String, Long> bornEpochMilliByMemberId) {
        return (memberId1, memberId2) -> {
            if (bornEpochMilliByMemberId.get(memberId1).equals(bornEpochMilliByMemberId.get(memberId2))) {
                return memberId1.compareTo(memberId2) < 0 ? memberId2 : memberId1;
            } else {
                return bornEpochMilliByMemberId.get(memberId1) < bornEpochMilliByMemberId.get(memberId2) ? memberId2 : memberId1;
            }
        };
    }
}
