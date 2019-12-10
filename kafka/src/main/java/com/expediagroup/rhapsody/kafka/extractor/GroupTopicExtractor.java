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
package com.expediagroup.rhapsody.kafka.extractor;

import java.util.Map;
import java.util.function.Function;

import com.expediagroup.rhapsody.api.GroupExtractor;

public class GroupTopicExtractor<T> implements Function<T, String> {

    private final GroupExtractor<T> groupExtractor;

    private final Map<?, String> topicsByGroup;

    public GroupTopicExtractor(GroupExtractor<T> groupExtractor, Map<?, String> topicsByGroup) {
        this.groupExtractor = groupExtractor;
        this.topicsByGroup = topicsByGroup;
    }

    @Override
    public String apply(T t) {
        return topicsByGroup.get(groupExtractor.apply(t));
    }
}
