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

import java.util.Arrays;
import java.util.Objects;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;

import com.expediagroup.rhapsody.kafka.avro.util.AvroSchemas;

public class GenericDataHolderWithAdditionalField<T> extends GenericDataHolder<T> implements GenericContainer {

    private String extraData;

    @Override
    public Schema getSchema() {
        return Schema.createRecord(GenericDataHolder.class.getName(), null, null, false, Arrays.asList(
            new Schema.Field("extraData", Schema.create(Schema.Type.STRING), null, Object.class.cast(null)),
            new Schema.Field("data", AvroSchemas.getOrReflectNullable(getData()), null, JsonProperties.NULL_VALUE)));
    }

    public void setExtraData(String extraData) {
        this.extraData = extraData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        GenericDataHolderWithAdditionalField<?> that = (GenericDataHolderWithAdditionalField<?>) o;
        return Objects.equals(extraData, that.extraData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), extraData);
    }
}
