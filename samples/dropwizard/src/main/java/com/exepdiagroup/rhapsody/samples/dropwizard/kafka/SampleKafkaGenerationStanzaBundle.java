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
package com.exepdiagroup.rhapsody.samples.dropwizard.kafka;

import java.util.function.Function;

import com.expediagroup.rhapsody.core.stanza.Stanza;
import com.expediagroup.rhapsody.dropwizard.stanza.StanzaBundle;

import io.dropwizard.setup.Environment;

public class SampleKafkaGenerationStanzaBundle<T> extends StanzaBundle<T, SampleKafkaGenerationStanzaConfig> {

    private final Function<? super T, ? extends SampleKafkaConfiguration> configurationFunction;

    public SampleKafkaGenerationStanzaBundle(Function<? super T, ? extends SampleKafkaConfiguration> configurationFunction) {
        this.configurationFunction = configurationFunction;
    }

    @Override
    protected Stanza<SampleKafkaGenerationStanzaConfig> buildStanza(T configuration, Environment environment) {
        return new SampleKafkaGenerationStanza();
    }

    @Override
    protected SampleKafkaGenerationStanzaConfig buildStanzaConfig(T configuration, Environment environment) {
        SampleKafkaConfiguration config = configurationFunction.apply(configuration);
        return new SampleKafkaGenerationStanzaConfig(config.getBootstrapServers(), config.getTopic());
    }
}
