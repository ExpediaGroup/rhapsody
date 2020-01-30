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

import java.util.function.Consumer;
import java.util.function.Function;

import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.jersey.servlet.ServletContainer;

import com.expediagroup.rhapsody.core.stanza.Stanza;
import com.expediagroup.rhapsody.dropwizard.stanza.StanzaBundle;

import io.dropwizard.setup.Environment;

public class SampleKafkaProcessingStanzaBundle<T> extends StanzaBundle<T, SampleKafkaProcessingStanzaConfig> {

    private final Function<? super T, ? extends SampleKafkaConfiguration> configurationFunction;

    public SampleKafkaProcessingStanzaBundle(Function<? super T, ? extends SampleKafkaConfiguration> configurationFunction) {
        this.configurationFunction = configurationFunction;
    }

    @Override
    protected Stanza<SampleKafkaProcessingStanzaConfig> buildStanza(T configuration, Environment environment) {
        return new SampleKafkaProcessingStanza();
    }

    @Override
    protected SampleKafkaProcessingStanzaConfig buildStanzaConfig(T configuration, Environment environment) {
        SampleKafkaConfiguration config = configurationFunction.apply(configuration);

        // For the sake of showing possibilities, we'll look for a Consumer resource in the environment
        Consumer<String> consumer = ServletContainer.class.cast(environment.getJerseyServletContainer())
            .getApplicationHandler()
            .getServiceLocator()
            .getService(new TypeLiteral<Consumer<String>>() {}.getType());

        return new SampleKafkaProcessingStanzaConfig(config.getBootstrapServers(), config.getTopic(), consumer);
    }
}
