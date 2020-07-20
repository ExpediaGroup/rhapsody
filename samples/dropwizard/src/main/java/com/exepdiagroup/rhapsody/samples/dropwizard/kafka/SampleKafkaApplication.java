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

import javax.ws.rs.core.GenericType;

import org.glassfish.jersey.internal.inject.AbstractBinder;

import com.expediagroup.rhapsody.dropwizard.metrics.DefaultDropwizardMeterRegistry;
import com.expediagroup.rhapsody.kafka.test.TestKafkaFactory;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.micrometer.core.instrument.Metrics;

public class SampleKafkaApplication extends Application<SampleKafkaApplicationConfiguration> {

    public static void main(String[] args) throws Exception {
        new SampleKafkaApplication().run(
            args == null || args.length == 0 ? new String[] { "server", "samples/dropwizard/src/main/resources/config-kafka.yaml" } : args);
    }

    @Override
    public void initialize(Bootstrap<SampleKafkaApplicationConfiguration> bootstrap) {
        // Start an embedded Kafka broker
        new TestKafkaFactory().createKafka();

        // Add Bundle that will generate Kafka data
        bootstrap.addBundle(new SampleKafkaGenerationStanzaBundle<>(SampleKafkaApplicationConfiguration::getKafka));

        // Add Bundle that will process the data we generate
        bootstrap.addBundle(new SampleKafkaProcessingStanzaBundle<>(SampleKafkaApplicationConfiguration::getKafka));
    }

    @Override
    public void run(SampleKafkaApplicationConfiguration sampleKafkaApplicationConfiguration, Environment environment) throws Exception {
        // Wire Codahale Metrics to Micrometer
        Metrics.addRegistry(new DefaultDropwizardMeterRegistry(environment.metrics()));

        environment.jersey().register(new AbstractBinder() {

            @Override
            protected void configure() {
                // Bind a Consumer that we'll access in Bundle(s)
                this.<Consumer<String>>bind(System.out::println).to(new GenericType<Consumer<String>>() {});
            }
        });
    }
}
