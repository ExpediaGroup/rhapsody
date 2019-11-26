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
package com.expedia.rhapsody.test.kafka.avro;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expedia.rhapsody.kafka.avro.util.AvroSchemas;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

import static org.junit.Assert.assertTrue;

public abstract class AvroSchemaCompatibilityTest {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaCompatibilityTest.class);

    protected final RestService restService;

    protected final SchemaRegistryClient schemaRegistryClient;

    public AvroSchemaCompatibilityTest(String schemaUrl) {
        this.restService = new RestService(new UrlList(schemaUrl));
        this.schemaRegistryClient = new CachedSchemaRegistryClient(restService, Integer.MAX_VALUE);
    }

    @Test
    public void generatedSchemaIsCompatibleWithExistingRegistry() {
        SubjectNameStrategy<Schema> subjectNameStrategy = getSubjectNameStrategy();

        Map<TopicDatum, Schema> schemas = getTopicData().stream()
            .collect(Collectors.toMap(Function.identity(), topicDatum -> AvroSchemas.getOrReflect(topicDatum.getDatum())));

        Map<String, Schema> incompatibleSchemas = schemas.entrySet().stream()
            .filter(entry -> !safelyTestCompatibility(extractSubjectName(subjectNameStrategy, entry.getKey(), entry.getValue()), entry.getValue()))
            .collect(Collectors.toMap(entry -> entry.getKey().getTopic(), Map.Entry::getValue));

        assertTrue("Generated Schema(s) is not compatible with Registry", incompatibleSchemas.isEmpty());
    }

    protected SubjectNameStrategy<Schema> getSubjectNameStrategy() {
        return new TopicNameStrategy();
    }

    protected abstract Collection<TopicDatum> getTopicData();

    protected final boolean safelyTestCompatibility(String subject, Schema schema) {
        LOGGER.info("Schema under test for Subject {} is {}", subject, schema);
        try {
            SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
            return testCompatibility(subject, schema, latestSchemaMetadata.getVersion(), calculateEarliestVersionToTest(subject, latestSchemaMetadata));
        } catch (RestClientException restClientException) {
            if (restClientException.getStatus() == 404) {
                LOGGER.info("The Subject {} does not exist. Any Schema should be compatible when registered. e={}", subject, restClientException);
                return true;
            }
            LOGGER.warn("Something REST-y happened with testing Schema compatibility for Subject {}. Returning 'incompatible': e={}", subject, restClientException);
            return false;
        } catch (Exception e) {
            LOGGER.warn("Could not safely test compatibility of Schema on Subject. Returning 'compatible': {} {} {}", subject, schema, e);
            return true;
        }
    }

    protected int calculateEarliestVersionToTest(String subject, SchemaMetadata latestSchemaMetadata) {
        return 1;
    }

    protected final boolean testCompatibility(String subject, Schema schema, int latestVersionToTest, int earliestVersionToTest) throws IOException, RestClientException {
        for (int version = latestVersionToTest; version >= earliestVersionToTest; version--) {
            SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, version);
            LOGGER.info("Registered Schema for Subject {} has id {} with version of {} and Schema {}",
                subject, schemaMetadata.getId(), schemaMetadata.getVersion(), schemaMetadata.getSchema());
            if (!testCompatibility(subject, schema, schemaMetadata)) {
                LOGGER.warn("Generated Schema {} is incompatible with Registered Schema {}", schema, schemaMetadata.getSchema());
                return false;
            }
        }
        return true;
    }

    protected boolean testCompatibility(String subject, Schema schema, SchemaMetadata schemaMetadata) throws IOException, RestClientException {
        return restService.testCompatibility(schema.toString(), subject, Integer.toString(schemaMetadata.getVersion()));
    }

    private static String extractSubjectName(SubjectNameStrategy<Schema> subjectNameStrategy, TopicDatum topicDatum, Schema schema) {
        return subjectNameStrategy.subjectName(topicDatum.getTopic(), topicDatum.isKey(), schema);
    }
}
