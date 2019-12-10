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
package com.expediagroup.rhapsody.amqp.test;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.Main;
import org.apache.qpid.server.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAmqpFactory {

    public static final String TEST_AMQP_PROPERTY_PREFIX = "test.amqp.";

    public static final String HOST_PROPERTY = "host";

    public static final String PORT_PROPERTY = "port";

    public static final String VIRTUAL_HOST_PROPERTY = "virtual-host";

    public static final String USERNAME_PROPERTY = "username";

    public static final String PASSWORD_PROPERTY = "password";

    public static final String SSL_PROPERTY = "ssl";

    public static final boolean LOCAL_AMQP = Boolean.valueOf(System.getProperty(TEST_AMQP_PROPERTY_PREFIX + "local", "true"));

    private static final String TEST_AMQP_HOST = System.getProperty(TEST_AMQP_PROPERTY_PREFIX + HOST_PROPERTY, "localhost");

    private static final String TEST_AMQP_PORT = System.getProperty(TEST_AMQP_PROPERTY_PREFIX + PORT_PROPERTY, "5672");

    private static final String TEST_AMQP_VIRTUAL_HOST = System.getProperty(TEST_AMQP_PROPERTY_PREFIX + VIRTUAL_HOST_PROPERTY, "/");

    private static final String TEST_AMQP_USERNAME = System.getProperty(TEST_AMQP_PROPERTY_PREFIX + USERNAME_PROPERTY, "guest");

    private static final String TEST_AMQP_PASSWORD = System.getProperty(TEST_AMQP_PROPERTY_PREFIX + PASSWORD_PROPERTY, "guest");

    private static final String TEST_AMQP_SSL_PROTOCOL = System.getProperty(TEST_AMQP_PROPERTY_PREFIX + SSL_PROPERTY, "TLS");

    private static final Logger LOGGER = LoggerFactory.getLogger(TestAmqpFactory.class);

    private static Map<String, String> brokerOptions;

    public Map<String, ?> createAmqp() {
        return getBrokerOptions();
    }

    private static Map<String, String> getBrokerOptions() {
        return brokerOptions == null ? brokerOptions = initializeBroker() : brokerOptions;
    }

    private static Map<String, String> initializeBroker() {
        Map<String, String> brokerOptions = createBrokerOptions();
        if (LOCAL_AMQP) {
            startLocalBroker(brokerOptions);
        }
        return brokerOptions;
    }

    private static Map<String, String> createBrokerOptions() {
        Map<String, String> brokerOptions = new HashMap<>();
        brokerOptions.put(HOST_PROPERTY, TEST_AMQP_HOST);
        brokerOptions.put(PORT_PROPERTY, TEST_AMQP_PORT);
        brokerOptions.put(VIRTUAL_HOST_PROPERTY, TEST_AMQP_VIRTUAL_HOST);
        brokerOptions.put(USERNAME_PROPERTY, TEST_AMQP_USERNAME);
        brokerOptions.put(PASSWORD_PROPERTY, TEST_AMQP_PASSWORD);
        brokerOptions.put(SSL_PROPERTY, TEST_AMQP_SSL_PROTOCOL);
        return brokerOptions;
    }

    private static void startLocalBroker(Map<String, String> brokerOptions) {
        try {
            LOGGER.info("BEGINNING STARTUP OF LOCAL AMQP BROKER");
            Path tempDirectory = Files.createTempDirectory(TestAmqpFactory.class.getSimpleName() + "_" + System.currentTimeMillis());
            List<String> localBrokerArguments = createLocalBrokerArguments(brokerOptions, tempDirectory);
            System.getProperties().putIfAbsent("derby.stream.error.file", new File(tempDirectory.toFile(), "derby.log").getAbsolutePath());
            org.apache.qpid.server.Main.main(localBrokerArguments.toArray(new String[0]));
            LOGGER.info("FINISHED STARTUP OF LOCAL AMQP BROKER");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start local Broker: " + e);
        }
    }

    private static List<String> createLocalBrokerArguments(Map<String, String> brokerOptions, Path tempDirectory) throws Exception {
        List<String> brokerArguments = new ArrayList<>(Arrays.asList(
            "--initial-config-path", createAmqpConfig(tempDirectory).getCanonicalPath(),
            "--config-property", String.format("%s=%s", SystemConfig.QPID_WORK_DIR, tempDirectory.toString()),
            "--config-property", String.format("%s=%s", Main.PROPERTY_QPID_HOME, tempDirectory.toString())));
        brokerOptions.forEach((key, value) -> brokerArguments.addAll(Arrays.asList(
            "--config-property", String.format("%s=%s", key, value))));
        return brokerArguments;
    }

    private static File createAmqpConfig(Path directory) throws Exception {
        File configFile = Files.createTempFile(directory, "amqp", SystemConfig.DEFAULT_INITIAL_CONFIG_NAME).toFile();
        PrintWriter configWriter = new PrintWriter(configFile);
        configWriter.println("{");
        configWriter.println("    \"name\": \"broker\",");
        configWriter.println("    \"modelVersion\": \"7.1\",");
        configWriter.println("    \"virtualhostnodes\": [{");
        configWriter.println("        \"type\": \"Memory\",");
        configWriter.println("        \"name\": \"default\",");
        configWriter.println("        \"defaultVirtualHostNode\": \"true\",");
        configWriter.println("        \"virtualHostInitialConfiguration\": \"{\\\"type\\\": \\\"Memory\\\"}\"");
        configWriter.println("    }],");
        configWriter.println("    \"authenticationproviders\": [{");
        configWriter.println("        \"type\": \"Plain\",");
        configWriter.println("        \"name\": \"plain\",");
        configWriter.println("        \"users\": [{");
        configWriter.println("            \"type\": \"managed\",");
        configWriter.println("            \"name\": \"${" + USERNAME_PROPERTY + "}\",");
        configWriter.println("            \"password\": \"${" + PASSWORD_PROPERTY + "}\"");
        configWriter.println("        }]");
        configWriter.println("    }],");
        configWriter.println("    \"keystores\": [{");
        configWriter.println("        \"type\": \"AutoGeneratedSelfSigned\",");
        configWriter.println("        \"name\": \"default\"");
        configWriter.println("    }],");
        configWriter.println("    \"ports\": [{");
        configWriter.println("        \"name\": \"AMQP\",");
        configWriter.println("        \"port\": \"${" + PORT_PROPERTY + "}\",");
        configWriter.println("        \"transports\": [\"SSL\"],");
        configWriter.println("        \"authenticationProvider\": \"plain\",");
        configWriter.println("        \"keyStore\": \"default\",");
        configWriter.println("        \"virtualhostaliases\": [{");
        configWriter.println("            \"type\": \"defaultAlias\",");
        configWriter.println("            \"name\": \"defaultAlias\"");
        configWriter.println("        }, {");
        configWriter.println("            \"type\": \"hostnameAlias\",");
        configWriter.println("            \"name\": \"hostnameAlias\"");
        configWriter.println("        }, {");
        configWriter.println("            \"type\": \"nameAlias\",");
        configWriter.println("            \"name\": \"nameAlias\"");
        configWriter.println("        }]");
        configWriter.println("    }]");
        configWriter.println("}");
        configWriter.flush();
        configWriter.close();
        return configFile;
    }
}
