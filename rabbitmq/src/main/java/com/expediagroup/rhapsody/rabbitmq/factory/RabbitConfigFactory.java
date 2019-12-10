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
package com.expediagroup.rhapsody.rabbitmq.factory;

import java.util.Map;
import java.util.Objects;

import com.expediagroup.rhapsody.core.factory.ConfigFactory;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitConfigFactory extends ConfigFactory<Map<String, Object>> {

    public static final String HOST_PROPERTY = "host";

    public static final String PORT_PROPERTY = "port";

    public static final String VIRTUAL_HOST_PROPERTY = "virtual-host";

    public static final String USERNAME_PROPERTY = "username";

    public static final String PASSWORD_PROPERTY = "password";

    public static final String SSL_PROPERTY = "ssl";

    public static final String DISABLED_CONFIG = "disabled";

    public ConnectionFactory createConnectionFactory() {
        return createConnectionFactory(create());
    }

    public static ConnectionFactory createConnectionFactory(Map<String, ?> properties) {
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(Objects.toString(properties.get(HOST_PROPERTY)));
            connectionFactory.setPort(Integer.valueOf(Objects.toString(properties.get(PORT_PROPERTY))));
            connectionFactory.setVirtualHost(Objects.toString(properties.get(VIRTUAL_HOST_PROPERTY)));
            connectionFactory.setUsername(Objects.toString(properties.get(USERNAME_PROPERTY)));
            connectionFactory.setPassword(Objects.toString(properties.get(PASSWORD_PROPERTY)));
            if (!Objects.toString(properties.get(SSL_PROPERTY), DISABLED_CONFIG).equals(DISABLED_CONFIG)) {
                connectionFactory.useSslProtocol(Objects.toString(properties.get(SSL_PROPERTY)));
            }
            return connectionFactory;
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not create ConnectionFactory: " + e);
        }
    }

    public RabbitConfigFactory copy() {
        return copyInto(RabbitConfigFactory::new);
    }

    @Override
    public RabbitConfigFactory with(String key, Object value) {
        put(key, value);
        return this;
    }

    @Override
    protected void validateProperties(Map<String, Object> properties) {
        validateAddressProperties(properties);
        validateNonNullProperty(properties, VIRTUAL_HOST_PROPERTY);
        validateNonNullProperty(properties, USERNAME_PROPERTY);
        validateNonNullProperty(properties, PASSWORD_PROPERTY);
    }

    @Override
    protected Map<String, Object> postProcessProperties(Map<String, Object> properties) {
        return properties;
    }

    @Override
    protected String getDefaultSpecifier() {
        return "rabbit";
    }

    protected void validateAddressProperties(Map<String, Object> properties) {
        validateNonNullProperty(properties, HOST_PROPERTY);
        validateNonNullProperty(properties, PORT_PROPERTY);
    }
}
