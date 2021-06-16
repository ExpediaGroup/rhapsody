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
package com.expediagroup.rhapsody.dropwizard.stanza;

import java.lang.management.ManagementFactory;
import java.util.Objects;
import java.util.Optional;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;

import com.expediagroup.rhapsody.core.stanza.Stanza;
import com.expediagroup.rhapsody.core.stanza.StanzaConfig;
import com.expediagroup.rhapsody.util.Throwing;

import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * Wraps a Stanza in to a Bundle which is managed by the Server LifeCycle and also wired in to JMX
 *
 * @param <T> The type of Configuration (usually for the application)
 * @param <C> The type used to configure a Stanza
 */
public abstract class ManagedStanzaBundle<T, C extends StanzaConfig> implements ConfiguredBundle<T> {

    @Override
    public void initialize(Bootstrap<?> bootstrap) {

    }

    @Override
    public final void run(T configuration, Environment environment) throws Exception {
        environment.lifecycle().addLifeCycleListener(new ManagedStanzaLifeCycleListener(configuration, environment));
    }

    /**
     * The Stanza will be built upon <i>running of the Bundle</i>. Note that this means the
     * Environment may not yet be fully set up when this method is called.
     */
    protected abstract Stanza<C> buildStanza(T configuration, Environment environment);

    /**
     * The StanzaConfig will be built <i>after the Server has started</i>, which makes it
     * safe(r) to try and access resources from the environment (like ones provided by
     * dependency injection) when this method is called and contain them in the Config.
     */
    protected abstract C buildStanzaConfig(T configuration, Environment environment);

    /**
     * Name used to register Stanza with JMX. Can be overridden to return empty if
     * JMX registration is not desired
     * https://www.oracle.com/java/technologies/javase/management-extensions-best-practices.html#mozTocId434075
     */
    protected Optional<String> jmxObjectName(T configuration, Environment environment, C stanzaConfig) {
        return Optional.of(String.format("com.expediagroup.rhapsody.dropwizard:type=Stanza,subType=%s,name=%s",
            getClass().getSimpleName(), stanzaConfig.name()));
    }

    protected boolean shouldStartOnStartup(T configuration, Environment environment, C stanzaConfig) {
        String key = "stanza.bundle." + stanzaConfig.name() + ".start.on.startup";
        String startOnStartup = System.getProperty(key, System.getenv(key));
        return !Objects.equals("false", startOnStartup);
    }

    public interface ManagedStanzaLifeCycleListenerMBean {

        void start();

        void stop();

        String state();
    }

    private final class ManagedStanzaLifeCycleListener extends AbstractLifeCycle.AbstractLifeCycleListener implements ManagedStanzaLifeCycleListenerMBean {

        private final T configuration;

        private final Environment environment;

        private final Stanza<C> stanza;

        public ManagedStanzaLifeCycleListener(T configuration, Environment environment) {
            this.configuration = configuration;
            this.environment = environment;
            this.stanza = buildStanza(configuration, environment);
        }

        @Override
        public void lifeCycleStarted(LifeCycle event) {
            C stanzaConfig = buildStanzaConfig(configuration, environment);
            register(stanzaConfig);
            if (shouldStartOnStartup(configuration, environment, stanzaConfig)) {
                stanza.start(stanzaConfig);
            }
        }

        @Override
        public void lifeCycleStopping(LifeCycle event) {
            stop();
        }

        @Override
        public void start() {
            stanza.start(buildStanzaConfig(configuration, environment));
        }

        @Override
        public void stop() {
            stanza.stop();
        }

        @Override
        public String state() {
            return stanza.state().name();
        }

        private void register(C stanzaConfig) {
            jmxObjectName(configuration, environment, stanzaConfig).ifPresent(objectName -> {
                try {
                    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                    mBeanServer.registerMBean(this, new ObjectName(objectName));
                } catch (MalformedObjectNameException |
                    NotCompliantMBeanException |
                    MBeanRegistrationException |
                    InstanceAlreadyExistsException e) {
                    throw Throwing.propagate(e, IllegalStateException::new);
                }
            });
        }
    }
}