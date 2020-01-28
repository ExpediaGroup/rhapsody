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

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;

import com.expediagroup.rhapsody.core.stanza.Stanza;
import com.expediagroup.rhapsody.core.stanza.StanzaConfig;

import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * Wraps a Stanza in to a Bundle which is managed by the Server LifeCycle
 *
 * @param <T> The type of Configuration (usually for the application)
 * @param <C> The type used to configure a Stanza
 */
public abstract class StanzaBundle<T, C extends StanzaConfig> implements ConfiguredBundle<T> {

    @Override
    public void initialize(Bootstrap<?> bootstrap) {

    }

    @Override
    public final void run(T configuration, Environment environment) throws Exception {
        environment.lifecycle().addLifeCycleListener(new StanzaLifeCycleListener(configuration, environment));
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

    private class StanzaLifeCycleListener extends AbstractLifeCycle.AbstractLifeCycleListener {

        private final T configuration;

        private final Environment environment;

        private final Stanza<C> stanza;

        public StanzaLifeCycleListener(T configuration, Environment environment) {
            this.configuration = configuration;
            this.environment = environment;
            this.stanza = buildStanza(configuration, environment);
        }

        @Override
        public void lifeCycleStarted(LifeCycle event) {
            stanza.start(buildStanzaConfig(configuration, environment));
        }

        @Override
        public void lifeCycleStopping(LifeCycle event) {
            stanza.stop();
        }
    }
}
