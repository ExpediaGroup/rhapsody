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
package com.expedia.rhapsody.zookeeper.test;

import java.net.URL;

import org.apache.curator.test.TestingServer;

public class TestZookeeperFactory {

    public static final String TEST_ZOOKEEPER_PROPERTY_PREFIX = "test.zookeeper.";

    public static final boolean LOCAL_ZOOKEEPER = Boolean.parseBoolean(System.getProperty(TEST_ZOOKEEPER_PROPERTY_PREFIX + "local", "true"));

    private static final String TEST_ZOOKEEPER_CONNECT = System.getProperty(TEST_ZOOKEEPER_PROPERTY_PREFIX + "connect", "localhost:2181");

    private static URL zookeeperConnect;

    public URL createConnect() {
        return zookeeperConnect == null ? zookeeperConnect = initializeConnect() : zookeeperConnect;
    }

    private static URL initializeConnect() {
        URL zookeeperConnect = convertToConnectUrl(TEST_ZOOKEEPER_CONNECT);
        return LOCAL_ZOOKEEPER ? startLocalZookeeper(zookeeperConnect) : zookeeperConnect;
    }

    private static URL startLocalZookeeper(URL zookeeperConnect) {
        try {
            new TestingServer(zookeeperConnect.getPort());
            return convertToConnectUrl("localhost:" + zookeeperConnect.getPort());
        } catch (Exception e) {
            throw new IllegalStateException("Could not start local Zookeeper Server", e);
        }
    }

    private static URL convertToConnectUrl(String connect) {
        try {
            return new URL("http://" + connect);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not create URL for Connect: " + connect, e);
        }
    }
}
