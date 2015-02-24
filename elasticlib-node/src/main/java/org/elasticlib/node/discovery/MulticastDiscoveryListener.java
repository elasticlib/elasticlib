/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.node.discovery;

import com.google.common.base.Charsets;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import javax.json.Json;
import javax.json.JsonWriter;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.json.JsonWriting;
import org.elasticlib.node.config.NodeConfig;
import org.elasticlib.node.service.NodeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multicast discovery listener. Listen for incoming discovery requests and answer them.
 */
public class MulticastDiscoveryListener {

    private static final String PAYLOAD = "elasticlib-discovery";
    private static final Logger LOG = LoggerFactory.getLogger(MulticastDiscoveryListener.class);

    private final Config config;
    private final NodeService nodeService;
    private boolean started;
    private DiscoveryThread thread;

    /**
     * Constructor.
     *
     * @param config Server config.
     * @param nodeService Local node service.
     */
    public MulticastDiscoveryListener(Config config, NodeService nodeService) {
        this.config = config;
        this.nodeService = nodeService;
    }

    /**
     * Starts the listener.
     */
    public synchronized void start() {
        if (!config.getBoolean(NodeConfig.DISCOVERY_MULTICAST_LISTEN)) {
            LOG.info("Multicast discovery listening is disabled");
            return;
        }
        if (started) {
            return;
        }
        thread = new DiscoveryThread();
        thread.start();
        try {
            while (!started) {
                wait();
            }
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Properly stops the listener and release underlying ressources.
     */
    public void stop() {
        synchronized (this) {
            if (!started) {
                return;
            }
            thread.close();
        }
        try {
            thread.join();

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    private class DiscoveryThread extends Thread {

        private InetAddress group;
        private MulticastSocket socket;

        public DiscoveryThread() {
            super("multicast-discovery-listener");
            setDaemon(true);
        }

        @Override
        public void run() {
            buildSocket();
            while (isStarted()) {
                try {
                    processRequest();

                } catch (IOException e) {
                    if (isStarted()) {
                        LOG.error("Unexpected IO error", e);
                    }
                }
            }
        }

        private boolean isStarted() {
            synchronized (MulticastDiscoveryListener.this) {
                return started;
            }
        }

        private void buildSocket() {
            synchronized (MulticastDiscoveryListener.this) {
                try {
                    String address = config.getString(NodeConfig.DISCOVERY_MULTICAST_GROUP);
                    int port = config.getInt(NodeConfig.DISCOVERY_MULTICAST_PORT);
                    int ttl = config.getInt(NodeConfig.DISCOVERY_MULTICAST_TTL);

                    group = InetAddress.getByName(address);
                    socket = new MulticastSocket(port);
                    socket.joinGroup(group);
                    socket.setTimeToLive(ttl);
                    socket.setLoopbackMode(false);

                    LOG.info("Multicast discovery listener bound to [{}:{}]", address, port);

                    started = true;
                    MulticastDiscoveryListener.this.notifyAll();

                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        private void processRequest() throws IOException {
            byte[] buffer = new byte[1024];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            socket.receive(packet);
            if (!isValid(packet)) {
                return;
            }

            LOG.info("Responding to multicast discovery request from {}", sender(packet));

            byte[] response = response();
            socket.send(new DatagramPacket(response,
                                           response.length,
                                           packet.getAddress(),
                                           packet.getPort()));
        }

        private boolean isValid(DatagramPacket packet) throws IOException {
            if (packet.getLength() == 0) {
                return false;
            }
            String payload = new String(packet.getData(), Charsets.UTF_8).trim().toLowerCase();
            return payload.equals(PAYLOAD);
        }

        private String sender(DatagramPacket packet) {
            return String.format("[%s:%s]", packet.getAddress().getHostAddress(), packet.getPort());
        }

        private byte[] response() throws IOException {
            try (ByteArrayOutputStream output = new ByteArrayOutputStream();
                    JsonWriter writer = Json.createWriter(output)) {
                writer.write(JsonWriting.write(nodeService.getNodeDef()));
                return output.toByteArray();
            }
        }

        /**
         * Leave the multicast group and close the socket.
         */
        public void close() {
            try {
                socket.leaveGroup(group);

            } catch (IOException e) {
                LOG.warn("Failed to leave group " + group.getHostAddress(), e);

            } finally {
                started = false;
                socket.close();
            }
        }
    }
}
