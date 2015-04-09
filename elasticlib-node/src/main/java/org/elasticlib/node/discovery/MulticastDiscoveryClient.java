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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.json.Json;
import javax.json.JsonReader;
import org.elasticlib.common.config.Config;
import static org.elasticlib.common.config.ConfigUtil.duration;
import static org.elasticlib.common.config.ConfigUtil.unit;
import org.elasticlib.common.json.JsonReading;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.node.config.NodeConfig;
import org.elasticlib.node.manager.task.Task;
import org.elasticlib.node.manager.task.TaskManager;
import org.elasticlib.node.service.RemotesService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multicast discovery client. Sends periodically discovery requests and collects responses.
 */
public class MulticastDiscoveryClient {

    private static final byte[] PAYLOAD = "elasticlib-discovery".getBytes(Charsets.UTF_8);
    private static final String ERROR_MESSAGE = "Unexpected IO error";
    private static final Logger LOG = LoggerFactory.getLogger(MulticastDiscoveryClient.class);
    private final Config config;
    private final TaskManager taskManager;
    private final RemotesService remotesService;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private MulticastSocket socket;
    private Thread listeningThread;
    private Task pingTask;

    /**
     * Constructor.
     *
     * @param config Config.
     * @param taskManager Asynchronous tasks manager.
     * @param remotesService Remote nodes service.
     */
    public MulticastDiscoveryClient(Config config, TaskManager taskManager, RemotesService remotesService) {
        this.config = config;
        this.taskManager = taskManager;
        this.remotesService = remotesService;
    }

    /**
     * Starts the client.
     */
    public void start() {
        if (!config.getBoolean(NodeConfig.DISCOVERY_MULTICAST_PING_ENABLED)) {
            LOG.info("Multicast discovery is disabled");
            return;
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        initSocket();
        initTasks();
    }

    private void initSocket() {
        try {
            socket = new MulticastSocket();
            socket.setTimeToLive(config.getInt(NodeConfig.DISCOVERY_MULTICAST_TTL));

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void initTasks() {
        listeningThread = new ListeningThread();
        listeningThread.start();

        pingTask = taskManager.schedule(duration(config, NodeConfig.DISCOVERY_MULTICAST_PING_INTERVAL),
                                        unit(config, NodeConfig.DISCOVERY_MULTICAST_PING_INTERVAL),
                                        "Sending multicast discovery request",
                                        new PingTask());
    }

    /**
     * Stops the client, releasing underlying ressources.
     */
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        socket.close();
        pingTask.cancel();

        try {
            listeningThread.join();

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Send a discovery request.
     */
    private class PingTask implements Runnable {

        @Override
        public void run() {
            if (config.getBoolean(NodeConfig.DISCOVERY_HYBRID) && remotesService.hasReachableRemotes()) {
                return;
            }
            try {
                InetAddress group = InetAddress.getByName(config.getString(NodeConfig.DISCOVERY_MULTICAST_GROUP));
                int port = config.getInt(NodeConfig.DISCOVERY_MULTICAST_PORT);
                socket.send(new DatagramPacket(PAYLOAD, PAYLOAD.length, group, port));

            } catch (IOException e) {
                if (started.get()) {
                    LOG.error(ERROR_MESSAGE, e);
                }
            }
        }
    }

    /**
     * Listens to discovery responses.
     */
    private class ListeningThread extends Thread {

        public ListeningThread() {
            super("multicast-discovery-ping-listener");
            setDaemon(true);
        }

        @Override
        public void run() {
            while (started.get()) {
                try {
                    byte[] buffer = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                    socket.receive(packet);

                    LOG.info("Received multicast discovery response from {}", sender(packet));
                    NodeDef def = read(packet);
                    remotesService.saveRemote(def.getPublishUris(), def.getGuid());

                } catch (IOException e) {
                    if (started.get()) {
                        LOG.error(ERROR_MESSAGE, e);
                    }
                }
            }
        }

        private String sender(DatagramPacket packet) {
            return String.format("[%s:%s]", packet.getAddress().getHostAddress(), packet.getPort());
        }

        private NodeDef read(DatagramPacket packet) throws IOException {
            try (InputStream input = new ByteArrayInputStream(packet.getData());
                    JsonReader reader = Json.createReader(input);) {

                return JsonReading.read(reader.readObject(), NodeDef.class);
            }
        }
    }
}
