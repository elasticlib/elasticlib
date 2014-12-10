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
package org.elasticlib.console.discovery;

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.Collections;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.concurrent.Executors;
import static java.util.concurrent.Executors.defaultThreadFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.json.Json;
import javax.json.JsonReader;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.json.JsonReading;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.console.config.ConsoleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multicast discovery client. Sends periodically discovery requests and collects responses. Maintains a list of alive
 * nodes.
 */
public class DiscoveryClient {

    private static final byte[] PAYLOAD = "store-discovery".getBytes(Charsets.UTF_8);
    private static final String ERROR_MESSAGE = "Unexpected IO error";
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryClient.class);

    private final ConsoleConfig config;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicReference<List<NodeDef>> snapshot = new AtomicReference<>(Collections.<NodeDef>emptyList());
    private MulticastSocket socket;
    private Cache<Guid, NodeDef> cache;
    private ScheduledExecutorService executor;

    /**
     * Constructor.
     *
     * @param config Config.
     */
    public DiscoveryClient(ConsoleConfig config) {
        this.config = config;
    }

    /**
     * Starts the client.
     */
    public void start() {
        if (!config.isDiscoveryEnabled() || !started.compareAndSet(false, true)) {
            return;
        }
        initSocket();
        initCache();
        initExecutor();
    }

    private void initSocket() {
        try {
            socket = new MulticastSocket();
            socket.setTimeToLive(config.getDiscoveryTimeToLive());

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

    }

    private void initCache() {
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(3 * config.getDiscoveryPingIntervalDuration(),
                                  config.getDiscoveryPingIntervalUnit())
                .build();
    }

    private void initExecutor() {
        executor = Executors.newScheduledThreadPool(2, new DiscoveryThreadFactory());

        executor.execute(new ListenTask());
        executor.scheduleAtFixedRate(new PingTask(),
                                     0,
                                     config.getDiscoveryPingIntervalDuration(),
                                     config.getDiscoveryPingIntervalUnit());
    }

    /**
     * Stops the client, releasing underlying ressources.
     */
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        socket.close();
        executor.shutdown();
    }

    /**
     * Provides a snapshot of the definitions of all current alive nodes.
     *
     * @return A list of node definitions.
     */
    public List<NodeDef> nodes() {
        cache.asMap().values();
        return snapshot.get();
    }

    /**
     * Provides thread for the discovery related tasks.
     */
    private static class DiscoveryThreadFactory implements ThreadFactory {

        private final ThreadFactory defaultFactory = defaultThreadFactory();
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = defaultFactory.newThread(runnable);
            thread.setName("discovery-client-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }

    /**
     * Send a discovery request.
     */
    private class PingTask implements Runnable {

        @Override
        public void run() {
            try {
                socket.send(new DatagramPacket(PAYLOAD,
                                               PAYLOAD.length,
                                               InetAddress.getByName(config.getDiscoveryGroup()),
                                               config.getDiscoveryPort()));

            } catch (IOException e) {
                if (started.get()) {
                    LOG.error(ERROR_MESSAGE, e);
                }
            }
        }
    }

    /**
     * Listen to discovery responses.
     */
    private class ListenTask implements Runnable {

        @Override
        public void run() {
            while (started.get()) {
                try {
                    byte[] buffer = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                    socket.receive(packet);
                    NodeDef def = read(packet);
                    cache.put(def.getGuid(), def);
                    takeSnapshot();

                } catch (IOException e) {
                    if (started.get()) {
                        LOG.error(ERROR_MESSAGE, e);
                    }
                }
            }
        }

        private NodeDef read(DatagramPacket packet) throws IOException {
            try (InputStream input = new ByteArrayInputStream(packet.getData());
                    JsonReader reader = Json.createReader(input);) {

                return JsonReading.read(reader.readObject(), NodeDef.class);
            }
        }

        private void takeSnapshot() {
            List<NodeDef> list = new ArrayList<>(cache.asMap().values());
            list.sort((lhs, rhs) -> lhs.getName().compareTo(rhs.getName()));
            snapshot.set(unmodifiableList(list));
        }
    }
}
