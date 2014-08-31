package store.server;

import com.google.common.base.Charsets;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.json.Json;
import javax.json.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.config.Config;
import store.common.json.JsonWriting;
import store.server.config.ServerConfig;
import store.server.service.NodesService;

/**
 * Multicast discovery listener. Listen for incoming discovery requests and answer them.
 */
public class DiscoveryListener {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryListener.class);
    private final Config config;
    private final NodesService nodesService;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private DiscoveryThread thread;

    /**
     * Constructor.
     *
     * @param config Server config.
     * @param nodesService The nodes service.
     */
    public DiscoveryListener(Config config, NodesService nodesService) {
        this.config = config;
        this.nodesService = nodesService;
    }

    /**
     * Start the module.
     */
    public void start() {
        if (!config.getBoolean(ServerConfig.DISCOVERY_ENABLE)) {
            LOG.info("Multicast discovery disabled");
            return;
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        synchronized (this) {
            thread = new DiscoveryThread();
            thread.start();
            try {
                wait();

            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    /**
     * Properly shutdown the module and release underlying ressources.
     */
    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        thread.close();
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
            super("discovery-listener");
            setDaemon(true);
        }

        @Override
        public void run() {
            buildSocket();
            while (started.get()) {
                try {
                    processRequest();

                } catch (IOException e) {
                    if (started.get()) {
                        LOG.error("Unexpected IO error", e);
                    }
                }
            }
        }

        private void buildSocket() {
            synchronized (DiscoveryListener.this) {
                try {
                    String address = config.getString(ServerConfig.DISCOVERY_GROUP);
                    int port = config.getInt(ServerConfig.DISCOVERY_PORT);
                    int ttl = config.getInt(ServerConfig.DISCOVERY_TTL);

                    group = InetAddress.getByName(address);
                    socket = new MulticastSocket(port);
                    socket.joinGroup(group);
                    socket.setTimeToLive(ttl);
                    socket.setLoopbackMode(false);

                    LOG.info("Multicast discovery listener bound to [{}:{}]", address, port);
                    DiscoveryListener.this.notifyAll();

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

            LOG.info("Responding to discovery request from {}", sender(packet));

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
            return payload.equals("store-discovery");
        }

        private String sender(DatagramPacket packet) {
            return String.format("[%s:%s]", packet.getAddress().getHostAddress(), packet.getPort());
        }

        private byte[] response() throws IOException {
            try (ByteArrayOutputStream output = new ByteArrayOutputStream();
                    JsonWriter writer = Json.createWriter(output)) {
                writer.write(JsonWriting.write(nodesService.getNodeDef()));
                return output.toByteArray();
            }
        }

        /**
         * Leave the multicast group and close the socket.
         */
        public synchronized void close() {
            try {
                socket.leaveGroup(group);

            } catch (IOException e) {
                LOG.warn("Failed to leave group " + group.getHostAddress(), e);

            } finally {
                socket.close();
            }
        }
    }
}
