package store.server;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.NodeDef;
import store.common.config.Config;
import static store.common.config.ConfigUtil.duration;
import static store.common.config.ConfigUtil.unit;
import store.common.json.JsonReading;
import store.server.async.AsyncManager;
import store.server.async.Task;
import store.server.config.ServerConfig;
import store.server.service.NodesService;

/**
 * Multicast discovery client. Sends periodically discovery requests and collects responses.
 */
public class MulticastDiscoveryClient {

    private static final byte[] PAYLOAD = "store-discovery".getBytes(Charsets.UTF_8);
    private static final String ERROR_MESSAGE = "Unexpected IO error";
    private static final Logger LOG = LoggerFactory.getLogger(MulticastDiscoveryClient.class);
    private final Config config;
    private final AsyncManager asyncManager;
    private final NodesService nodesService;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private MulticastSocket socket;
    private Thread listeningThread;
    private Task pingTask;

    /**
     * Constructor.
     *
     * @param config Config.
     * @param asyncManager Asynchronous tasks manager.
     * @param nodesService The nodes service.
     */
    public MulticastDiscoveryClient(Config config, AsyncManager asyncManager, NodesService nodesService) {
        this.config = config;
        this.asyncManager = asyncManager;
        this.nodesService = nodesService;
    }

    /**
     * Starts the client.
     */
    public void start() {
        if (!config.getBoolean(ServerConfig.DISCOVERY_MULTICAST_PING_ENABLED)) {
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
            socket.setTimeToLive(config.getInt(ServerConfig.DISCOVERY_MULTICAST_TTL));

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void initTasks() {
        listeningThread = new ListeningThread();
        listeningThread.start();

        pingTask = asyncManager.schedule(duration(config, ServerConfig.DISCOVERY_MULTICAST_PING_INTERVAL),
                                         unit(config, ServerConfig.DISCOVERY_MULTICAST_PING_INTERVAL),
                                         "Sending multicast discovery request",
                                         new PingTask());
    }

    /**
     * Stops the client, releasing underlying ressources.
     */
    public void shutdown() {
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
            try {
                InetAddress group = InetAddress.getByName(config.getString(ServerConfig.DISCOVERY_MULTICAST_GROUP));
                int port = config.getInt(ServerConfig.DISCOVERY_MULTICAST_PORT);
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
                    nodesService.saveRemote(read(packet));

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
