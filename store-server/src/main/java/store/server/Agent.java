package store.server;

import com.google.common.base.Optional;
import java.io.Closeable;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import store.common.Operation;
import store.common.Uid;
import store.server.exception.StoreException;

public class Agent implements Closeable {

    private final AgentManager agentManager;
    private final Uid destinationId;
    private final Volume source;
    private final Volume destination;
    private final List<Event> events;
    private long cursor;
    private boolean signaled;
    private boolean closed;
    private boolean running;

    public Agent(AgentManager agentManager, Uid destinationId, Volume source, Volume destination) {
        this.agentManager = agentManager;
        this.destinationId = destinationId;
        this.source = source;
        this.destination = destination;
        events = new ArrayList<>();
    }

    @Override
    public synchronized void close() {
        closed = true;
    }

    private synchronized boolean isClosed() {
        return closed;
    }

    public synchronized void signal() {
        if (closed) {
            return;
        }
        signaled = true;
        if (!running) {
            running = true;
            this.new AgentThread().start();
        }
    }

    private synchronized void clearSignal() {
        signaled = false;
    }

    private synchronized boolean clearRunning() {
        if (!signaled) {
            running = false;
            return true;
        }
        return false;
    }

    private Optional<Event> next() {
        clearSignal();
        if (isClosed()) {
            return Optional.absent();
        }
        updateEvents();
        if (events.isEmpty()) {
            return Optional.absent();
        }
        return Optional.of(events.remove(0));
    }

    private void updateEvents() {
        List<Event> chunk = source.history(true, cursor, 1000);
        while (!chunk.isEmpty()) {
            events.addAll(chunk);
            cursor = chunk.get(chunk.size() - 1).getSeq();
            chunk = source.history(true, cursor, 1000);
        }
        for (int pos = 0; pos < events.size(); pos++) {
            Event event = events.get(pos);
            if (event.getOperation() == Operation.PUT) {
                ListIterator<Event> it = events.listIterator(pos + 1);
                while (it.hasNext()) {
                    int nextIndex = it.nextIndex();
                    Event next = it.next();
                    if (next.getHash().equals(event.getHash()) && next.getOperation() == Operation.DELETE) {
                        events.remove(pos);
                        events.remove(nextIndex);
                        pos--;
                        break;
                    }
                }
            }
        }
    }

    private class AgentThread extends Thread {

        @Override
        public void run() {
            do {
                Optional<Event> optEvent = next();
                while (optEvent.isPresent()) {
                    process(optEvent.get());
                    optEvent = next();
                }
            } while (!clearRunning());
        }

        private void process(Event event) {
            try {
                switch (event.getOperation()) {
                    case PUT:
                        put(event.getHash());
                        break;
                    case DELETE:
                        delete(event.getHash());
                        break;
                }
                agentManager.signal(destinationId);

            } catch (StoreException e) {
                events.add(0, event);
            }
        }

        private void put(Hash hash) {
            ContentInfo info = source.info(hash);
            try (PipedInputStream in = new PipedInputStream()) {
                new PipeWriterThread(in, hash).start();
                destination.put(info, in);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void delete(Hash hash) {
            destination.delete(hash);
        }
    }

    private class PipeWriterThread extends Thread {

        private final PipedInputStream in;
        private final Hash hash;

        public PipeWriterThread(PipedInputStream in, Hash hash) {
            this.in = in;
            this.hash = hash;
        }

        @Override
        public void run() {
            try (PipedOutputStream out = new PipedOutputStream(in)) {
                source.get(hash, out);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
