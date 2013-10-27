package store.server;

import com.google.common.base.Optional;
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
import store.server.exception.ConcurrentOperationException;
import store.server.exception.ContentAlreadyStoredException;
import store.server.exception.IntegrityCheckingFailedException;
import store.server.exception.StoreException;
import store.server.exception.UnknownHashException;
import store.server.exception.VolumeClosedException;
import store.server.exception.WriteException;

public class Agent {

    private final AgentManager agentManager;
    private final Uid destinationId;
    private final Volume source;
    private final Volume destination;
    private final List<Event> events;
    private long cursor;
    private boolean signaled;
    private boolean stoped;
    private boolean running;

    public Agent(AgentManager agentManager, Uid destinationId, Volume source, Volume destination) {
        this.agentManager = agentManager;
        this.destinationId = destinationId;
        this.source = source;
        this.destination = destination;
        events = new ArrayList<>();
    }

    public synchronized void start() {
        stoped = false;
        signal();
    }

    public synchronized void stop() {
        stoped = true;
    }

    private synchronized boolean isStoped() {
        return stoped;
    }

    public synchronized void signal() {
        if (stoped) {
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
        if (isStoped()) {
            return Optional.absent();
        }
        clearSignal();
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
                        events.remove(nextIndex - 1);
                        pos--;
                        break;
                    }
                }
            }
        }
    }

    private class AgentThread extends Thread {

        private int attempt = 1;

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

            } catch (ConcurrentOperationException e) {
                try {
                    Thread.sleep(attempt > 10 ? 10000 : 1000 * attempt);
                    attempt++;
                    process(event);

                } catch (InterruptedException interrupt) {
                    throw new RuntimeException(interrupt);
                }
            } catch (UnknownHashException | VolumeClosedException e) {
                events.add(0, event);
            } finally {
                attempt = 1;
            }
        }

        private void put(Hash hash) {
            ContentInfo info = source.info(hash);
            try (PipedInputStream in = new PipedInputStream()) {
                PipeWriterThread pipeWriter = new PipeWriterThread(in, hash);
                try {
                    pipeWriter.start();
                    destination.put(info, in);

                } catch (IntegrityCheckingFailedException | WriteException e) {
                    pipeWriter.throwCauseIfAny();
                    throw e;
                } catch (ContentAlreadyStoredException e) {
                    // Ok
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void delete(Hash hash) {
            try {
                destination.delete(hash);

            } catch (UnknownHashException e) {
                // Ok
            }
        }
    }

    private class PipeWriterThread extends Thread {

        private final PipedInputStream in;
        private final Hash hash;
        private StoreException storeException;

        public PipeWriterThread(PipedInputStream in, Hash hash) {
            this.in = in;
            this.hash = hash;
        }

        @Override
        public void run() {
            try (PipedOutputStream out = new PipedOutputStream(in)) {
                source.get(hash, out);

            } catch (UnknownHashException |
                    ConcurrentOperationException |
                    VolumeClosedException e) {
                storeException = e;

            } catch (Throwable e) {
                // Ignore it
            }
        }

        public void throwCauseIfAny() throws StoreException {
            if (storeException != null) {
                throw storeException;
            }
        }
    }
}
