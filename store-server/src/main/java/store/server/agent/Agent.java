package store.server.agent;

import com.google.common.base.Optional;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import store.common.Event;
import store.common.Hash;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.ServerException;
import store.server.exception.UnknownContentException;

abstract class Agent {

    private final List<Event> events = new ArrayList<>();
    private long cursor;
    private boolean signaled;
    private boolean stoped;
    private boolean running;

    public final synchronized void start() {
        stoped = false;
        signal();
    }

    public final synchronized void stop() {
        stoped = true;
    }

    public final synchronized void signal() {
        if (stoped) {
            return;
        }
        signaled = true;
        if (!running) {
            running = true;
            newAgentThread().start();
        }
    }

    private synchronized boolean isStoped() {
        return stoped;
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
        if (events.isEmpty()) {
            List<Event> chunk = history(true, cursor, 100);
            events.addAll(chunk);
            if (!chunk.isEmpty()) {
                cursor = chunk.get(chunk.size() - 1).getSeq();
            }
        }
        if (events.isEmpty()) {
            return Optional.absent();
        }
        return Optional.of(events.remove(0));
    }

    abstract List<Event> history(boolean chronological, long first, int number);

    abstract void get(Hash hash, OutputStream outputStream);

    abstract AgentThread newAgentThread();

    abstract class AgentThread extends Thread {

        @Override
        public final void run() {
            do {
                Optional<Event> nextEvent = next();
                while (nextEvent.isPresent()) {
                    Event event = nextEvent.get();
                    if (!process(event)) {
                        events.add(0, event);
                    }
                    nextEvent = next();
                }
            } while (!clearRunning());
        }

        protected abstract boolean process(Event event);
    }

    protected final class PipeWriterThread extends Thread {

        private final PipedInputStream in;
        private final Hash hash;
        private ServerException storeException;

        public PipeWriterThread(PipedInputStream in, Hash hash) {
            this.in = in;
            this.hash = hash;
        }

        @Override
        public void run() {
            try (PipedOutputStream out = new PipedOutputStream(in)) {
                get(hash, out);

            } catch (UnknownContentException | RepositoryNotStartedException e) {
                storeException = e;

            } catch (Exception e) {
                // Ignore it
            }
        }

        public void throwCauseIfAny() {
            if (storeException != null) {
                throw storeException;
            }
        }
    }
}
