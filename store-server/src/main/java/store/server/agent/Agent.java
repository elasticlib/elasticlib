package store.server.agent;

import com.google.common.base.Optional;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import store.common.Event;
import store.common.Hash;
import store.common.Operation;
import store.server.exception.StoreException;
import store.server.exception.UnknownHashException;
import store.server.exception.VolumeClosedException;
import store.server.volume.Volume;

abstract class Agent {

    protected final Volume volume;
    private final List<Event> events;
    private long cursor;
    private boolean signaled;
    private boolean stoped;
    private boolean running;

    public Agent(Volume volume) {
        this.volume = volume;
        events = new ArrayList<>();
    }

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

    private final synchronized boolean isStoped() {
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
        updateEvents();
        if (events.isEmpty()) {
            return Optional.absent();
        }
        return Optional.of(events.remove(0));
    }

    private void updateEvents() {
        List<Event> chunk = volume.history(true, cursor, 1000);
        while (!chunk.isEmpty()) {
            events.addAll(chunk);
            cursor = chunk.get(chunk.size() - 1).getSeq();
            chunk = volume.history(true, cursor, 1000);
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

    protected abstract AgentThread newAgentThread();

    protected abstract class AgentThread extends Thread {

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
        private StoreException storeException;

        public PipeWriterThread(PipedInputStream in, Hash hash) {
            this.in = in;
            this.hash = hash;
        }

        @Override
        public void run() {
            try (PipedOutputStream out = new PipedOutputStream(in)) {
                volume.get(hash, out);

            } catch (UnknownHashException | VolumeClosedException e) {
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
