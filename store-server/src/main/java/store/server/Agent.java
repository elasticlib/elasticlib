package store.server;

import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.List;
import store.common.Event;

/**
 * Performs asynchronous replication or indexing.
 */
public abstract class Agent {

    private final List<Event> events = new ArrayList<>();
    private long cursor;
    private boolean signaled;
    private boolean stoped;
    private boolean running;

    /**
     * Start this agent.
     */
    public final synchronized void start() {
        stoped = false;
        signal();
    }

    /**
     * Stop this agent.
     */
    public final synchronized void stop() {
        stoped = true;
    }

    /**
     * Signal this agent that a change may have happen on its source.
     */
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
}
