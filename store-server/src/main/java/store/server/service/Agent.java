package store.server.service;

import com.google.common.base.Optional;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.Event;
import store.server.exception.ServerException;

/**
 * Performs asynchronous replication or indexing.
 */
abstract class Agent {

    private static final Logger LOG = LoggerFactory.getLogger(Agent.class);
    private final Deque<Event> events = new ArrayDeque<>();
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

    private synchronized void abort() {
        running = false;
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
        return Optional.fromNullable(events.pollFirst());
    }

    abstract List<Event> history(boolean chronological, long first, int number);

    abstract AgentThread newAgentThread();

    abstract class AgentThread extends Thread {

        public AgentThread(String name) {
            super(name);
        }

        @Override
        public final void run() {
            try {
                do {
                    Optional<Event> nextEvent = next();
                    while (nextEvent.isPresent()) {
                        Event event = nextEvent.get();
                        if (!process(event)) {
                            events.addFirst(event);
                        }
                        nextEvent = next();
                    }
                } while (!clearRunning());

            } catch (ServerException e) {
                LOG.error("Unexpected error, stopping", e);
                abort();
            }
        }

        protected abstract boolean process(Event event);
    }
}
