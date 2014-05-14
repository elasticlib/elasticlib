package store.server.service;

import com.google.common.base.Optional;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.Event;
import store.server.exception.ServerException;
import static store.server.storage.DatabaseEntries.asLong;
import static store.server.storage.DatabaseEntries.entry;

/**
 * Performs asynchronous replication or indexing.
 */
abstract class Agent {

    private static final Logger LOG = LoggerFactory.getLogger(Agent.class);
    private final Database cursorsDatabase;
    private final String name;
    private final Deque<Event> events = new ArrayDeque<>();
    private long cursor;
    private boolean signaled;
    private boolean stoped;
    private boolean running;

    /**
     * Constructor.
     *
     * @param name Agent name.
     * @param cursorsDatabase Database used to persist agent cursor value.
     */
    protected Agent(String name, Database cursorsDatabase) {
        this.name = name;
        this.cursorsDatabase = cursorsDatabase;

        DatabaseEntry entry = new DatabaseEntry();
        if (cursorsDatabase.get(null, entry(name), entry, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            cursor = asLong(entry);
        }
    }

    /**
     * Start this agent.
     */
    public final void start() {
        signal();
    }

    /**
     * Stop this agent.
     */
    public final synchronized void stop() {
        signaled = false;
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
            events.addAll(history(true, cursor, 100));
        }
        return Optional.fromNullable(events.pollFirst());
    }

    abstract List<Event> history(boolean chronological, long first, int number);

    abstract AgentThread newAgentThread();

    abstract class AgentThread extends Thread {

        public AgentThread() {
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
                        } else {
                            cursor = event.getSeq() + 1;
                            cursorsDatabase.put(null, entry(name), entry(cursor));
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
