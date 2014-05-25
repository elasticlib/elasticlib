package store.server.service;

import com.google.common.base.Optional;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final Thread agentThread;
    private final Database cursorsDatabase;
    private final DatabaseEntry cursorKey;
    private final String name;
    private final Deque<Event> events = new ArrayDeque<>();
    private long cursor;
    private boolean signaled;
    private boolean stoped;

    /**
     * Constructor.
     *
     * @param name Agent name.
     * @param cursorsDatabase Database used to persist agent cursor value.
     * @param cursorKey The key persisted agent cursor value is associated to in cursors Database.
     */
    protected Agent(String name, Database cursorsDatabase, DatabaseEntry cursorKey) {
        this.name = name;
        this.cursorsDatabase = cursorsDatabase;
        this.cursorKey = cursorKey;

        DatabaseEntry entry = new DatabaseEntry();
        if (cursorsDatabase.get(null, cursorKey, entry, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            cursor = asLong(entry);
        }
        agentThread = new AgentThread();
    }

    /**
     * Start this agent.
     */
    public final void start() {
        agentThread.start();
    }

    /**
     * Stop this agent. Waits for underlying processing thread to terminates before returning. This allows any indexing
     * agent to properly close its underlying writer on target index (and to complete any related merge task). In the
     * case of a replication deletion, it also avoids to have agent trying to override the cursor database after this
     * latter has been reset.
     */
    public final void stop() {
        lock.lock();
        try {
            stoped = true;
            condition.signal();

        } finally {
            lock.unlock();
        }
        try {
            agentThread.join();

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Signal this agent that a change may have happen on its source.
     */
    public final void signal() {
        lock.lock();
        try {
            signaled = true;
            condition.signal();

        } finally {
            lock.unlock();
        }
    }

    protected abstract List<Event> history(boolean chronological, long first, int number);

    protected abstract boolean process(Event event);

    private class AgentThread extends Thread {

        public AgentThread() {
            super(name);
        }

        @Override
        public final void run() {
            try {
                Optional<Event> nextEvent = next();
                while (nextEvent.isPresent()) {
                    Event event = nextEvent.get();
                    if (!process(event)) {
                        events.addFirst(event);
                    } else {
                        cursor = event.getSeq() + 1;
                        cursorsDatabase.put(null, cursorKey, entry(cursor));
                    }
                    nextEvent = next();
                }
            } catch (ServerException e) {
                LOG.error("Unexpected error, stopping", e);
            }
        }

        private Optional<Event> next() {
            lock.lock();
            try {
                while (!stoped && events.isEmpty()) {
                    events.addAll(history(true, cursor, 100));
                    if (events.isEmpty()) {
                        signaled = false;
                        while (!stoped && !signaled) {
                            condition.awaitUninterruptibly();
                        }
                    }
                }
                if (stoped) {
                    return Optional.absent();
                }
                return Optional.of(events.removeFirst());

            } finally {
                lock.unlock();
            }
        }
    }
}
