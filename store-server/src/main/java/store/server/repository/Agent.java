package store.server.repository;

import com.google.common.base.Optional;
import static com.google.common.collect.Iterables.getLast;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.exception.IOFailureException;
import store.common.exception.NodeException;
import store.common.exception.RepositoryClosedException;
import store.common.exception.UnexpectedFailureException;
import store.common.model.AgentInfo;
import store.common.model.AgentState;
import store.common.model.Event;
import static store.server.manager.storage.DatabaseEntries.asLong;
import static store.server.manager.storage.DatabaseEntries.entry;

/**
 * Tracks a repository and performs a task for each event of his history.
 */
public abstract class Agent {

    private static final int FETCH_SIZE = 20;
    private static final Logger LOG = LoggerFactory.getLogger(Agent.class);
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final Repository repository;
    private final AgentThread agentThread;
    private boolean signaled;
    private boolean stopped;

    /**
     * Constructor.
     *
     * @param name Agent name.
     * @param repository Tracked repository.
     * @param curSeqsDb Database used to persist agent curSeq value.
     * @param curSeqKey The key persisted agent curSeq value is associated with in curSeqs Database.
     */
    protected Agent(String name, Repository repository, Database curSeqsDb, DatabaseEntry curSeqKey) {
        this.repository = repository;
        agentThread = new AgentThread(name, curSeqsDb, curSeqKey);
    }

    /**
     * Callback called to process a given event in the tracked repository.
     *
     * @param event An event from tracked repository history.
     * @return True if supplied event was succesfully processed, false otherwise.
     */
    protected abstract boolean process(Event event);

    /**
     * Causes processing thread to wait.
     *
     * @param seconds The time to wait in seconds.
     */
    protected void pause(long seconds) {
        agentThread.pause(seconds);
    }

    /**
     * Starts this agent.
     */
    public final void start() {
        agentThread.start();
    }

    /**
     * Stops this agent. Waits for the underlying processing thread to terminates before returning. This allows any
     * indexing agent to properly close its underlying writer on target index (and to complete any related merge task).
     * In the case of a replication deletion, it also avoids to have agent trying to override the cursor database after
     * this latter has been reset.
     */
    public final void stop() {
        lock.lock();
        try {
            stopped = true;
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
     * Signals this agent that a change may have happen on its source.
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

    /**
     * Provides info about this agent.
     *
     * @return A snapshot of current info about this agent.
     */
    public final AgentInfo info() {
        return agentThread.info();
    }

    private class AgentThread extends Thread {

        private final Database curSeqsDb;
        private final DatabaseEntry curSeqKey;
        private final Deque<Event> events = new ArrayDeque<>(FETCH_SIZE);
        private final AtomicReference<AgentInfo> info;
        private long curSeq;
        private long maxSeq;

        public AgentThread(String name, Database curSeqsDb, DatabaseEntry curSeqKey) {
            super(name);
            this.curSeqsDb = curSeqsDb;
            this.curSeqKey = curSeqKey;
            curSeq = loadCurSeq();
            info = new AtomicReference<>(new AgentInfo(curSeq, maxSeq, AgentState.NEW));
        }

        private long loadCurSeq() {
            DatabaseEntry entry = new DatabaseEntry();
            if (curSeqsDb.get(null, curSeqKey, entry, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                return asLong(entry);
            }
            return 0;
        }

        public AgentInfo info() {
            return info.get();
        }

        @Override
        public final void run() {
            try {
                Optional<Event> nextEvent = next();
                while (nextEvent.isPresent()) {
                    Event event = nextEvent.get();
                    if (!tryProcess(event)) {
                        events.addFirst(event);
                    } else {
                        updateCurSeq(event.getSeq());
                    }
                    nextEvent = next();
                }
            } catch (RepositoryClosedException e) {
                LOG.info("Repository closed, stopping");
                updateInfo(AgentState.STOPPED);

            } catch (NodeException e) {
                LOG.error("Unexpected error, stopping", e);
                updateInfo(AgentState.ERROR);
            }
        }

        private boolean tryProcess(Event event) {
            try {
                return process(event);

            } catch (IOFailureException | UnexpectedFailureException | RepositoryClosedException e) {
                throw e;

            } catch (NodeException e) {
                LOG.warn("Failed to process event " + event.getSeq(), e);
                return false;
            }
        }

        private Optional<Event> next() {
            lock.lock();
            try {
                while (!stopped && events.isEmpty()) {
                    fetchEvents();
                    if (events.isEmpty()) {
                        signaled = false;
                        while (!stopped && !signaled) {
                            updateInfo(AgentState.WAITING);
                            condition.awaitUninterruptibly();
                            updateInfo(AgentState.RUNNING);
                        }
                    }
                }
                if (stopped) {
                    return Optional.absent();
                }
                return Optional.of(events.removeFirst());

            } finally {
                lock.unlock();
            }
        }

        private void fetchEvents() {
            List<Event> chunk = repository.history(true, curSeq + 1, FETCH_SIZE);
            events.addAll(chunk);

            if (chunk.size() == FETCH_SIZE) {
                maxSeq = repository.history(false, Long.MAX_VALUE, 1).get(0).getSeq();

            } else if (!chunk.isEmpty()) {
                maxSeq = getLast(chunk).getSeq();

            } else {
                maxSeq = curSeq;
            }
            updateInfo(AgentState.RUNNING);
        }

        private void updateCurSeq(long c) {
            curSeq = c;
            curSeqsDb.put(null, curSeqKey, entry(curSeq));
            updateInfo(AgentState.RUNNING);
        }

        private void updateInfo(AgentState state) {
            info.set(new AgentInfo(curSeq, maxSeq, state));
        }

        /**
         * Pauses execution.
         *
         * @param seconds The time to wait in seconds.
         */
        public void pause(long seconds) {
            try {
                updateInfo(AgentState.WAITING);
                condition.await(seconds, TimeUnit.SECONDS);
                updateInfo(AgentState.RUNNING);

            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }
}
