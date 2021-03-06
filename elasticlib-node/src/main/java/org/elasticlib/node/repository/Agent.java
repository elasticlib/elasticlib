/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.node.repository;

import static com.google.common.collect.Iterables.getLast;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.exception.IOFailureException;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.exception.RepositoryClosedException;
import org.elasticlib.common.exception.UnexpectedFailureException;
import org.elasticlib.common.exception.UnreachableNodeException;
import org.elasticlib.common.model.AgentInfo;
import org.elasticlib.common.model.AgentState;
import org.elasticlib.common.model.Event;
import static org.elasticlib.node.config.NodeConfig.AGENTS_HISTORY_FETCH_SIZE;
import org.elasticlib.node.dao.CurSeqsDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks a repository and performs a task for each event of his history.
 */
public abstract class Agent {

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
     * @param config Configuration holder.
     * @param repository Tracked repository.
     * @param curSeqsDao The agents sequences DAO.
     * @param curSeqKey The key persisted agent curSeq value is associated with in curSeqsDao.
     */
    protected Agent(String name, Config config, Repository repository, CurSeqsDao curSeqsDao, String curSeqKey) {
        this.repository = repository;
        agentThread = new AgentThread(name, config, curSeqsDao, curSeqKey);
    }

    /**
     * Callback called to process a given event in the tracked repository.
     *
     * @param event An event from tracked repository history.
     * @return True if supplied event was succesfully processed, false otherwise.
     */
    protected abstract boolean process(Event event);

    /**
     * Checks whether this agent has been stopped.
     *
     * @return True if this the case.
     */
    protected boolean isStopped() {
        lock.lock();
        try {
            return stopped;

        } finally {
            lock.unlock();
        }
    }

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

    /**
     * Thread responsible for executing agent tasks.
     */
    private class AgentThread extends Thread {

        private final Config config;
        private final CurSeqsDao curSeqsDao;
        private final String curSeqKey;
        private final Deque<Event> events;
        private final AtomicReference<AgentInfo> info;
        private long curSeq;
        private long maxSeq;

        /**
         * Constructor.
         *
         * @param name Agent name.
         * @param config Configuration holder.
         * @param curSeqsDao The agents sequences DAO.
         * @param curSeqKey The key persisted agent curSeq value is associated with in curSeqsDao.
         */
        public AgentThread(String name, Config config, CurSeqsDao curSeqsDao, String curSeqKey) {
            super(name);
            this.config = config;
            this.curSeqsDao = curSeqsDao;
            this.curSeqKey = curSeqKey;
            events = new ArrayDeque<>(config.getInt(AGENTS_HISTORY_FETCH_SIZE));
            info = new AtomicReference<>(new AgentInfo(curSeq, maxSeq, AgentState.NEW));
        }

        /**
         * @return A snapshot of current info about this agent.
         */
        public AgentInfo info() {
            return info.get();
        }

        @Override
        public final void run() {
            try {
                curSeq = curSeqsDao.load(curSeqKey);
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

            } catch (UnreachableNodeException e) {
                LOG.info("Remote node unreachable, stopping");
                updateInfo(AgentState.STOPPED);

            } catch (NodeException e) {
                LOG.error("Unexpected error, stopping", e);
                updateInfo(AgentState.ERROR);
            }
        }

        private boolean tryProcess(Event event) {
            try {
                return process(event);

            } catch (IOFailureException |
                    UnexpectedFailureException |
                    RepositoryClosedException |
                    UnreachableNodeException e) {
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
                    return Optional.empty();
                }
                return Optional.of(events.removeFirst());

            } finally {
                lock.unlock();
            }
        }

        private void fetchEvents() {
            int fetchSize = config.getInt(AGENTS_HISTORY_FETCH_SIZE);
            List<Event> chunk = repository.history(true, curSeq + 1, fetchSize);
            events.addAll(chunk);

            if (chunk.size() == fetchSize) {
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
            curSeqsDao.save(curSeqKey, curSeq);
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
