package store.server.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.UnknownContentException;
import store.server.exception.WriteException;

/**
 * An agent that performs replication from a repository to another one.
 */
class ReplicationAgent extends Agent {

    private final Repository source;
    private final Repository destination;

    /**
     * Constructor.
     *
     * @param source Source repository.
     * @param destination Destination repository.
     */
    public ReplicationAgent(Repository source, Repository destination) {
        this.source = source;
        this.destination = destination;
    }

    @Override
    List<Event> history(boolean chronological, long first, int number) {
        return source.history(chronological, first, number);
    }

    @Override
    AgentThread newAgentThread() {
        return this.new ReplicationAgentThread();
    }

    private class ReplicationAgentThread extends AgentThread {

        public ReplicationAgentThread() {
            super("replication-" + source.getName() + ">" + destination.getName());
        }

        @Override
        protected boolean process(Event event) {
            try {
                switch (event.getOperation()) {
                    case CREATE:
                    case UPDATE:
                        put(event.getHash());
                        break;
                    case DELETE:
                        delete(event.getHash());
                        break;
                }
                return true;

            } catch (UnknownContentException | RepositoryNotStartedException e) {
                return false;
            }
        }

        private void put(Hash hash) {
            ContentInfo info = source.getInfoHead(hash).get(0);
            try (InputStream inputStream = source.getContent(hash)) {
                destination.put(info, inputStream, RevSpec.any());

            } catch (IOException e) {
                throw new WriteException(e);
            }
        }

        private void delete(Hash hash) {
            try {
                destination.delete(hash, RevSpec.any());

            } catch (UnknownContentException e) {
                // Ok
            }
        }
    }
}
