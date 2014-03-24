package store.server.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.Hash;
import static store.common.Operation.DELETE;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.UnknownContentException;
import store.server.exception.WriteException;

/**
 * An agent that performs indexing from a volume to its associated index.
 */
class IndexingAgent extends Agent {

    private final String name;
    private final Volume volume;
    private final Index index;

    /**
     * Constructor.
     *
     * @param name Repository name.
     * @param volume Source volume.
     * @param index Destination index.
     */
    public IndexingAgent(String name, Volume volume, Index index) {
        this.name = name;
        this.volume = volume;
        this.index = index;
    }

    @Override
    List<Event> history(boolean chronological, long first, int number) {
        return volume.history(chronological, first, number);
    }

    @Override
    AgentThread newAgentThread() {
        return this.new IndexingAgentThread();
    }

    private class IndexingAgentThread extends AgentThread {

        public IndexingAgentThread() {
            super("indexation-" + name);
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
            ContentInfoTree contentInfoTree = volume.getInfoTree(hash);
            try (InputStream inputStream = volume.getContent(hash)) {
                index.put(contentInfoTree, inputStream);

            } catch (IOException e) {
                throw new WriteException(e);
            }
        }

        private void delete(Hash hash) {
            index.delete(hash); // Idempotent
        }
    }
}