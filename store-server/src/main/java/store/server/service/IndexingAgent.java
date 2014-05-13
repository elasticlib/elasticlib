package store.server.service;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import store.common.ContentInfoTree;
import store.common.Event;

/**
 * An agent that performs indexing from a repository to its internal index.
 */
class IndexingAgent extends Agent {

    private final Repository repository;
    private final Index index;

    /**
     * Constructor.
     *
     * @param repository Repository.
     * @param index its index.
     */
    public IndexingAgent(Repository repository, Index index) {
        this.repository = repository;
        this.index = index;
    }

    @Override
    List<Event> history(boolean chronological, long first, int number) {
        return repository.history(chronological, first, number);
    }

    @Override
    AgentThread newAgentThread() {
        return this.new IndexingAgentThread();
    }

    private class IndexingAgentThread extends AgentThread {

        public IndexingAgentThread() {
            super("indexation-" + repository.getName());
        }

        @Override
        protected boolean process(Event event) {
            ContentInfoTree tree = repository.getInfoTree(event.getContent());
            if (tree.isDeleted()) {
                index.delete(tree.getContent());
                return true;

            } else {
                Optional<InputStream> inputStreamOpt = repository.getContent(tree.getContent(), tree.getHead());
                if (!inputStreamOpt.isPresent()) {
                    return false;
                }
                try (InputStream inputStream = inputStreamOpt.get()) {
                    index.index(tree, inputStream);
                    return true;

                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }
    }
}
