package store.server.service;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import store.common.ContentInfoTree;
import store.common.Event;

/**
 * An agent that performs indexing from a volume to its associated index.
 */
class IndexingAgent extends Agent {

    private final Volume volume;
    private final Index index;

    /**
     * Constructor.
     *
     * @param volume Source volume.
     * @param index Destination index.
     */
    public IndexingAgent(Volume volume, Index index) {
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
            super("indexation-" + volume.getName());
        }

        @Override
        protected boolean process(Event event) {
            ContentInfoTree tree = volume.getInfoTree(event.getHash());
            if (tree.isDeleted()) {
                index.delete(tree.getHash());
                return true;

            } else {
                Optional<InputStream> inputStreamOpt = volume.getContent(tree.getHash(), tree.getHead());
                if (!inputStreamOpt.isPresent()) {
                    return false;
                }
                try (InputStream inputStream = inputStreamOpt.get()) {
                    index.put(tree, inputStream);
                    return true;

                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }
    }
}
