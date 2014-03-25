package store.server.service;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import store.common.ContentInfoTree;
import store.common.Event;

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
            ContentInfoTree tree = source.getInfoTree(event.getHash());
            if (tree.isDeleted()) {
                destination.put(tree, RevSpec.any());
                return true;

            } else {
                Optional<InputStream> inputStreamOpt = source.getContent(tree.getHash(), tree.getHead());
                if (!inputStreamOpt.isPresent()) {
                    return false;
                }
                try (InputStream inputStream = inputStreamOpt.get()) {
                    destination.put(tree, inputStream, RevSpec.any());
                    return true;

                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }
    }
}
