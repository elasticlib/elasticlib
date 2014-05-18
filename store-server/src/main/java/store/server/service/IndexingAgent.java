package store.server.service;

import com.google.common.base.Optional;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
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

    public IndexingAgent(Repository repository, Index index, Database cursorsDatabase, DatabaseEntry cursorKey) {
        super("indexation-" + repository.getName(), cursorsDatabase, cursorKey);
        this.repository = repository;
        this.index = index;
    }

    @Override
    protected List<Event> history(boolean chronological, long first, int number) {
        return repository.history(chronological, first, number);
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
