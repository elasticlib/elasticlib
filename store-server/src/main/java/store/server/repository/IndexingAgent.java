package store.server.repository;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.io.IOException;
import java.io.InputStream;
import store.common.model.Event;
import store.common.model.RevisionTree;

/**
 * An agent that performs indexing from a repository to its internal index.
 */
class IndexingAgent extends Agent {

    private final Repository repository;
    private final Index index;

    public IndexingAgent(Repository repository, Index index, Database curSeqsDb, DatabaseEntry curSeqKey) {
        super("indexation-" + repository.getDef().getName(), repository, curSeqsDb, curSeqKey);

        this.repository = repository;
        this.index = index;
    }

    @Override
    protected boolean process(Event event) {
        RevisionTree tree = repository.getTree(event.getContent());
        if (tree.isDeleted()) {
            index.delete(tree.getContent());
            return true;

        } else {
            try (InputStream inputStream = repository.getContent(tree.getContent())) {
                index.index(tree, inputStream);
                return true;

            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }
}
