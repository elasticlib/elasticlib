package store.server.repository;

import com.google.common.base.Optional;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.io.IOException;
import java.io.InputStream;
import store.common.ContentInfoTree;
import store.common.Event;

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
        ContentInfoTree tree = repository.getContentInfoTree(event.getContent());
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
