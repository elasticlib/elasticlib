package store.server.service;

import com.google.common.base.Optional;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import store.common.CommandResult;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.Operation;

/**
 * An agent that performs replication from a repository to another one.
 */
class ReplicationAgent extends Agent {

    private final Repository source;
    private final Repository destination;

    public ReplicationAgent(Repository source, Repository destination, Database curSeqsDb, DatabaseEntry curSeqKey) {
        super("replication-" + source.getName() + ">" + destination.getName(), curSeqsDb, curSeqKey);
        this.source = source;
        this.destination = destination;
    }

    @Override
    protected List<Event> history(boolean chronological, long first, int number) {
        return source.history(chronological, first, number);
    }

    @Override
    protected boolean process(Event event) {
        ContentInfoTree tree = source.getInfoTree(event.getContent());
        if (tree.isDeleted()) {
            destination.mergeTree(tree);
            return true;

        } else {
            Optional<InputStream> inputStreamOpt = source.getContent(tree.getContent(), tree.getHead());
            if (!inputStreamOpt.isPresent()) {
                return false;
            }
            try (InputStream inputStream = inputStreamOpt.get()) {
                CommandResult result = destination.mergeTree(tree);
                if (!result.isNoOp() && result.getOperation() == Operation.CREATE) {
                    destination.addContent(result.getTransactionId(), tree.getContent(), inputStream);
                }
                return true;

            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }
}
