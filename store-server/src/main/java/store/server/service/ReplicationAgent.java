package store.server.service;

import com.google.common.base.Optional;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.io.IOException;
import java.io.InputStream;
import store.common.CommandResult;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.Operation;
import store.server.repository.Agent;
import store.server.repository.Repository;

/**
 * An agent that performs replication from a repository to another one.
 */
class ReplicationAgent extends Agent {

    private final Repository source;
    private final Repository destination;

    public ReplicationAgent(Repository source, Repository destination, Database curSeqsDb, DatabaseEntry curSeqKey) {
        super("replication-" + source.getDef().getName() + ">" + destination.getDef().getName(),
              source, curSeqsDb, curSeqKey);

        this.source = source;
        this.destination = destination;
    }

    @Override
    protected boolean process(Event event) {
        ContentInfoTree tree = source.getContentInfoTree(event.getContent());
        if (tree.isDeleted()) {
            destination.mergeContentInfoTree(tree);
            return true;

        } else {
            Optional<InputStream> inputStreamOpt = source.getContent(tree.getContent(), tree.getHead());
            if (!inputStreamOpt.isPresent()) {
                return false;
            }
            try (InputStream inputStream = inputStreamOpt.get()) {
                CommandResult result = destination.mergeContentInfoTree(tree);
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
