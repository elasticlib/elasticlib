package store.server.service;

import com.google.common.base.Optional;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.exception.IOFailureException;
import store.common.exception.NodeException;
import store.common.exception.RepositoryClosedException;
import store.common.exception.UnexpectedFailureException;
import store.common.model.ContentState;
import store.common.model.Event;
import store.common.model.RevisionTree;
import store.common.model.StagingInfo;
import store.server.repository.Agent;
import store.server.repository.Repository;

/**
 * An agent that performs replication from a repository to another one.
 */
class ReplicationAgent extends Agent {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationAgent.class);
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
        try {
            RevisionTree srcTree = source.getTree(event.getContent());
            ContentState destState = destination.getContentInfo(event.getContent()).getState();
            return process(srcTree, destState);

        } catch (IOFailureException | UnexpectedFailureException | RepositoryClosedException e) {
            throw e;

        } catch (NodeException e) {
            LOG.warn("Failed to process event " + event.getSeq(), e);
            return false;
        }
    }

    private boolean process(RevisionTree srcTree, ContentState destState) {
        if (srcTree.isDeleted() || destState == ContentState.STAGED || destState == ContentState.PRESENT) {
            destination.mergeTree(srcTree);
            return true;
        }
        if (destState == ContentState.STAGING) {
            pause(10);
            return false;
        }

        Optional<InputStream> inputStreamOpt = source.getContent(srcTree.getContent(), srcTree.getHead());
        if (!inputStreamOpt.isPresent()) {
            return false;
        }
        try (InputStream inputStream = inputStreamOpt.get()) {
            StagingInfo stagingInfo = destination.stageContent(srcTree.getContent());
            destination.writeContent(srcTree.getContent(), stagingInfo.getSessionId(), inputStream, 0);
            destination.mergeTree(srcTree);
            return true;

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
