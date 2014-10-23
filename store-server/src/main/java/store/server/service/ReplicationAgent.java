package store.server.service;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.io.IOException;
import java.io.InputStream;
import store.common.hash.Hash;
import store.common.model.ContentState;
import store.common.model.Event;
import store.common.model.RevisionTree;
import store.common.model.StagingInfo;
import static store.common.util.IoUtil.copyAndDigest;
import store.common.util.SinkOutputStream;
import store.server.repository.Agent;
import store.server.repository.Repository;

/**
 * An agent that performs replication from a repository to another one.
 */
class ReplicationAgent extends Agent {

    private static final long CHUNK_SIZE = 1024 * 1024;
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
        RevisionTree srcTree = source.getTree(event.getContent());
        ContentState destState = destination.getContentInfo(event.getContent()).getState();
        if (!srcTree.isDeleted() && destState != ContentState.STAGED & destState != ContentState.PRESENT) {
            if (destState == ContentState.STAGING) {
                pause(10);
                return false;
            }
            if (!writeContent(srcTree.getContent(), srcTree.getLength())) {
                return false;
            }
        }
        destination.mergeTree(srcTree);
        return true;
    }

    private boolean writeContent(Hash content, long length) {
        StagingInfo stagingInfo = destination.stageContent(content);
        try {
            stagingInfo = checkDigest(content, stagingInfo);
            while (stagingInfo.getLength() < length) {
                if (isStopped()) {
                    return false;
                }
                stagingInfo = writeChunk(content, stagingInfo);
            }
            return true;

        } catch (IOException e) {
            throw new AssertionError(e);

        } finally {
            destination.unstageContent(content, stagingInfo.getSessionId());
        }
    }

    private StagingInfo checkDigest(Hash content, StagingInfo stagingInfo) throws IOException {
        if (stagingInfo.getLength() == 0) {
            return stagingInfo;
        }
        try (InputStream inputStream = source.getContent(content, 0, stagingInfo.getLength())) {
            Hash expected = stagingInfo.getHash();
            Hash actual = copyAndDigest(inputStream, SinkOutputStream.sink()).getHash();

            return expected.equals(actual) ? stagingInfo : new StagingInfo(stagingInfo.getSessionId(), null, 0L);
        }
    }

    private StagingInfo writeChunk(Hash content, StagingInfo stagingInfo) throws IOException {
        try (InputStream inputStream = source.getContent(content,
                                                         stagingInfo.getLength(),
                                                         stagingInfo.getLength() + CHUNK_SIZE)) {

            return destination.writeContent(content,
                                            stagingInfo.getSessionId(),
                                            inputStream,
                                            stagingInfo.getLength());
        }
    }
}
