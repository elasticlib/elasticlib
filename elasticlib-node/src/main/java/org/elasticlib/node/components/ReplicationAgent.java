/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.node.components;

import java.io.IOException;
import java.io.InputStream;
import static java.lang.Math.min;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.ContentState;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;
import static org.elasticlib.node.config.NodeConfig.AGENTS_CONTENT_CHUNK_SIZE;
import org.elasticlib.node.dao.CurSeqsDao;
import org.elasticlib.node.repository.Agent;
import org.elasticlib.node.repository.Repository;

/**
 * An agent that performs replication from a repository to another one.
 */
class ReplicationAgent extends Agent {

    private final Config config;
    private final Repository source;
    private final Repository destination;

    /**
     * Constructor.
     *
     * @param guid Replication guid.
     * @param config Configuration holder.
     * @param source Source repository.
     * @param destination Destination repository.
     * @param curSeqsDao The agents sequences DAO.
     * @param curSeqKey The key persisted agent curSeq value is associated with in curSeqsDao.
     */
    public ReplicationAgent(Guid guid,
                            Config config,
                            Repository source,
                            Repository destination,
                            CurSeqsDao curSeqsDao,
                            String curSeqKey) {

        super("replication-" + guid.asHexadecimalString(), config, source, curSeqsDao, curSeqKey);

        this.config = config;
        this.source = source;
        this.destination = destination;
    }

    @Override
    protected boolean process(Event event) {
        RevisionTree srcTree = source.getTree(event.getContent());
        ContentState destState = destination.getContentInfo(event.getContent()).getState();
        if (!srcTree.isDeleted() && destState != ContentState.STAGED && destState != ContentState.PRESENT) {
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
                stagingInfo = writeChunk(content, length, stagingInfo);
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
        Hash expected = stagingInfo.getHash();
        Hash actual = source.getDigest(content, 0, stagingInfo.getLength()).getHash();

        return expected.equals(actual) ? stagingInfo : new StagingInfo(stagingInfo.getSessionId(), null, 0L);
    }

    private StagingInfo writeChunk(Hash content, long totalLength, StagingInfo stagingInfo) throws IOException {
        long offset = stagingInfo.getLength();
        long length = min(config.getInt(AGENTS_CONTENT_CHUNK_SIZE), totalLength - offset);

        try (InputStream inputStream = source.getContent(content, offset, length)) {
            return destination.writeContent(content, stagingInfo.getSessionId(), inputStream, offset);
        }
    }
}
