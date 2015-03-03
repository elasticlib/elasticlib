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
package org.elasticlib.common;

import com.google.common.collect.ImmutableMap;
import java.io.FileNotFoundException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.TreeSet;
import org.elasticlib.common.exception.IOFailureException;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.exception.UnexpectedFailureException;
import org.elasticlib.common.exception.UnknownRepositoryException;
import org.elasticlib.common.hash.Digest.DigestBuilder;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.AgentInfo;
import org.elasticlib.common.model.AgentState;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.ContentState;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.Event.EventBuilder;
import org.elasticlib.common.model.IndexEntry;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.common.model.Operation;
import org.elasticlib.common.model.RemoteInfo;
import org.elasticlib.common.model.ReplicationDef;
import org.elasticlib.common.model.ReplicationInfo;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.RepositoryStats;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.Revision.RevisionBuilder;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.RevisionTree.RevisionTreeBuilder;
import org.elasticlib.common.model.StagingInfo;
import org.elasticlib.common.value.Value;

/**
 * Test data.
 */
public final class TestData {

    /**
     * A StagingInfo instance.
     */
    public static final StagingInfo STAGING_INFO;
    /**
     * Some Revision instances.
     */
    public static final List<Revision> REVISIONS = new ArrayList<>();
    /**
     * A RevisionTree instance.
     */
    public static final RevisionTree REVISION_TREE;
    /**
     * A ContentInfo instance.
     */
    public static final ContentInfo CONTENT_INFO;
    /**
     * Some Event instances.
     */
    public static final List<Event> EVENTS = new ArrayList<>();
    /**
     * Some CommandResult instances.
     */
    public static final List<CommandResult> COMMAND_RESULTS = new ArrayList<>();
    /**
     * Some IndexEntry instances.
     */
    public static final List<IndexEntry> INDEX_ENTRIES = new ArrayList<>();
    /**
     * Some RepositoryDef instances.
     */
    public static final List<RepositoryDef> REPOSITORY_DEFS = new ArrayList<>();
    /**
     * Some ReplicationDef instances.
     */
    public static final List<ReplicationDef> REPLICATION_DEFS = new ArrayList<>();
    /**
     * Some RepositoryInfo instances.
     */
    public static final List<RepositoryInfo> REPOSITORY_INFOS = new ArrayList<>();
    /**
     * Some ReplicationInfo instances.
     */
    public static final List<ReplicationInfo> REPLICATION_INFOS = new ArrayList<>();
    /**
     * Some NodeDef instances.
     */
    public static final List<NodeDef> NODE_DEFS = new ArrayList<>();
    /**
     * Some NodeInfo instances.
     */
    public static final List<NodeInfo> NODE_INFOS = new ArrayList<>();
    /**
     * Some RemoteInfo instances.
     */
    public static final List<RemoteInfo> REMOTE_INFOS = new ArrayList<>();
    /**
     * Some NodeException instances.
     */
    public static final List<NodeException> NODE_EXCEPTIONS = new ArrayList<>();

    static {
        String[] HASHES = new String[]{"8d5f3c77e94a0cad3a32340d342135f43dbb7cbb",
                                       "0827c43f0aad546501f99b11f0bd44be42d68870",
                                       "39819150ee99549a8c0a59782169bb3be65b46a4"};

        String[] REVS = new String[]{"0d99dd9895a2a1c485e0c75f79f92cc14457bb62",
                                     "a0b87ac4b04a0bed394517d0b01792635531aa42"};

        Guid[] GUIDS = new Guid[]{new Guid("8d5f3c77e94a0cad3a32340d342135f4"),
                                  new Guid("0d99dd9895a2a1c485e0c75f79f92cc1"),
                                  new Guid("39819150ee99549a8c0a59782169bb3b"),
                                  new Guid("eac7690f2ca05940e9239d5300037551"),
                                  new Guid("da8d63a4a8bd8760a203b18a948fab75")};

        STAGING_INFO = new StagingInfo(GUIDS[0], new Hash(HASHES[1]), 123456L);

        REVISIONS.add(new RevisionBuilder()
                .withContent(new Hash(HASHES[0]))
                .withLength(10)
                .with("text", Value.of("ipsum lorem"))
                .build(new Hash(REVS[0])));

        REVISIONS.add(new RevisionBuilder()
                .withContent(new Hash(HASHES[1]))
                .withLength(120)
                .withParent(new Hash(REVS[0]))
                .build(new Hash(REVS[1])));

        REVISION_TREE = new RevisionTreeBuilder()
                .add(REVISIONS.get(0))
                .add(new RevisionBuilder()
                        .withContent(new Hash(HASHES[0]))
                        .withLength(10)
                        .withParent(new Hash(REVS[0]))
                        .build(new Hash(REVS[1])))
                .build();

        CONTENT_INFO = new ContentInfo(ContentState.PRESENT,
                                       new DigestBuilder().getHash(),
                                       0,
                                       singletonList(REVISIONS.get(0)));

        EVENTS.add(new EventBuilder()
                .withSeq(0)
                .withContent(new Hash(HASHES[0]))
                .withRevisions(new TreeSet<>(singleton(new Hash(REVS[0]))))
                .withTimestamp(Instant.EPOCH)
                .withOperation(Operation.CREATE)
                .build());

        EVENTS.add(new EventBuilder()
                .withSeq(1)
                .withContent(new Hash(HASHES[1]))
                .withRevisions(new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1]))))
                .withTimestamp(Instant.ofEpochMilli(123000))
                .withOperation(Operation.DELETE)
                .build());

        COMMAND_RESULTS.add(CommandResult.of(Operation.CREATE,
                                             new Hash(HASHES[0]),
                                             new TreeSet<>(singleton(new Hash(REVS[0])))));

        COMMAND_RESULTS.add(CommandResult.noOp(new Hash(HASHES[1]),
                                               new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1])))));

        INDEX_ENTRIES.add(new IndexEntry(new Hash(HASHES[0]),
                                         new TreeSet<>(singleton(new Hash(REVS[0])))));

        INDEX_ENTRIES.add(new IndexEntry(new Hash(HASHES[1]),
                                         new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1])))));

        REPOSITORY_DEFS.add(new RepositoryDef("primary",
                                              GUIDS[0],
                                              "/repo/primary"));

        REPOSITORY_DEFS.add(new RepositoryDef("secondary",
                                              GUIDS[1],
                                              "/repo/secondary"));

        REPLICATION_DEFS.add(new ReplicationDef(GUIDS[3], GUIDS[0], GUIDS[1]));
        REPLICATION_DEFS.add(new ReplicationDef(GUIDS[4], GUIDS[1], GUIDS[0]));

        REPOSITORY_INFOS.add(new RepositoryInfo(REPOSITORY_DEFS.get(0),
                                                new RepositoryStats(12, 3, 2, ImmutableMap.of("contentType", 9L,
                                                                                              "description", 5L)),
                                                new AgentInfo(15, 17, AgentState.RUNNING),
                                                new AgentInfo(17, 17, AgentState.WAITING)));

        REPOSITORY_INFOS.add(new RepositoryInfo(REPOSITORY_DEFS.get(1)));

        REPLICATION_INFOS.add(new ReplicationInfo(GUIDS[3],
                                                  REPOSITORY_DEFS.get(0),
                                                  REPOSITORY_DEFS.get(1),
                                                  new AgentInfo(15, 17, AgentState.RUNNING)));

        REPLICATION_INFOS.add(new ReplicationInfo(GUIDS[4],
                                                  REPOSITORY_DEFS.get(0),
                                                  REPOSITORY_DEFS.get(1)));

        NODE_DEFS.add(new NodeDef("alpha",
                                  GUIDS[0],
                                  singletonList(URI.create("http://192.168.0.1:8080"))));

        NODE_DEFS.add(new NodeDef("beta",
                                  GUIDS[1],
                                  asList(URI.create("http://192.168.0.2:8080"),
                                         URI.create("http://31.34.134.14:8080"))));

        NODE_DEFS.add(new NodeDef("gamma",
                                  GUIDS[2],
                                  emptyList()));

        NODE_INFOS.add(new NodeInfo(NODE_DEFS.get(0), emptyList()));
        NODE_INFOS.add(new NodeInfo(NODE_DEFS.get(1), REPOSITORY_INFOS));

        REMOTE_INFOS.add(new RemoteInfo(NODE_INFOS.get(0),
                                        Instant.EPOCH));

        REMOTE_INFOS.add(new RemoteInfo(NODE_INFOS.get(1),
                                        NODE_DEFS.get(1).getPublishUris().get(1),
                                        Instant.ofEpochMilli(123000)));

        NODE_EXCEPTIONS.add(new UnknownRepositoryException());
        NODE_EXCEPTIONS.add(new UnexpectedFailureException(new NullPointerException()));
        NODE_EXCEPTIONS.add(new IOFailureException(new FileNotFoundException("/tmp/test")));
    }

    private TestData() {
    }
}
