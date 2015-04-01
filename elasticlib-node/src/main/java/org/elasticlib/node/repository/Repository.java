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
package org.elasticlib.node.repository;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.Digest;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.IndexEntry;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;

/**
 * Represents a store of contents with their metadata.
 */
public interface Repository {

    /**
     * Provides the definition of this repository.
     *
     * @return A RepositoryDef instance.
     */
    public RepositoryDef getDef();

    /**
     * Provides various info about this repository.
     *
     * @return A RepositoryInfo instance.
     */
    public RepositoryInfo getInfo();

    /**
     * Prepares to add a new content in this repository.
     *
     * @param hash Hash of the content to be added latter.
     * @return Info about the staging session created.
     */
    public StagingInfo stageContent(Hash hash);

    /**
     * Writes bytes to a staged content.
     *
     * @param hash Hash of the staged content (when staging is completed).
     * @param sessionId Staging session identifier.
     * @param source Bytes to write.
     * @param position Position in staged content at which write should begin.
     * @return Updated info of the staging session.
     */
    public StagingInfo writeContent(Hash hash, Guid sessionId, InputStream source, long position);

    /**
     * Terminates a content staging session. Actually, this only releases the session, but leaves staged content as it.
     * Does nothing if such a session does not exist or has expired.
     *
     * @param hash Hash of the staged content (when staging is completed).
     * @param sessionId Staging session identifier.
     */
    public void unstageContent(Hash hash, Guid sessionId);

    /**
     * Adds a revision. If associated content is not present, started transaction is suspended so that caller may latter
     * complete this operation by adding this content.
     *
     * @param revision revision.
     * @return Actual operation result.
     */
    public CommandResult addRevision(Revision revision);

    /**
     * Merges supplied revision tree with existing one, if any. If associated content is not present, started
     * transaction is suspended so that caller may latter complete this operation by creating this content.
     *
     * @param tree Revision tree.
     * @return Actual operation result.
     */
    public CommandResult mergeTree(RevisionTree tree);

    /**
     * Deletes a content.
     *
     * @param hash Hash of the content to delete.
     * @param head Hashes of expected head revisions of the info associated with the content.
     * @return Actual operation result.
     */
    public CommandResult deleteContent(Hash hash, SortedSet<Hash> head);

    /**
     * Provides info about a given content.
     *
     * @param hash Hash of the content.
     * @return Corresponding content info.
     */
    public ContentInfo getContentInfo(Hash hash);

    /**
     * Provides revision tree associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding revision tree.
     */
    public RevisionTree getTree(Hash hash);

    /**
     * Provides head revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding head revisions.
     */
    public List<Revision> getHead(Hash hash);

    /**
     * Provides requested revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @param revs Hash of the revisions to return.
     * @return Corresponding revisions.
     */
    public List<Revision> getRevisions(Hash hash, Collection<Hash> revs);

    /**
     * Provides an input stream on a content in this repository.
     *
     * @param hash Hash of the content.
     * @param offset The position of first byte to return, inclusive. Expected to be positive or zero.
     * @param length The maximum amount of bytes to returns. Expected to be positive or zero.
     * @return An input stream on this content.
     */
    public InputStream getContent(Hash hash, long offset, long length);

    /**
     * Provides the digest of a content in this repository.
     *
     * @param hash Hash of the content.
     * @return Actually computed digest of this content.
     */
    public Digest getDigest(Hash hash);

    /**
     * Provides a partial digest of a content in this repository.
     *
     * @param hash Hash of the content.
     * @param offset The position of the first byte to digest, inclusive. Expected to be positive or zero.
     * @param length The amount of bytes to digest. Expected to be positive or zero.
     * @return Actually computed digest.
     */
    public Digest getDigest(Hash hash, long offset, long length);

    /**
     * Provides a paginated view of the history of this repository.
     *
     * @param chronological If true, returned list of events will sorted chronologically.
     * @param first Event sequence identifier to start with.
     * @param number Number of events to return.
     * @return A list of events.
     */
    public List<Event> history(boolean chronological, long first, int number);

    /**
     * Finds index entries matching supplied query.
     *
     * @param query Search query.
     * @param first First result to return.
     * @param number Number of results to return.
     * @return A list of content hashes.
     */
    public List<IndexEntry> find(String query, int first, int number);

    /**
     * Close this repository, releasing underlying resources. Does nothing if it already closed. Any latter operation
     * will fail.
     */
    public void close();
}
