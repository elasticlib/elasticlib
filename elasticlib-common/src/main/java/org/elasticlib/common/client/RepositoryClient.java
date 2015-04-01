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
package org.elasticlib.common.client;

import com.google.common.base.Joiner;
import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Splitter;
import com.google.common.net.HttpHeaders;
import java.io.InputStream;
import java.util.Collection;
import static java.util.Collections.emptyList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.client.Entity.json;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import static org.elasticlib.common.client.ClientUtil.checkStatus;
import static org.elasticlib.common.client.ClientUtil.ensureSuccess;
import static org.elasticlib.common.client.ClientUtil.read;
import static org.elasticlib.common.client.ClientUtil.readAll;
import static org.elasticlib.common.client.ClientUtil.result;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import static org.elasticlib.common.json.JsonWriting.write;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.Digest;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.IndexEntry;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;
import static org.glassfish.jersey.media.multipart.Boundary.addBoundary;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;

/**
 * Client to a remote repository.
 */
public class RepositoryClient {

    private static final String CONTENT = "content";
    private static final String REVISIONS = "revisions";
    private static final String INDEX = "index";
    private static final String HISTORY = "history";
    private static final String INFO_TEMPLATE = "info/{hash}";
    private static final String STAGE_TEMPLATE = "stage/{hash}";
    private static final String WRITE_TEMPLATE = "stage/{hash}/{sessionId}";
    private static final String CONTENTS_TEMPLATE = "contents/{hash}";
    private static final String DIGEST_TEMPLATE = "digests/{hash}";
    private static final String REVISIONS_TEMPLATE = "revisions/{hash}";
    private static final String SESSION_ID = "sessionId";
    private static final String POSITION = "position";
    private static final String HASH = "hash";
    private static final String REV = "rev";
    private static final String HEAD = "head";
    private static final String CONTENT_DISPOSITION = "Content-Disposition";
    private static final String FILENAME = "filename";
    private static final String OFFSET = "offset";
    private static final String LENGTH = "length";
    private static final String QUERY = "query";
    private static final String FROM = "from";
    private static final String SIZE = "size";
    private static final String SORT = "sort";
    private static final String ASC = "asc";
    private static final String DESC = "desc";
    private final WebTarget resource;

    /**
     * Constructor.
     *
     * @param resource Base web-resource.
     */
    RepositoryClient(WebTarget resource) {
        this.resource = resource;
    }

    /**
     * Provides info about this repository.
     *
     * @return A RepositoryInfo instance.
     */
    public RepositoryInfo getInfo() {
        Response response = resource
                .request()
                .get();

        return read(response, RepositoryInfo.class);
    }

    /**
     * Add revision about a (possibly new) content.
     *
     * @param revision New head revision.
     * @return Actual command result.
     */
    public CommandResult addRevision(Revision revision) {
        return result(resource
                .path(REVISIONS)
                .request()
                .post(json(write(revision))));
    }

    /**
     * Merges supplied revision tree with existing one,
     *
     * @param tree Revision tree to merge
     * @return Actual command result.
     */
    public CommandResult mergeTree(RevisionTree tree) {
        return result(resource
                .path(REVISIONS)
                .request()
                .post(json(write(tree))));
    }

    /**
     * Prepares to add a new content.
     *
     * @param hash Hash of the content to be added latter.
     * @return Info about the staging session created.
     */
    public StagingInfo stageContent(Hash hash) {
        Response response = resource.path(STAGE_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .request()
                .post(null);

        return read(response, StagingInfo.class);
    }

    /**
     * Writes bytes to a staged content.
     *
     * @param hash Hash of the staged content (when staging is completed).
     * @param sessionId Staging session identifier.
     * @param inputStream Bytes to write.
     * @param position Position in staged content at which write should begin.
     * @return Updated info of the staging session.
     */
    public StagingInfo writeContent(Hash hash, Guid sessionId, InputStream inputStream, long position) {
        MultiPart multipart = new FormDataMultiPart()
                .bodyPart(new StreamDataBodyPart(CONTENT,
                                                 inputStream,
                                                 CONTENT,
                                                 MediaType.APPLICATION_OCTET_STREAM_TYPE));

        Response response = resource.path(WRITE_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .resolveTemplate(SESSION_ID, sessionId)
                .queryParam(POSITION, position)
                .request()
                .post(entity(multipart, addBoundary(multipart.getMediaType())));

        return read(response, StagingInfo.class);
    }

    /**
     * Terminates a content staging session. Actually, this only releases the session, but leaves staged content as it.
     * Does nothing if such a session does not exist or has expired.
     *
     * @param hash Hash of the content to be added latter.
     * @param sessionId Staging session identifier.
     */
    public void unstageContent(Hash hash, Guid sessionId) {
        ensureSuccess(resource.path(WRITE_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .resolveTemplate(SESSION_ID, sessionId)
                .request()
                .delete());
    }

    /**
     * Delete an exising content.
     *
     * @param hash Content hash.
     * @param head Expected content head revisions hashes.
     * @return Actual command result.
     */
    public CommandResult deleteContent(Hash hash, Set<Hash> head) {
        return result(resource
                .path(CONTENTS_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .queryParam(REV, Joiner.on('-').join(head))
                .request()
                .delete());
    }

    /**
     * Provides info about a given content.
     *
     * @param hash Content hash.
     * @return Corresponding content info.
     */
    public ContentInfo getContentInfo(Hash hash) {
        Response response = resource.path(INFO_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .request()
                .get();

        return read(response, ContentInfo.class);
    }

    /**
     * Provides revision tree of a given content.
     *
     * @param hash Content hash.
     * @return Corresponding revision tree.
     */
    public RevisionTree getTree(Hash hash) {
        Response response = resource.path(REVISIONS_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .request()
                .get();

        return read(response, RevisionTree.class);
    }

    /**
     * Provides head revisions of a given content.
     *
     * @param hash Content hash.
     * @return Corresponding head revisions.
     */
    public List<Revision> getHead(Hash hash) {
        return getRevisions(hash, HEAD);
    }

    /**
     * Provides some revisions of a given content.
     *
     * @param hash Content hash.
     * @param revs Requested revision hashes.
     * @return Corresponding revisions.
     */
    public List<Revision> getRevisions(Hash hash, Collection<Hash> revs) {
        if (revs.isEmpty()) {
            return emptyList();
        }
        return getRevisions(hash, join(revs));
    }

    private List<Revision> getRevisions(Hash hash, String rev) {
        Response response = resource.path(REVISIONS_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .queryParam(REV, rev)
                .request()
                .get();

        return readAll(response, Revision.class);
    }

    private static String join(Collection<Hash> revs) {
        return revs.stream()
                .map(r -> r.asHexadecimalString())
                .reduce((x, y) -> x + "-" + y)
                .get();
    }

    /**
     * Downloads a content from this repository.
     *
     * @param hash Content hash.
     * @return Corresponding content.
     */
    public Content getContent(Hash hash) {
        return getContent(resource.path(CONTENTS_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .request());
    }

    /**
     * Partially downloads a content from this repository. Supplied range [offset, offset + length[ is expected to be a
     * included in [0, totalContentLength].
     *
     * @param hash Content hash.
     * @param offset The position of first byte to return, inclusive. Expected to be positive or zero.
     * @param length The amount of bytes to returns. Expected to be positive or zero.
     * @return Corresponding content.
     */
    public Content getContent(Hash hash, long offset, long length) {
        checkArgument(offset >= 0, "Offset is negative");
        checkArgument(length >= 0, "Length is negative");

        return getContent(resource.path(CONTENTS_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .request()
                .header(HttpHeaders.RANGE, range(offset, length)));
    }

    private static String range(long offset, long length) {
        return String.format("bytes=%d-%d", offset, offset + length - 1);
    }

    private static Content getContent(Invocation.Builder invocation) {
        Response response = invocation.get();

        checkStatus(response);
        return new Content(fileName(response),
                           response.getMediaType(),
                           response.getLength(),
                           response.readEntity(InputStream.class));
    }

    private static Optional<String> fileName(Response response) {
        String header = response.getHeaders().getFirst(CONTENT_DISPOSITION).toString();
        if (header.isEmpty()) {
            return Optional.empty();
        }
        for (String param : split(header, ';')) {
            List<String> parts = split(param, '=');
            if (parts.size() == 2 && parts.get(0).equalsIgnoreCase(FILENAME)) {
                return Optional.of(parts.get(1));
            }
        }
        return Optional.empty();
    }

    private static List<String> split(String text, char character) {
        return Splitter.on(character)
                .trimResults()
                .omitEmptyStrings()
                .splitToList(text);
    }

    /**
     * Provides the digest of a content of this repository.
     *
     * @param hash Hash of the content.
     * @return Actually computed digest of this content.
     */
    public Digest getDigest(Hash hash) {
        Response response = resource.path(DIGEST_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .request()
                .get();

        return read(response, Digest.class);
    }

    /**
     * Provides a partial digest of a content of this repository.
     *
     * @param hash Hash of the content.
     * @param offset The position of the first byte to digest, inclusive. Expected to be positive or zero.
     * @param length The amount of bytes to digest. Expected to be positive or zero.
     * @return Actually computed digest.
     */
    public Digest getDigest(Hash hash, long offset, long length) {
        checkArgument(offset >= 0, "Offset is negative");
        checkArgument(length >= 0, "Length is negative");

        Response response = resource.path(DIGEST_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .queryParam(OFFSET, offset)
                .queryParam(LENGTH, length)
                .request()
                .get();

        return read(response, Digest.class);
    }

    /**
     * Find index entries matching a given query in a paginated way.
     *
     * @param query Query.
     * @param from First item to return.
     * @param size Number of items to return.
     * @return A list of index entries.
     */
    public List<IndexEntry> find(String query, int from, int size) {
        Response response = resource.path(INDEX)
                .queryParam(QUERY, query)
                .queryParam(FROM, from)
                .queryParam(SIZE, size)
                .request()
                .get();

        return readAll(response, IndexEntry.class);
    }

    /**
     * Find revisions matching a given query in a paginated way.
     *
     * @param query Query.
     * @param from First item to return.
     * @param size Number of items to return.
     * @return A list of content infos.
     */
    public List<Revision> findRevisions(String query, int from, int size) {
        Response response = resource.path(REVISIONS)
                .queryParam(QUERY, query)
                .queryParam(FROM, from)
                .queryParam(SIZE, size)
                .request()
                .get();

        return readAll(response, Revision.class);
    }

    /**
     * Provides history in a paginated way.
     *
     * @param asc If true, returned list is sorted chronologically.
     * @param from First item to return.
     * @param size Number of items to return.
     * @return A list of history events.
     */
    public List<Event> history(boolean asc, long from, int size) {
        Response response = resource.path(HISTORY)
                .queryParam(SORT, asc ? ASC : DESC)
                .queryParam(FROM, from)
                .queryParam(SIZE, size)
                .request()
                .get();

        return readAll(response, Event.class);
    }
}
