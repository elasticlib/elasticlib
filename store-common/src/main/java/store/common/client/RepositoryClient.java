package store.common.client;

import com.google.common.base.Joiner;
import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Splitter;
import com.google.common.net.HttpHeaders;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.client.Entity.json;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import static org.glassfish.jersey.media.multipart.Boundary.addBoundary;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import static store.common.client.ClientUtil.checkStatus;
import static store.common.client.ClientUtil.ensureSuccess;
import static store.common.client.ClientUtil.read;
import static store.common.client.ClientUtil.readAll;
import static store.common.client.ClientUtil.result;
import store.common.hash.Guid;
import store.common.hash.Hash;
import static store.common.json.JsonWriting.write;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.Event;
import store.common.model.IndexEntry;
import store.common.model.Revision;
import store.common.model.RevisionTree;
import store.common.model.StagingInfo;

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
    private static final String REVISIONS_TEMPLATE = "revisions/{hash}";
    private static final String SESSION_ID = "sessionId";
    private static final String POSITION = "position";
    private static final String HASH = "hash";
    private static final String REV = "rev";
    private static final String HEAD = "head";
    private static final String CONTENT_DISPOSITION = "Content-Disposition";
    private static final String FILENAME = "filename";
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
        Response response = resource.path(REVISIONS_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .queryParam(REV, HEAD)
                .request()
                .get();

        return readAll(response, Revision.class);
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

    private static Content getContent(Invocation.Builder invocation) {
        Response response = invocation.get();

        checkStatus(response);
        return new Content(fileName(response),
                           response.getMediaType(),
                           response.getLength(),
                           response.readEntity(InputStream.class));
    }

    private static String range(long offset, long length) {
        return String.format("bytes=%d-%d", offset, offset + length - 1);
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
