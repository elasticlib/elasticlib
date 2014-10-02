package store.common.client;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import java.io.InputStream;
import static java.util.Collections.emptyList;
import java.util.List;
import java.util.Set;
import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.client.Entity.json;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import static org.glassfish.jersey.media.multipart.Boundary.addBoundary;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import static store.common.client.ClientUtil.checkStatus;
import static store.common.client.ClientUtil.read;
import static store.common.client.ClientUtil.readAll;
import static store.common.client.ClientUtil.result;
import store.common.hash.Hash;
import static store.common.json.JsonWriting.write;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.ContentInfoTree;
import store.common.model.Event;
import store.common.model.IndexEntry;

/**
 * Client to a remote repository.
 */
public class RepositoryClient {

    private static final String CONTENT = "content";
    private static final String INFO = "info";
    private static final String INDEX = "index";
    private static final String HISTORY = "history";
    private static final String CONTENT_TEMPLATE = "content/{hash}";
    private static final String INFO_TEMPLATE = "info/{hash}";
    private static final String TX_ID = "txId";
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
     * Add info about a (possibly new) content.
     *
     * @param info New head revision.
     * @return Actual command result.
     */
    public CommandResult addInfo(ContentInfo info) {
        return result(resource
                .path(INFO)
                .request()
                .post(json(write(info))));
    }

    /**
     * Add a new content. Caller is responsible for closing supplied input-stream.
     *
     * @param transactionId Identifier of the transaction used to previously add content associated info.
     * @param hash Content hash.
     * @param inputStream Content input-stream
     * @return Actual command result.
     */
    public CommandResult addContent(long transactionId, Hash hash, InputStream inputStream) {
        MultiPart multipart = new FormDataMultiPart()
                .bodyPart(new StreamDataBodyPart(CONTENT,
                                                 inputStream,
                                                 CONTENT,
                                                 MediaType.APPLICATION_OCTET_STREAM_TYPE));

        return result(resource.path(CONTENT_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .queryParam(TX_ID, transactionId)
                .request()
                .put(entity(multipart, addBoundary(multipart.getMediaType()))));
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
                .path(CONTENT_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .queryParam(REV, Joiner.on('-').join(head))
                .request()
                .delete());
    }

    /**
     * Provides content info tree for a given content.
     *
     * @param hash Content hash.
     * @return Corresponding info tree.
     */
    public ContentInfoTree getInfoTree(Hash hash) {
        Response response = resource.path(INFO_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .request()
                .get();

        return read(response, ContentInfoTree.class);
    }

    /**
     * Provides info head revisions for a given content.
     *
     * @param hash Content hash.
     * @return Corresponding info head.
     */
    public List<ContentInfo> getInfoHead(Hash hash) {
        return readAll(infoHead(hash), ContentInfo.class);
    }

    /**
     * Provides info head revisions for a given content, or an empty list if this content does not exist.
     *
     * @param hash Content hash.
     * @return Corresponding info head, if any.
     */
    public List<ContentInfo> getInfoHeadIfAny(Hash hash) {
        Response response = infoHead(hash);
        try {
            if (response.getStatusInfo().getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                return emptyList();
            }
            return readAll(response, ContentInfo.class);

        } finally {
            response.close();
        }
    }

    private Response infoHead(Hash hash) {
        return resource.path(INFO_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .queryParam(REV, HEAD)
                .request()
                .get();
    }

    /**
     * Download a content from this repository.
     *
     * @param hash Content hash.
     * @return Corresponding content.
     */
    public Content getContent(Hash hash) {
        Response response = resource.path(CONTENT_TEMPLATE)
                .resolveTemplate(HASH, hash)
                .request()
                .get();

        checkStatus(response);
        return new Content(fileName(response), response.getMediaType(), response.readEntity(InputStream.class));
    }

    private static Optional<String> fileName(Response response) {
        String header = response.getHeaders().getFirst(CONTENT_DISPOSITION).toString();
        if (header == null || header.isEmpty()) {
            return Optional.absent();
        }
        for (String param : split(header, ';')) {
            List<String> parts = split(param, '=');
            if (parts.size() == 2 && parts.get(0).equalsIgnoreCase(FILENAME)) {
                return Optional.of(parts.get(1));
            }
        }
        return Optional.absent();
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
     * Find content infos matching a given query in a paginated way.
     *
     * @param query Query.
     * @param from First item to return.
     * @param size Number of items to return.
     * @return A list of content infos.
     */
    public List<ContentInfo> findInfo(String query, int from, int size) {
        Response response = resource.path(INFO)
                .queryParam(QUERY, query)
                .queryParam(FROM, from)
                .queryParam(SIZE, size)
                .request()
                .get();

        return readAll(response, ContentInfo.class);
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
