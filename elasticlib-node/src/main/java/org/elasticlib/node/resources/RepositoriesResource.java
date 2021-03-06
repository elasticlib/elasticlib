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
package org.elasticlib.node.resources;

import com.google.common.base.Splitter;
import com.google.common.net.HttpHeaders;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Collections.emptyMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import static java.util.stream.Collectors.toList;
import javax.inject.Inject;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import org.elasticlib.common.exception.BadRequestException;
import org.elasticlib.common.exception.IOFailureException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import static org.elasticlib.common.json.JsonReading.read;
import static org.elasticlib.common.json.JsonValidation.hasStringValue;
import static org.elasticlib.common.json.JsonValidation.isValid;
import org.elasticlib.common.metadata.Properties.Common;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.Digest;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.IndexEntry;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;
import static org.elasticlib.common.util.IoUtil.copy;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;
import org.elasticlib.node.multipart.FormDataMultipart;
import org.elasticlib.node.repository.Repository;
import org.elasticlib.node.service.RepositoriesService;

/**
 * Repositories REST resource.
 */
@Path("repositories")
public class RepositoriesResource {

    private static final String PATH = "path";
    private static final String ACTION = "action";
    private static final String REPOSITORY = "repository";
    private static final String HASH = "hash";
    private static final String REV = "rev";
    private static final String SESSION_ID = "sessionId";
    private static final String POSITION = "position";
    private static final String HEAD = "head";
    private static final String CONTENT = "content";
    private static final String OFFSET = "offset";
    private static final String LENGTH = "length";
    private static final String QUERY = "query";
    private static final String SORT = "sort";
    private static final String FROM = "from";
    private static final String SIZE = "size";
    private static final String ASC = "asc";
    private static final String DESC = "desc";
    private static final String DEFAULT_FROM = "0";
    private static final String DEFAULT_SIZE = "20";

    @Inject
    private RepositoriesService repositoriesService;
    @Context
    private UriInfo uriInfo;

    /**
     * Alters state of a repository.
     * <p>
     * Response:<br>
     * - 201 CREATED: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 412 PRECONDITION FAILED: Repository could not be created at supplied path.
     *
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postRepository(JsonObject json) {
        Action action = Action.of(json);
        switch (action) {
            case CREATE:
                return createRepository(json);
            case ADD:
                return addRepository(json);
            case OPEN:
                return openRepository(json);
            case CLOSE:
                return closeRepository(json);
            case REMOVE:
                return removeRepository(json);
            case DELETE:
                return deleteRepository(json);
            default:
                throw new AssertionError();
        }
    }

    private static enum Action {

        CREATE, ADD, OPEN, CLOSE, REMOVE, DELETE;

        public static Action of(JsonObject json) {
            if (!hasStringValue(json, ACTION)) {
                return CREATE;
            }
            String raw = json.getString(ACTION).toUpperCase();
            for (Action action : values()) {
                if (action.name().equals(raw)) {
                    return action;
                }
            }
            throw newInvalidJsonException();
        }
    }

    private Response createRepository(JsonObject json) {
        if (!hasStringValue(json, PATH)) {
            throw newInvalidJsonException();
        }
        java.nio.file.Path path = Paths.get(json.getString(PATH))
                .toAbsolutePath()
                .normalize();

        repositoriesService.createRepository(path);
        return Response
                .created(uriInfo.getAbsolutePathBuilder().path(path.getFileName().toString()).build())
                .build();
    }

    private Response addRepository(JsonObject json) {
        if (!hasStringValue(json, PATH)) {
            throw newInvalidJsonException();
        }
        java.nio.file.Path path = Paths.get(json.getString(PATH))
                .toAbsolutePath()
                .normalize();

        repositoriesService.addRepository(path);
        return Response
                .created(uriInfo.getAbsolutePathBuilder().path(path.getFileName().toString()).build())
                .build();
    }

    private Response openRepository(JsonObject json) {
        if (!hasStringValue(json, REPOSITORY)) {
            throw newInvalidJsonException();
        }
        repositoriesService.openRepository(json.getString(REPOSITORY));
        return Response.ok().build();
    }

    private Response closeRepository(JsonObject json) {
        if (!hasStringValue(json, REPOSITORY)) {
            throw newInvalidJsonException();
        }
        repositoriesService.closeRepository(json.getString(REPOSITORY));
        return Response.ok().build();
    }

    private Response removeRepository(JsonObject json) {
        if (!hasStringValue(json, REPOSITORY)) {
            throw newInvalidJsonException();
        }
        repositoriesService.removeRepository(json.getString(REPOSITORY));
        return Response.ok().build();
    }

    private Response deleteRepository(JsonObject json) {
        if (!hasStringValue(json, REPOSITORY)) {
            throw newInvalidJsonException();
        }
        return deleteRepository(json.getString(REPOSITORY));
    }

    /**
     * Deletes a repository.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param repositoryKey repository name or encoded GUID
     * @return HTTP response
     */
    @DELETE
    @Path("{repository}")
    public Response deleteRepository(@PathParam(REPOSITORY) String repositoryKey) {
        repositoriesService.deleteRepository(repositoryKey);
        return Response.ok().build();
    }

    /**
     * Lists info about existing repositoryies.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     *
     * @return output data
     */
    @GET
    public GenericEntity<List<RepositoryInfo>> listRepositories() {
        return new GenericEntity<List<RepositoryInfo>>(repositoriesService.listRepositoryInfos()) {
        };
    }

    /**
     * Provides info about a repository.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param repositoryKey repository name or encoded GUID
     * @return output data
     */
    @GET
    @Path("{repository}")
    public RepositoryInfo getRepository(@PathParam(REPOSITORY) String repositoryKey) {
        return repositoriesService.getRepositoryInfo(repositoryKey);
    }

    /**
     * Prepares to add a new content in a repository.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.<br>
     * - 409 CONFLICT: Another staging session is already in progress.<br>
     * - 412 PRECONDITION FAILED: Content is already completely staged or present<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @return Info about the staging session created
     */
    @POST
    @Path("{repository}/stage/{hash}")
    public StagingInfo stageContent(@PathParam(REPOSITORY) String repositoryKey, @PathParam(HASH) Hash hash) {
        return repository(repositoryKey).stageContent(hash);
    }

    /**
     * Writes bytes to a staged content.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started or staging session has expired.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @param sessionId Staging session identifier
     * @param position Position in staged content at which write should begin
     * @param formData entity form data
     * @return Updated info of the staging session
     */
    @POST
    @Path("{repository}/stage/{hash}/{sessionId}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public StagingInfo writeContent(@PathParam(REPOSITORY) String repositoryKey,
                                    @PathParam(HASH) Hash hash,
                                    @PathParam(SESSION_ID) Guid sessionId,
                                    @QueryParam(POSITION) long position,
                                    FormDataMultipart formData) {

        try (InputStream inputStream = formData.next(CONTENT).getAsInputStream()) {
            return repository(repositoryKey).writeContent(hash, sessionId, inputStream, position);

        } catch (IOException e) {
            throw new IOFailureException(e);
        }
    }

    /**
     * Terminates a content staging session. Actually, this only releases the session, but leaves staged content as it.
     * Does nothing if such a session does not exist or has expired.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started or staging session has expired.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash Hash of the staged content (when staging is completed).
     * @param sessionId Staging session identifier.
     */
    @DELETE
    @Path("{repository}/stage/{hash}/{sessionId}")
    public void unstageContent(@PathParam(REPOSITORY) String repositoryKey,
                               @PathParam(HASH) Hash hash,
                               @PathParam(SESSION_ID) Guid sessionId) {

        repository(repositoryKey).unstageContent(hash, sessionId);
    }

    /**
     * Adds a revision or a revision tree. If associated content is not present, started transaction is suspended so
     * that client may create this content in a latter request.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 404 NOT FOUND: Repository or content was not found.<br>
     * - 409 CONFLICT: Supplied rev spec did not match existing one.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Path("{repository}/revisions")
    @Consumes(MediaType.APPLICATION_JSON)
    public CommandResult addRevision(@PathParam(REPOSITORY) String repositoryKey, JsonObject json) {
        if (isValid(json, Revision.class)) {
            return repository(repositoryKey).addRevision(read(json, Revision.class));
        }
        if (isValid(json, RevisionTree.class)) {
            return repository(repositoryKey).mergeTree(read(json, RevisionTree.class));
        }
        throw newInvalidJsonException();
    }

    /**
     * Deletes a content.
     * <p>
     * Query param:<br>
     * - rev: specify expected head to apply request on. May be set to "any" if requester makes to expectation about
     * existing head or to a dash-separated sequence of revision hashes of expected existing head. Default to "any".
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository or content was not found.<br>
     * - 409 CONFLICT: Supplied rev spec did not match existing one.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param rev expected head to apply request on
     * @param hash content hash
     * @return HTTP response
     */
    @DELETE
    @Path("{repository}/contents/{hash}")
    public CommandResult deleteContent(@PathParam(REPOSITORY) String repositoryKey,
                                       @QueryParam(REV) @DefaultValue("") String rev,
                                       @PathParam(HASH) Hash hash) {

        return repository(repositoryKey).deleteContent(hash, new TreeSet<>(parseRevisions(rev)));
    }

    /**
     * Provides info about a content.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @return Corresponding content info.
     */
    @GET
    @Path("{repository}/info/{hash}")
    public ContentInfo getContentInfo(@PathParam(REPOSITORY) String repositoryKey, @PathParam(HASH) Hash hash) {
        return repository(repositoryKey).getContentInfo(hash);
    }

    /**
     * Provides digest of a content.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository or content was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @param offset The position of the first byte to digest, inclusive. Optional. Expected to be positive or zero.
     * @param length The amount of bytes to digest. Optional. Expected to be positive or zero.
     * @return Corresponding digest
     */
    @GET
    @Path("{repository}/digests/{hash}")
    public Digest getDigest(@PathParam(REPOSITORY) String repositoryKey,
                            @PathParam(HASH) Hash hash,
                            @QueryParam(OFFSET) Long offset,
                            @QueryParam(LENGTH) Long length) {

        if (offset == null && length == null) {
            return repository(repositoryKey).getDigest(hash);
        }
        long off = unbox(OFFSET, offset, 0);
        long len = unbox(LENGTH, length, Long.MAX_VALUE);
        return repository(repositoryKey).getDigest(hash, off, len);
    }

    private static long unbox(String name, Long value, long defaultValue) {
        long unboxed = value != null ? value : defaultValue;
        check(unboxed >= 0, name + " has to be a positive value");
        return unboxed;
    }

    /**
     * Provides a given content.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository or content was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @param rangeParam Requested range, if any.
     * @return HTTP response
     */
    @GET
    @Path("{repository}/contents/{hash}")
    public Response getContent(@PathParam(REPOSITORY) String repositoryKey,
                               @PathParam(HASH) Hash hash,
                               @HeaderParam(HttpHeaders.RANGE) String rangeParam) {

        Repository repository = repository(repositoryKey);
        RevisionTree tree = repository.getTree(hash);
        Range range = new Range(rangeParam, tree.getLength());

        ResponseBuilder response = Response
                .status(range.getStatus())
                .entity((StreamingOutput) outputStream -> {
                    try (InputStream inputStream = repository.getContent(tree.getContent(),
                                                                         range.getOffset(),
                                                                         range.getLength())) {
                        copy(inputStream, outputStream);
                    }
                });

        range.getHttpResponseHeaders()
                .entrySet()
                .stream()
                .forEach(param -> response.header(param.getKey(), param.getValue()));

        Map<String, Value> metadata = metadata(tree);
        String contentType = value(metadata, Common.CONTENT_TYPE.key());
        if (!contentType.isEmpty()) {
            response.type(contentType);
        }
        String fileName = value(metadata, Common.FILE_NAME.key());
        if (!fileName.isEmpty()) {
            response.header("Content-Disposition", "attachment; filename=" + fileName);
        }
        return response.build();
    }

    private static Map<String, Value> metadata(RevisionTree tree) {
        for (Revision rev : tree.get(tree.getHead())) {
            if (!rev.isDeleted()) {
                return rev.getMetadata();
            }
        }
        return emptyMap();
    }

    private static String value(Map<String, Value> metadata, String key) {
        Value value = metadata.get(key);
        if (value == null || value.type() != ValueType.STRING) {
            return "";
        }
        return value.asString();
    }

    /**
     * Provides revisions about a content.
     * <p>
     * Query param:<br>
     * - rev: specify revisions to returns. May be set to "head" to return current head revisions or to a dash-separated
     * sequence of wanted revision hashes. If unspecified, the whole revision tree is returned.
     *
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository or content was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @param rev requested revisions
     * @return HTTP response
     */
    @GET
    @Path("{repository}/revisions/{hash}")
    public Response getRevisions(@PathParam(REPOSITORY) String repositoryKey,
                                 @PathParam(HASH) Hash hash,
                                 @QueryParam(REV) @DefaultValue("") String rev) {

        if (rev.isEmpty()) {
            return Response.ok()
                    .entity(repository(repositoryKey).getTree(hash))
                    .build();
        }
        if (rev.equals(HEAD)) {
            return response(repository(repositoryKey).getHead(hash));
        }
        return response(repository(repositoryKey).getRevisions(hash, parseRevisions(rev)));
    }

    private static List<Hash> parseRevisions(String arg) {
        List<Hash> revisions = new ArrayList<>();
        for (String part : Splitter.on('-').split(arg)) {
            revisions.add(new Hash(part));
        }
        return revisions;
    }

    private static Response response(List<Revision> contentInfos) {
        GenericEntity<List<Revision>> entity = new GenericEntity<List<Revision>>(contentInfos) {
        };
        return Response.ok()
                .entity(entity)
                .build();
    }

    /**
     * Provides repository history.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid query parameters.<br>
     * - 404 NOT FOUND: Repository was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param sort chronological sorting. Allowed values are "asc" and "desc".
     * @param from sequence value to start with.
     * @param size number of results to return.
     * @return output data
     */
    @GET
    @Path("{repository}/history")
    public GenericEntity<List<Event>> history(@PathParam(REPOSITORY) String repositoryKey,
                                              @QueryParam(SORT) @DefaultValue(DESC) String sort,
                                              @QueryParam(FROM) Long from,
                                              @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {
        if (!sort.equals(ASC) && !sort.equals(DESC)) {
            throw newInvalidJsonException();
        }
        if (from == null) {
            from = sort.equals(ASC) ? 0 : Long.MAX_VALUE;
        }
        List<Event> events = repository(repositoryKey).history(sort.equals(ASC), from, size);
        return new GenericEntity<List<Event>>(events) {
        };
    }

    /**
     * Finds index entries matching supplied query.
     * <p>
     * Output:<br>
     * - Array of content hashes.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param query Query
     * @param from sequence value to start with.
     * @param size number of results to return.
     * @return output data
     */
    @GET
    @Path("{repository}/index")
    public GenericEntity<List<IndexEntry>> find(@PathParam(REPOSITORY) String repositoryKey,
                                                @QueryParam(QUERY) String query,
                                                @QueryParam(FROM) @DefaultValue(DEFAULT_FROM) int from,
                                                @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {

        List<IndexEntry> entries = repository(repositoryKey).find(query, from, size);
        return new GenericEntity<List<IndexEntry>>(entries) {
        };
    }

    /**
     * Finds indexed revisions matching supplied query.
     * <p>
     * Output:<br>
     * - Array of revisions.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param query Query
     * @param from sequence value to start with.
     * @param size number of results to return.
     * @return output data
     */
    @GET
    @Path("{repository}/revisions")
    public GenericEntity<List<Revision>> findRevisions(@PathParam(REPOSITORY) String repositoryKey,
                                                       @QueryParam(QUERY) String query,
                                                       @QueryParam(FROM) @DefaultValue(DEFAULT_FROM) int from,
                                                       @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {
        Repository repository = repository(repositoryKey);
        List<Revision> infos = repository.find(query, from, size)
                .stream()
                .flatMap(entry -> repository.getRevisions(entry.getHash(), entry.getRevisions()).stream())
                .collect(toList());

        return new GenericEntity<List<Revision>>(infos) {
        };
    }

    private Repository repository(String name) {
        return repositoriesService.getRepository(name);
    }

    private static void check(boolean expression, String message) {
        if (!expression) {
            throw new BadRequestException(message);
        }
    }

    private static BadRequestException newInvalidJsonException() {
        return new BadRequestException("Invalid json data");
    }
}
