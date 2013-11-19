package store.server.resources;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import static javax.ws.rs.core.Response.Status.NOT_IMPLEMENTED;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import store.common.Hash;
import static store.common.JsonUtil.hasBooleanValue;
import static store.common.JsonUtil.hasStringValue;
import static store.common.JsonUtil.readContentInfo;
import static store.common.JsonUtil.writeContentInfo;
import static store.common.JsonUtil.writeEvents;
import store.server.Repository;
import store.server.exception.BadRequestException;
import store.server.exception.WriteException;
import store.server.multipart.BodyPart;
import store.server.multipart.FormDataMultipart;
import store.server.volume.Status;

/**
 * Volumes REST resource.
 */
@Path("volumes")
public class VolumesResource {

    @Inject
    private Repository repository;
    @Context
    private UriInfo uriInfo;

    /**
     * Create a new volume.
     * <p>
     * Input:<br>
     * - path (String): Volume path on file system. Volume name is the last part of this path.
     * <p>
     * Response:<br>
     * - 201 CREATED: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 412 PRECONDITION FAILED: Volume could not be created at supplied path.
     *
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createVolume(JsonObject json) {
        if (!hasStringValue(json, "path")) {
            throw new BadRequestException();
        }
        java.nio.file.Path path = Paths.get(json.getString("path"));
        repository.createVolume(path);
        return Response
                .created(UriBuilder.fromUri(uriInfo.getRequestUri()).fragment(path.getFileName().toString()).build())
                .build();
    }

    /**
     * Create a new volume.
     *
     * @see createVolume(JsonObject)
     * @param name volume name
     * @param json input JSON data
     * @return HTTP response
     */
    @PUT
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createVolume(@PathParam("name") String name, JsonObject json) {
        if (!hasStringValue(json, "path")) {
            throw new BadRequestException();
        }
        repository.createVolume(Paths.get(json.getString("path")).resolve(name));
        return Response.created(uriInfo.getRequestUri()).build();
    }

    /**
     * Delete a volume.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Volume was not found.
     *
     * @param name volume name
     * @return HTTP response
     */
    @DELETE
    @Path("{name}")
    public Response dropVolume(@PathParam("name") String name) {
        repository.dropVolume(name);
        return Response.ok().build();
    }

    /**
     * Update a volume.
     * <p>
     * Input:<br>
     * - started (Boolean): Starts/stops the volume.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 404 NOT FOUND: Volume was not found.
     *
     * @param name volume name
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updateVolume(@PathParam("name") String name, JsonObject json) {
        if (!hasBooleanValue(json, "started")) {
            throw new BadRequestException();
        }
        if (json.getBoolean("started")) {
            repository.start(name);
        } else {
            repository.stop(name);
        }
        return Response.ok().build();
    }

    /**
     * List existing volumes.
     * <p>
     * Output:<br>
     * - Array of volume names.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     *
     * @return output JSON data
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray listVolumes() {
        JsonArrayBuilder builder = Json.createArrayBuilder();
        for (java.nio.file.Path path : repository.config().getVolumes()) {
            builder.add(path.getFileName().toString());
        }
        return builder.build();
    }

    /**
     * Get info about a volume.
     * <p>
     * Output:<br>
     * - name (String): Volume name.<br>
     * - path (String): Volume path on file system.<br>
     * - started (Boolean): If volume is started.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Volume was not found.
     *
     * @param name volume name
     * @return output JSON data
     */
    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getVolume(@PathParam("name") String name) {
        Status status = repository.volumeStatus(name);
        return Json.createObjectBuilder()
                .add("name", name)
                .add("path", status.getPath().toString())
                .add("started", status.isStarted())
                .build();
    }

    /**
     * Create a new content.
     * <p>
     * Input:<br>
     * - info (JSON): Content info JSON data.<br>
     * - content (Raw): Content data.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid form data.<br>
     * - 404 NOT FOUND: Volume was not found.<br>
     * - 412 PRECONDITION FAILED: Content already exists or integrity checking failed.<br>
     * - 503 SERVICE UNAVAILABLE: Volume is not started.
     *
     * @param name volume name
     * @param formData entity form data
     * @return HTTP response
     */
    @POST
    @Path("{name}/content")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response postContent(@PathParam("name") String name, FormDataMultipart formData) {
        JsonObject json = formData.next("info").getAsJsonObject();
        BodyPart content = formData.next("content");
        try (InputStream inputStream = content.getAsInputStream()) {
            repository.put(name, readContentInfo(json), inputStream);
            return Response
                    .created(UriBuilder.fromUri(uriInfo.getRequestUri()).fragment(json.getString("hash")).build())
                    .build();

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    /**
     * Create a new content.
     *
     * @see postContent(String, FormDataMultipart)
     * @param name volume name
     * @param hash content hash
     * @param formData entity form data
     * @return HTTP response
     */
    @PUT
    @Path("{name}/content/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response putContent(@PathParam("name") String name, @PathParam("hash") Hash hash, FormDataMultipart formData) {
        // TODO à implémenter
        return Response.status(NOT_IMPLEMENTED).build();
    }

    /**
     * Delete a content.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Volume or content was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Volume is not started.
     *
     * @param name volume name
     * @param hash content hash
     * @return HTTP response
     */
    @DELETE
    @Path("{name}/content/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response deleteContent(@PathParam("name") String name, @PathParam("hash") Hash hash) {
        repository.delete(name, hash);
        return Response.ok().build();
    }

    /**
     * Get a content.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Volume or content was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Volume is not started.
     *
     * @param name volume name
     * @param hash content hash
     * @return HTTP response
     */
    @GET
    @Path("{name}/content/{hash}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getContent(@PathParam("name") final String name, @PathParam("hash") final Hash hash) {
        return Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException {
                repository.get(name, hash, outputStream);
            }
        }).build();
    }

    /**
     * Update content info.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 404 NOT FOUND: Volume or content was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Volume is not started.
     *
     * @param name volume name
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Path("{name}/info")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updateInfo(@PathParam("name") String name, JsonObject json) {
        // TODO à implémenter en vue de la mise à jour
        return Response.status(NOT_IMPLEMENTED).build();
    }

    /**
     * Get info about a content.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Volume or content was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Volume is not started.
     *
     * @param name volume name
     * @param hash content hash
     * @return output JSON data
     */
    @GET
    @Path("{name}/info/{hash}")
    public JsonObject getInfo(@PathParam("name") String name, @PathParam("hash") Hash hash) {
        return writeContentInfo(repository.info(name, hash));
    }

    /**
     * Get volume history.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid query parameters.<br>
     * - 404 NOT FOUND: Volume was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Volume is not started.
     *
     * @param name volume name
     * @param sort chronological sorting. Allowed values are "asc" and "desc".
     * @param from sequence value to start with.
     * @param size number of results to return.
     * @return output JSON data
     */
    @GET
    @Path("{name}/history")
    public JsonArray history(@PathParam("name") String name,
                             @QueryParam("sort") @DefaultValue("desc") String sort,
                             @QueryParam("from") Long from,
                             @QueryParam("size") @DefaultValue("20") int size) {

        if (!sort.equals("asc") && !sort.equals("desc")) {
            throw new BadRequestException();
        }
        if (from == null) {
            from = sort.equals("asc") ? 0 : Long.MAX_VALUE;
        }
        return writeEvents(repository.history(name, sort.equals("asc"), from, size));
    }
}
