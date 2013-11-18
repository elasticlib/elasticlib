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
import static store.common.JsonUtil.readContentInfo;
import static store.common.JsonUtil.writeContentInfo;
import static store.common.JsonUtil.writeEvents;
import store.server.Repository;
import store.server.exception.BadRequestException;
import store.server.exception.WriteException;
import store.server.multipart.BodyPart;
import store.server.multipart.FormDataMultipart;
import store.server.volume.Status;

@Path("volumes")
public class VolumesResource {

    @Inject
    private Repository repository;
    @Context
    private UriInfo uriInfo;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createVolume(JsonObject json) {
        java.nio.file.Path path = Paths.get(json.getString("path"));
        repository.createVolume(path);
        return Response
                .created(UriBuilder.fromUri(uriInfo.getRequestUri()).fragment(path.getFileName().toString()).build())
                .build();
    }

    @PUT
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createVolume(@PathParam("name") String name, JsonObject json) {
        repository.createVolume(Paths.get(json.getString("path")).resolve(name));
        return Response.created(uriInfo.getRequestUri()).build();
    }

    @DELETE
    @Path("{name}")
    public Response dropVolume(@PathParam("name") String name) {
        repository.dropVolume(name);
        return Response.ok().build();
    }

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

    private static boolean hasBooleanValue(JsonObject json, String key) {
        if (!json.containsKey(key)) {
            return false;
        }
        switch (json.get(key).getValueType()) {
            case TRUE:
            case FALSE:
                return true;

            default:
                return false;
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray listVolumes() {
        JsonArrayBuilder builder = Json.createArrayBuilder();
        for (java.nio.file.Path path : repository.config().getVolumes()) {
            builder.add(path.getFileName().toString());
        }
        return builder.build();
    }

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

    @PUT
    @Path("{name}/content/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response putContent(@PathParam("name") String name, @PathParam("hash") Hash hash, FormDataMultipart formData) {
        // TODO à implémenter
        return Response.status(NOT_IMPLEMENTED).build();
    }

    @DELETE
    @Path("{name}/content/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response deleteContent(@PathParam("name") String name, @PathParam("hash") Hash hash) {
        repository.delete(name, hash);
        return Response.ok().build();
    }

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

    @POST
    @Path("{name}/info")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updateInfo(@PathParam("name") String name, JsonObject json) {
        // TODO à implémenter en vue de la mise à jour
        return Response.status(NOT_IMPLEMENTED).build();
    }

    @GET
    @Path("{name}/info/{hash}")
    public JsonObject getInfo(@PathParam("name") String name, @PathParam("hash") Hash hash) {
        return writeContentInfo(repository.info(name, hash));
    }

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
