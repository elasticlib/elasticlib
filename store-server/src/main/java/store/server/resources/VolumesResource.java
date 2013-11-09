package store.server.resources;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CREATED;
import javax.ws.rs.core.StreamingOutput;
import org.glassfish.jersey.media.multipart.FormDataParam;
import store.common.Hash;
import static store.common.JsonUtil.readContentInfo;
import static store.common.JsonUtil.writeContentInfo;
import static store.common.JsonUtil.writeEvents;
import store.server.Repository;

@Path("volumes")
public class VolumesResource {

    private final Repository repository;

    /**
     * Constructor.
     *
     * @param repository The repository.
     */
    public VolumesResource(Repository repository) {
        this.repository = repository;
    }

    @PUT
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createVolume(@PathParam("name") String name, JsonObject json) {
        repository.createVolume(Paths.get(json.getString("path")).resolve(name));
        return Response.status(CREATED).build();
    }

    @DELETE
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response dropVolume(@PathParam("name") String name) {
        repository.dropVolume(name);
        return Response.ok().build();
    }

    @POST
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response execute(@PathParam("name") String name, JsonObject json) {
        switch (json.getString("command")) {
            case "start":
                repository.start(name);
                return Response.ok().build();

            case "stop":
                repository.stop(name);
                return Response.ok().build();

            default:
                return Response.status(BAD_REQUEST).build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray listVolumes() {
        return Json.createArrayBuilder().build(); // TODO this is a stub
    }

    @PUT
    @Path("{name}/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response put(@PathParam("name") String name,
                        @PathParam("hash") Hash hash,
                        @FormDataParam("info") JsonObject json,
                        @FormDataParam("source") InputStream inputStream) {
        // En fait le hash ne sert à rien ici : redondant avec le contentInfo :(
        repository.put(name, readContentInfo(json), inputStream);
        return Response.status(CREATED).build();
    }

    @DELETE
    @Path("{name}/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response delete(@PathParam("name") String name, @PathParam("hash") Hash hash) {
        repository.delete(name, hash);
        return Response.ok().build();
    }

    @GET
    @Path("{name}/{hash}/info")
    public JsonObject info(@PathParam("name") final String name, @PathParam("hash") final Hash hash) {
        return writeContentInfo(repository.info(name, hash));
    }

    @GET
    @Path("{name}/{hash}/content")
    public Response get(@PathParam("name") final String name, @PathParam("hash") final Hash hash) {
        return Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException {
                repository.get(name, hash, outputStream);
            }
        }).build();
    }

    @POST
    @Path("{name}/log")
    public JsonArray log(@PathParam("name") final String name, JsonObject json) {
        // On pourrait aussi le faire en GET avec des queryParams
        return writeEvents(repository.history(name,
                                              json.getBoolean("reverse"),
                                              json.getJsonNumber("from").longValue(),
                                              json.getInt("size")));
    }
}
