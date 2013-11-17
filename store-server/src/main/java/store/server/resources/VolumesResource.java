package store.server.resources;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.NOT_IMPLEMENTED;
import javax.ws.rs.core.StreamingOutput;
import org.glassfish.jersey.media.multipart.FormDataParam;
import store.common.Hash;
import static store.common.JsonUtil.readContentInfo;
import static store.common.JsonUtil.writeContentInfo;
import static store.common.JsonUtil.writeEvents;
import store.server.Repository;
import store.server.volume.Status;

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

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createVolume(JsonObject json) {
        repository.createVolume(Paths.get(json.getString("path")));
        return Response.status(CREATED).build();
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
    public Response dropVolume(@PathParam("name") String name) {
        repository.dropVolume(name);
        return Response.ok().build();
    }

    @POST
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updateVolume(@PathParam("name") String name, JsonObject json) {
        if (!hasBooleanValue(json, "started")) {
            return Response.status(BAD_REQUEST).build();
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
    public Response postContent(@PathParam("name") String name,
                                @FormDataParam("info") JsonObject json,
                                @FormDataParam("content") InputStream inputStream) {

        //
        // FIXME : Soucis avec cette méthode
        // - flexibilité : on est obligé de calculer le hash/length côté client - a priori KO pour un upload HTML/JS.
        // - UX : parce lecture et digest une fois avant l'upload.
        // - UX/perfs : parce Jersey-multipart impose la mise en tampon du body sur disque alors que c'est inutile ici.
        //
        // SOLUTION :
        // - Rendre la part "info" facultative. Si elle n'est pas fournie, faire le digest et l'extraction côté serveur.
        // - Avoir un multipartReader ad-hoc qui fournisse le contentInfo systématiquement et qui streame l'inputstream.
        //      * Si info fourni dans le multipart. Ne cache rien sur disque, suppose que l'info est la 1re part, et
        //        lit l'info à la volée. Filtre le boundary de fin de l'entité. Lance un IOException si pas de boundary
        //        de fin.
        //
        //      * Si pas d'info, cache le content sur disque en calculant au passage le digest et les métadonnées.
        //
        //      * Quoi qu'il arrive, console les métadonnées avec le filename/content-type lu dans les headers de la
        //        part du content.
        //
        // Soucis résiduels :
        // - Si on cache sur disque, on devra ensuite faire une autre recopie vers le volume. On aimerait pouvoir cacher
        //   directement sur la partition du volume et faire un simple move dans un second temps.
        //
        // - Mine de rien, l'implé Jersey gère un paquet de chose. Prévoir pas mal de soucis liés à la réinvention
        //   de la roue. Envisager de réutiliser tout ou partie de le leur implé !?
        //

        repository.put(name, readContentInfo(json), inputStream);
        return Response.status(CREATED).build();
    }

    @PUT
    @Path("{name}/content/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response putContent(@PathParam("name") String name,
                               @PathParam("hash") Hash hash,
                               @FormDataParam("info") JsonObject json,
                               @FormDataParam("content") InputStream inputStream) {

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
            throw new IllegalArgumentException(); // FIXME should response a 400
        }
        if (from == null) {
            from = sort.equals("asc") ? 0 : Long.MAX_VALUE;
        }
        return writeEvents(repository.history(name, sort.equals("asc"), from, size));
    }
}
