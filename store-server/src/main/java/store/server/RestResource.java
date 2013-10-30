package store.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import javax.inject.Singleton;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.glassfish.jersey.media.multipart.FormDataParam;
import store.common.ContentInfo;
import store.common.Hash;
import static store.common.JsonUtil.readContentInfo;
import static store.common.JsonUtil.writeConfig;
import static store.common.JsonUtil.writeContentInfo;
import static store.common.JsonUtil.writeContentInfos;
import static store.common.JsonUtil.writeEvents;
import store.common.Uid;

@Path("/")
@Singleton
public class RestResource {

    private final Repository repository;

    public RestResource(Repository repository) {
        this.repository = repository;
    }

    @POST
    @Path("createVolume/{path}")
    public Response createVolume(@PathParam("path") String path) {
        repository.createVolume(Paths.get(path));
        return Response.ok().build();
    }

    @POST
    @Path("dropVolume/{uid}")
    public Response dropVolume(@PathParam("uid") Uid uid) {
        repository.dropVolume(uid);
        return Response.ok().build();
    }

    @POST
    @Path("createIndex/{path}/{uid}")
    public Response createIndex(@PathParam("path") String path, @PathParam("uid") Uid volumeId) {
        repository.createIndex(Paths.get(path), volumeId);
        return Response.ok().build();
    }

    @POST
    @Path("dropIndex/{uid}")
    public Response dropIndex(@PathParam("uid") Uid uid) {
        repository.dropIndex(uid);
        return Response.ok().build();
    }

    @POST
    @Path("setWrite/{uid}")
    public Response setWrite(@PathParam("uid") Uid uid) {
        repository.setWrite(uid);
        return Response.ok().build();
    }

    @POST
    @Path("unsetWrite")
    public Response unsetWrite() {
        repository.unsetWrite();
        return Response.ok().build();
    }

    @POST
    @Path("setRead/{uid}")
    public Response setRead(@PathParam("uid") Uid uid) {
        repository.setRead(uid);
        return Response.ok().build();
    }

    @POST
    @Path("unsetRead")
    public Response unsetRead() {
        repository.unsetRead();
        return Response.ok().build();
    }

    @POST
    @Path("setSearch/{uid}")
    public Response setSearch(@PathParam("uid") Uid uid) {
        repository.setSearch(uid);
        return Response.ok().build();
    }

    @POST
    @Path("unsetSearch")
    public Response unsetSearch() {
        repository.unsetSearch();
        return Response.ok().build();
    }

    @POST
    @Path("sync/{source}/{destination}")
    public Response sync(@PathParam("source") Uid source, @PathParam("destination") Uid destination) {
        repository.sync(source, destination);
        return Response.ok().build();
    }

    @POST
    @Path("unsync/{source}/{destination}")
    public Response unsync(@PathParam("source") Uid source, @PathParam("destination") Uid destination) {
        repository.unsync(source, destination);
        return Response.ok().build();
    }

    @POST
    @Path("start/{uid}")
    public Response start(@PathParam("uid") Uid uid) {
        repository.start(uid);
        return Response.ok().build();
    }

    @POST
    @Path("stop/{uid}")
    public Response stop(@PathParam("uid") Uid uid) {
        repository.stop(uid);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    public JsonObject config() {
        return writeConfig(repository.config());
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("put")
    public Response put(@FormDataParam("info") JsonObject json, @FormDataParam("source") InputStream inputStream) {
        repository.put(readContentInfo(json), inputStream);
        return Response.ok().build();
    }

    @POST
    @Path("delete/{hash}")
    public Response delete(@PathParam("hash") Hash hash) {
        repository.delete(hash);
        return Response.ok().build();
    }

    @GET
    @Path("contains/{hash}")
    public Response contains(@PathParam("hash") Hash hash) {
        return Response.ok()
                .entity(repository.contains(hash))
                .build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("info/{hash}")
    public JsonObject info(@PathParam("hash") Hash hash) {
        ContentInfo info = repository.info(hash);
        return writeContentInfo(info);
    }

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("get/{hash}")
    public Response get(@PathParam("hash") final Hash hash) {
        return Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException {
                repository.get(hash, outputStream);
            }
        }).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("find/{query}")
    public JsonArray find(@PathParam("query") String query) {
        return writeContentInfos(repository.find(query));
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("history/{chronological}/{first}/{number}")
    public JsonArray history(@PathParam("chronological") boolean chronological,
                             @PathParam("first") long first,
                             @PathParam("number") int number) {
        return writeEvents(repository.history(chronological, first, number));
    }
}
