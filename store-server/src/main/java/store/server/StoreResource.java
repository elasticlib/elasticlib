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
public class StoreResource {

    private final StoreManager storeManager;

    public StoreResource(StoreManager storeManager) {
        this.storeManager = storeManager;
    }

    @POST
    @Path("createVolume/{path}")
    public Response createVolume(@PathParam("path") String path) {
        storeManager.createVolume(Paths.get(path));
        return Response.ok().build();
    }

    @POST
    @Path("dropVolume/{uid}")
    public Response dropVolume(@PathParam("uid") Uid uid) {
        storeManager.dropVolume(uid);
        return Response.ok().build();
    }

    @POST
    @Path("createIndex/{path}/{uid}")
    public Response createIndex(@PathParam("path") String path, @PathParam("uid") Uid volumeId) {
        storeManager.createIndex(Paths.get(path), volumeId);
        return Response.ok().build();
    }

    @POST
    @Path("dropIndex/{uid}")
    public Response dropIndex(@PathParam("uid") Uid uid) {
        storeManager.dropIndex(uid);
        return Response.ok().build();
    }

    @POST
    @Path("setWrite/{uid}")
    public Response setWrite(@PathParam("uid") Uid uid) {
        storeManager.setWrite(uid);
        return Response.ok().build();
    }

    @POST
    @Path("unsetWrite")
    public Response unsetWrite() {
        storeManager.unsetWrite();
        return Response.ok().build();
    }

    @POST
    @Path("setRead/{uid}")
    public Response setRead(@PathParam("uid") Uid uid) {
        storeManager.setRead(uid);
        return Response.ok().build();
    }

    @POST
    @Path("unsetRead")
    public Response unsetRead() {
        storeManager.unsetRead();
        return Response.ok().build();
    }

    @POST
    @Path("setSearch/{uid}")
    public Response setSearch(@PathParam("uid") Uid uid) {
        storeManager.setSearch(uid);
        return Response.ok().build();
    }

    @POST
    @Path("unsetSearch")
    public Response unsetSearch() {
        storeManager.unsetSearch();
        return Response.ok().build();
    }

    @POST
    @Path("sync/{source}/{destination}")
    public Response sync(@PathParam("source") Uid source, @PathParam("destination") Uid destination) {
        storeManager.sync(source, destination);
        return Response.ok().build();
    }

    @POST
    @Path("unsync/{source}/{destination}")
    public Response unsync(@PathParam("source") Uid source, @PathParam("destination") Uid destination) {
        storeManager.unsync(source, destination);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    public JsonObject config() {
        return writeConfig(storeManager.config());
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("put")
    public Response put(@FormDataParam("info") JsonObject json, @FormDataParam("source") InputStream inputStream) {
        storeManager.put(readContentInfo(json), inputStream);
        return Response.ok().build();
    }

    @POST
    @Path("delete/{hash}")
    public Response delete(@PathParam("hash") String encodedHash) {
        storeManager.delete(new Hash(encodedHash));
        return Response.ok().build();
    }

    @GET
    @Path("contains/{hash}")
    public Response contains(@PathParam("hash") String encodedHash) {
        return Response.ok()
                .entity(storeManager.contains(new Hash(encodedHash)))
                .build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("info/{hash}")
    public JsonObject info(@PathParam("hash") String encodedHash) {
        ContentInfo info = storeManager.info(new Hash(encodedHash));
        return writeContentInfo(info);
    }

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("get/{hash}")
    public Response get(@PathParam("hash") String encodedHash) {
        final Hash hash = new Hash(encodedHash);
        return Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException {
                storeManager.get(hash, outputStream);
            }
        }).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("find/{query}")
    public JsonArray find(@PathParam("query") String query) {
        return writeContentInfos(storeManager.find(query));
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("history/{chronological}/{first}/{number}")
    public JsonArray history(@PathParam("chronological") boolean chronological,
                             @PathParam("first") long first,
                             @PathParam("number") int number) {
        return writeEvents(storeManager.history(chronological, first, number));
    }
}
