package store.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.inject.Singleton;
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
import static store.common.JsonUtil.readConfig;
import static store.common.JsonUtil.readContentInfo;
import static store.common.JsonUtil.write;

@Path("/")
@Singleton
public class StoreResource {

    private final StoreManager storeManager;

    public StoreResource(StoreManager storeManager) {
        this.storeManager = storeManager;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("create")
    public Response create(JsonObject json) {
        storeManager.create(readConfig(json));
        return Response.ok().build();
    }

    @POST
    @Path("drop")
    public Response drop() {
        storeManager.drop();
        return Response.ok().build();
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
        return write(info);
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
}
