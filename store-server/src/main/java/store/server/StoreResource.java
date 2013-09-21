package store.server;

import java.io.InputStream;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.media.multipart.FormDataParam;
import static store.common.json.JsonCodec.decodeConfig;
import static store.common.json.JsonCodec.decodeContentInfo;

@Path("/")
public class StoreResource {

    private StoreManager storeManager = StoreManager.get();

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("create")
    public Response create(JsonObject json) {
        storeManager.create(decodeConfig(json));
        return Response.ok()
                .build();
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("put")
    public Response put(@FormDataParam("info") JsonObject json, @FormDataParam("source") InputStream inputStream) {
        storeManager.put(decodeContentInfo(json), inputStream);
        return Response.ok()
                .build();
    }

    @POST
    @Produces(MediaType.TEXT_PLAIN)
    @Path("delete")
    public String delete() {
        return "delete"; // TODO
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("get/{hash}")
    public String get(@PathParam("hash") String hash) {
        return hash; // TODO
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("info/{hash}")
    public String info(@PathParam("hash") String hash) {
        return hash; // TODO
    }
}
