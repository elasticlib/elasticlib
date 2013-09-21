package store.server;

import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import static store.common.json.JsonCodec.decodeConfig;

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
    @Produces(MediaType.TEXT_PLAIN)
    @Path("put")
    public String put() {
        return "put"; // TODO
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
