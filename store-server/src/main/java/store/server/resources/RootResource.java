package store.server.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

/**
 * Root REST resource.
 */
@Path("/")
public class RootResource {

    /**
     * Intended to test connection with this server.
     *
     * @return HTTP 200 OK.
     */
    @GET
    public Response getRoot() {
        return Response.ok().build();
    }
}
