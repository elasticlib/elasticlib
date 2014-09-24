package store.common.client;

import java.util.List;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonObject;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import static store.common.client.ClientUtil.ensureSuccess;
import static store.common.client.ClientUtil.readAll;
import store.common.model.ReplicationInfo;

/**
 * Replications API client.
 */
public class ReplicationsClient {

    private static final String REPLICATIONS = "replications";
    private static final String ACTION = "action";
    private static final String CREATE = "create";
    private static final String START = "start";
    private static final String STOP = "stop";
    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private final WebTarget resource;

    /**
     * Constructor.
     *
     * @param resource Base web-resource.
     */
    ReplicationsClient(WebTarget resource) {
        this.resource = resource.path(REPLICATIONS);
    }

    /**
     * Creates a new replication.
     *
     * @param source Source repository.
     * @param target Target repository.
     */
    public void create(String source, String target) {
        postReplication(CREATE, source, target);
    }

    /**
     * Starts an existing replication.
     *
     * @param source Source repository.
     * @param target Target repository.
     */
    public void start(String source, String target) {
        postReplication(START, source, target);
    }

    /**
     * Stops an existing replication.
     *
     * @param source Source repository.
     * @param target Target repository.
     */
    public void stop(String source, String target) {
        postReplication(STOP, source, target);
    }

    private void postReplication(String action, String source, String target) {
        JsonObject json = createObjectBuilder()
                .add(ACTION, action)
                .add(SOURCE, source)
                .add(TARGET, target)
                .build();

        ensureSuccess(resource
                .request()
                .post(Entity.json(json)));
    }

    /**
     * Deletes an existing replication.
     *
     * @param source Source repository.
     * @param target Target repository.
     */
    public void delete(String source, String target) {
        ensureSuccess(resource
                .queryParam(SOURCE, source)
                .queryParam(TARGET, target)
                .request()
                .delete());
    }

    /**
     * Lists infos of existing replications.
     *
     * @return A list of replication infos.
     */
    public List<ReplicationInfo> listInfos() {
        Response response = resource
                .request()
                .get();

        return readAll(response, ReplicationInfo.class);
    }
}
