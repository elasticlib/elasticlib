package store.common.client;

import java.nio.file.Path;
import java.util.List;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonObject;
import static javax.ws.rs.client.Entity.json;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import static store.common.client.ClientUtil.ensureSuccess;
import static store.common.client.ClientUtil.read;
import static store.common.client.ClientUtil.readAll;
import store.common.hash.Guid;
import store.common.model.RepositoryInfo;

/**
 * Repositories API client.
 */
public class RepositoriesClient {

    private static final String REPOSITORIES = "repositories";
    private static final String REPOSITORY = "repository";
    private static final String REPOSITORY_TEMPLATE = "{repository}";
    private static final String PATH = "path";
    private static final String ACTION = "action";
    private static final String CREATE = "create";
    private static final String ADD = "add";
    private static final String OPEN = "open";
    private static final String CLOSE = "close";
    private static final String REMOVE = "remove";
    private final WebTarget resource;

    /**
     * Constructor.
     *
     * @param resource Base web-resource.
     */
    RepositoriesClient(WebTarget resource) {
        this.resource = resource.path(REPOSITORIES);
    }

    /**
     * Creates a new repository at supplied path.
     *
     * @param path Repository path (from node perspective).
     */
    public void create(Path path) {
        post(createObjectBuilder()
                .add(ACTION, CREATE)
                .add(PATH, path.toAbsolutePath().toString())
                .build());
    }

    /**
     * Adds repository located at supplied path.
     *
     * @param path Repository path (from node perspective).
     */
    public void add(Path path) {
        post(createObjectBuilder()
                .add(ACTION, ADD)
                .add(PATH, path.toAbsolutePath().toString())
                .build());
    }

    /**
     * Opens an existing repository
     *
     * @param repository Repository name or encoded GUID.
     */
    public void open(String repository) {
        post(createObjectBuilder()
                .add(ACTION, OPEN)
                .add(REPOSITORY, repository)
                .build());
    }

    /**
     * Opens an existing repository
     *
     * @param guid Repository GUID.
     */
    public void open(Guid guid) {
        open(guid.asHexadecimalString());
    }

    /**
     * Closes an existing repository
     *
     * @param repository Repository name or encoded GUID.
     */
    public void close(String repository) {
        post(createObjectBuilder()
                .add(ACTION, CLOSE)
                .add(REPOSITORY, repository)
                .build());
    }

    /**
     * Closes an existing repository
     *
     * @param guid Repository GUID.
     */
    public void close(Guid guid) {
        close(guid.asHexadecimalString());
    }

    /**
     * Removes an existing repository
     *
     * @param repository Repository name or encoded GUID.
     */
    public void remove(String repository) {
        post(createObjectBuilder()
                .add(ACTION, REMOVE)
                .add(REPOSITORY, repository)
                .build());
    }

    /**
     * Removes an existing repository
     *
     * @param guid Repository GUID.
     */
    public void remove(Guid guid) {
        remove(guid.asHexadecimalString());
    }

    /**
     * Deletes an existing repository.
     *
     * @param repository Repository name or encoded GUID.
     */
    public void delete(String repository) {
        ensureSuccess(resource
                .path(REPOSITORY_TEMPLATE)
                .resolveTemplate(REPOSITORY, repository)
                .request()
                .delete());
    }

    /**
     * Deletes an existing repository
     *
     * @param guid Repository GUID.
     */
    public void delete(Guid guid) {
        delete(guid.asHexadecimalString());
    }

    /**
     * Lists infos of existing repositories.
     *
     * @return A list of repository infos.
     */
    public List<RepositoryInfo> listInfos() {
        Response response = resource
                .request()
                .get();

        return readAll(response, RepositoryInfo.class);
    }

    /**
     * Provides info about a repository.
     *
     * @param repository Repository name or encoded GUID.
     * @return Corresponding info.
     */
    public RepositoryInfo getInfo(String repository) {
        Response response = resource.path(REPOSITORY_TEMPLATE)
                .resolveTemplate(REPOSITORY, repository)
                .request()
                .get();

        return read(response, RepositoryInfo.class);
    }

    /**
     * Provides info about a repository.
     *
     * @param guid Repository GUID.
     * @return Corresponding info.
     */
    public RepositoryInfo getInfo(Guid guid) {
        return getInfo(guid.asHexadecimalString());
    }

    /**
     * Provides a repository client.
     *
     * @param repository Repository name or encoded GUID.
     * @return A client for this repository.
     */
    public RepositoryClient get(String repository) {
        return new RepositoryClient(resource
                .path(REPOSITORY_TEMPLATE)
                .resolveTemplate(REPOSITORY, repository));
    }

    /**
     * Provides a repository client.
     *
     * @param guid Repository GUID.
     * @return A client for this repository.
     */
    public RepositoryClient get(Guid guid) {
        return get(guid.asHexadecimalString());
    }

    private void post(JsonObject json) {
        ensureSuccess(resource
                .request()
                .post(json(json)));
    }
}
