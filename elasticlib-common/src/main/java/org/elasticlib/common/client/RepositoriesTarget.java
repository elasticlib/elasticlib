/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.common.client;

import static com.google.common.collect.ImmutableMap.of;
import java.util.List;
import java.util.Map;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonObjectBuilder;
import static javax.ws.rs.client.Entity.json;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import static org.elasticlib.common.client.ClientUtil.ensureSuccess;
import static org.elasticlib.common.client.ClientUtil.readAll;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RepositoryInfo;

/**
 * Repositories API.
 */
public class RepositoriesTarget {

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

    private final WebTarget target;

    /**
     * Constructor.
     *
     * @param target Underlying resource target.
     */
    RepositoriesTarget(WebTarget target) {
        this.target = target.path(REPOSITORIES);
    }

    /**
     * Creates a new repository at supplied path.
     *
     * @param path Repository path (from node perspective).
     */
    public void create(String path) {
        post(of(ACTION, CREATE,
                PATH, path));
    }

    /**
     * Adds repository located at supplied path.
     *
     * @param path Repository path (from node perspective).
     */
    public void add(String path) {
        post(of(ACTION, ADD,
                PATH, path));
    }

    /**
     * Opens an existing repository
     *
     * @param repository Repository name or encoded GUID.
     */
    public void open(String repository) {
        post(of(ACTION, OPEN,
                REPOSITORY, repository));
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
        post(of(ACTION, CLOSE,
                REPOSITORY, repository));
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
        post(of(ACTION, REMOVE,
                REPOSITORY, repository));
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
        ensureSuccess(target
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
        Response response = target
                .request()
                .get();

        return readAll(response, RepositoryInfo.class);
    }

    /**
     * Provides an API on a given repository.
     *
     * @param repository Repository name or encoded GUID.
     * @return A new API on this repository.
     */
    public RepositoryTarget get(String repository) {
        return new RepositoryTarget(target
                .path(REPOSITORY_TEMPLATE)
                .resolveTemplate(REPOSITORY, repository));
    }

    /**
     * Provides an API on a given repository.
     *
     * @param guid Repository GUID.
     * @return A new API on this repository.
     */
    public RepositoryTarget get(Guid guid) {
        return get(guid.asHexadecimalString());
    }

    private void post(Map<String, String> values) {
        JsonObjectBuilder builder = createObjectBuilder();
        values.forEach(builder::add);

        ensureSuccess(target
                .request()
                .post(json(builder.build())));
    }
}
