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
package org.elasticlib.node.components;

import com.google.common.base.Splitter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.exception.RepositoryClosedException;
import org.elasticlib.common.exception.UnknownRepositoryException;
import org.elasticlib.common.exception.UnreachableNodeException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RemoteInfo;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.node.dao.RemotesDao;
import org.elasticlib.node.manager.client.ClientsManager;
import org.elasticlib.node.manager.message.MessageManager;
import org.elasticlib.node.manager.message.RepositoryRemoved;
import org.elasticlib.node.manager.message.RepositoryUnavailable;
import org.elasticlib.node.repository.RemoteRepository;
import org.elasticlib.node.repository.Repository;

/**
 * Manages remote repositories.
 */
public class RemoteRepositoriesPool {

    private static final String SEPARATOR = ".";

    private final ClientsManager clientsManager;
    private final MessageManager messageManager;
    private final RemotesDao remotesDao;
    private final Map<Guid, Repository> repositories = new HashMap<>();

    /**
     * Constructor.
     *
     * @param clientsManager Node clients manager.
     * @param messageManager Messaging infrastructure manager.
     * @param remotesDao Remotes nodes DAO.
     */
    public RemoteRepositoriesPool(ClientsManager clientsManager, MessageManager messageManager, RemotesDao remotesDao) {
        this.clientsManager = clientsManager;
        this.messageManager = messageManager;
        this.remotesDao = remotesDao;
    }

    /**
     * Starts this component.
     */
    public void start() {
        messageManager.register(RepositoryUnavailable.class, "Closing remote repository", this::tryCloseRepository);
        messageManager.register(RepositoryRemoved.class, "Closing remote repository", this::tryCloseRepository);
    }

    /**
     * Closes all managed repositories, releasing underlying resources.
     */
    public synchronized void stop() {
        repositories.values().forEach(Repository::close);
        repositories.clear();
    }

    /**
     * Resolves GUID of a remote repository. Fails it if it is unknown.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding Repository GUID.
     */
    public Guid getRepositoryGuid(String key) {
        Optional<RemoteInfo> remoteInfo = remotesDao.tryGetRemoteInfo(x -> repositoryGuid(x, key).isPresent());
        if (!remoteInfo.isPresent()) {
            throw new UnknownRepositoryException();
        }
        return repositoryGuid(remoteInfo.get(), key).get();
    }

    private static Optional<Guid> repositoryGuid(RemoteInfo remoteInfo, String key) {
        if (!key.contains(SEPARATOR)) {
            return repositoryDefs(remoteInfo)
                    .filter(x -> matches(x.getGuid(), x.getName(), key))
                    .map(RepositoryDef::getGuid)
                    .findFirst();
        }
        List<String> parts = Splitter.on(SEPARATOR).splitToList(key);
        if (parts.size() != 2 || !matches(remoteInfo.getGuid(), remoteInfo.getName(), parts.get(0))) {
            return Optional.empty();
        }
        return repositoryGuid(remoteInfo, parts.get(1));
    }

    private static boolean matches(Guid guid, String name, String key) {
        return key.equals(name) || (Guid.isValid(key) && new Guid(key).equals(guid));
    }

    /**
     * Provides definition of a remote repository, if it is known.
     *
     * @param guid Repository GUID.
     * @return Corresponding RepositoryDef, if any.
     */
    public Optional<RepositoryDef> tryGetRepositoryDef(Guid guid) {
        Optional<RemoteInfo> remoteInfo = remotesDao.tryGetRemoteInfo(x -> hasRepository(x, guid));
        if (!remoteInfo.isPresent()) {
            return Optional.empty();
        }
        return repositoryDefs(remoteInfo.get())
                .filter(x -> x.getGuid().equals(guid))
                .findFirst();
    }

    private static boolean hasRepository(RemoteInfo remoteInfo, Guid guid) {
        return repositoryDefs(remoteInfo).anyMatch(x -> x.getGuid().equals(guid));
    }

    private static Stream<RepositoryDef> repositoryDefs(RemoteInfo remoteInfo) {
        return remoteInfo.listRepositoryInfos()
                .stream()
                .map(RepositoryInfo::getDef);
    }

    /**
     * Provides a remote repository. Fails if it unknown or unreachable.
     *
     * @param guid Repository GUID.
     * @return Corresponding repository.
     */
    public Repository getRepository(Guid guid) {
        Optional<RemoteInfo> remoteInfo = remotesDao.tryGetRemoteInfo(x -> hasRepository(x, guid));
        if (!remoteInfo.isPresent()) {
            throw new UnknownRepositoryException();
        }
        if (!remoteInfo.get().isReachable()) {
            throw new UnreachableNodeException();
        }
        if (!isRepositoryOpen(remoteInfo.get(), guid)) {
            throw new RepositoryClosedException();
        }
        return repository(remoteInfo.get(), guid);
    }

    /**
     * Provides a remote repository if it is known and currently reachable.
     *
     * @param guid Repository GUID.
     * @return Corresponding repository, if any.
     */
    public Optional<Repository> tryGetRepository(Guid guid) {
        Optional<RemoteInfo> remoteInfo = remotesDao.tryGetRemoteInfo(x -> hasRepository(x, guid));
        if (!remoteInfo.isPresent()) {
            return Optional.empty();
        }
        if (!remoteInfo.get().isReachable()) {
            return Optional.empty();
        }
        if (!isRepositoryOpen(remoteInfo.get(), guid)) {
            return Optional.empty();
        }
        return Optional.of(repository(remoteInfo.get(), guid));
    }

    private static boolean isRepositoryOpen(RemoteInfo remoteInfo, Guid guid) {
        return remoteInfo.listRepositoryInfos()
                .stream()
                .filter(x -> x.getDef().getGuid().equals(guid))
                .findFirst()
                .get()
                .isOpen();
    }

    private synchronized Repository repository(RemoteInfo remoteInfo, Guid guid) {
        if (!repositories.containsKey(guid)) {
            Client client = clientsManager.getClient(remoteInfo.getTransportUri());
            repositories.put(guid, new RemoteRepository(client, repositoryName(remoteInfo, guid), guid));
        }
        return repositories.get(guid);
    }

    private static String repositoryName(RemoteInfo remoteInfo, Guid guid) {
        String repositoryName = repositoryDefs(remoteInfo)
                .filter(x -> x.getGuid().equals(guid))
                .map(RepositoryDef::getName)
                .findFirst()
                .get();

        return String.join(SEPARATOR, remoteInfo.getName(), repositoryName);
    }

    /**
     * Closes repository matching supplied GUID, if any.
     *
     * @param guid Repository GUID.
     */
    public synchronized void tryCloseRepository(Guid guid) {
        if (!repositories.containsKey(guid)) {
            return;
        }
        repositories.remove(guid).close();
    }
}
