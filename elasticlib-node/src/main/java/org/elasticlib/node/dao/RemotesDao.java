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
package org.elasticlib.node.dao;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.elasticlib.common.exception.NodeAlreadyTrackedException;
import org.elasticlib.common.exception.UnknownNodeException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RemoteInfo;
import static org.elasticlib.node.manager.storage.DatabaseEntries.asMappable;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.DatabaseStream;
import org.elasticlib.node.manager.storage.StorageManager;

/**
 * Provides a persistent storage for remote node infos.
 */
public class RemotesDao {

    private static final String REMOTES = "remotes";

    private final StorageManager storageManager;
    private final Database remoteInfos;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     */
    public RemotesDao(StorageManager storageManager) {
        this.storageManager = storageManager;
        remoteInfos = storageManager.openDatabase(REMOTES);
    }

    /**
     * Checks whether a RemoteInfo with supplied GUID is already stored.
     *
     * @param guid A node GUID.
     * @return true if associated node definition is already stored.
     */
    public boolean containsRemoteInfo(Guid guid) {
        OperationStatus status = remoteInfos.get(storageManager.currentTransaction(),
                                                 entry(guid),
                                                 new DatabaseEntry(),
                                                 LockMode.DEFAULT);

        return status == OperationStatus.SUCCESS;
    }

    /**
     * Creates a new RemoteInfo if it does not exist, updates exiting one otherwise.
     *
     * @param info RemoteInfo to save.
     * @return Previous remote info, if any.
     */
    public Optional<RemoteInfo> saveRemoteInfo(RemoteInfo info) {
        DatabaseEntry key = entry(info.getGuid());
        DatabaseEntry value = new DatabaseEntry();
        OperationStatus retrieval = remoteInfos.get(storageManager.currentTransaction(), key, value, LockMode.RMW);
        remoteInfos.put(storageManager.currentTransaction(), key, entry(info));
        if (retrieval == OperationStatus.SUCCESS) {
            return Optional.of(asMappable(value, RemoteInfo.class));
        }
        return Optional.empty();
    }

    /**
     * Creates a new RemoteInfo. Fails if it already exist.
     *
     * @param info RemoteInfo to save.
     */
    public void createRemoteInfo(RemoteInfo info) {
        OperationStatus status = remoteInfos.putNoOverwrite(storageManager.currentTransaction(),
                                                            entry(info.getGuid()),
                                                            entry(info));

        if (status == OperationStatus.KEYEXIST) {
            throw new NodeAlreadyTrackedException();
        }
    }

    /**
     * Deletes a RemoteInfo. Fails if it does not exist.
     *
     * @param key Node name or encoded GUID.
     * @return Deleted remote info.
     */
    public RemoteInfo deleteRemoteInfo(String key) {
        if (Guid.isValid(key)) {
            DatabaseEntry k = entry(new Guid(key));
            DatabaseEntry v = new DatabaseEntry();
            OperationStatus retrieval = remoteInfos.get(storageManager.currentTransaction(), k, v, LockMode.RMW);
            if (retrieval == OperationStatus.SUCCESS) {
                remoteInfos.delete(storageManager.currentTransaction(), k);
                return asMappable(v, RemoteInfo.class);
            }
        }
        Optional<RemoteInfo> deleted = stream().first((cursor, info) -> {
            if (info.getName().equals(key)) {
                cursor.delete();
                return true;
            }
            return false;
        });
        if (!deleted.isPresent()) {
            throw new UnknownNodeException();
        }
        return deleted.get();
    }

    /**
     * Deletes all RemoteInfo matching supplied predicate.
     *
     * @param predicate Filtering predicate.
     */
    public void deleteRemoteInfos(Predicate<RemoteInfo> predicate) {
        stream().each((cursor, info) -> {
            if (predicate.test(info)) {
                cursor.delete();
            }
        });
    }

    /**
     * Loads all RemoteInfo matching supplied predicate.
     *
     * @param predicate Filtering predicate.
     * @return Matching stored node infos.
     */
    public List<RemoteInfo> listRemoteInfos(Predicate<RemoteInfo> predicate) {
        return stream().orderBy(RemoteInfo::getName).list(predicate);
    }

    /**
     * Loads a RemoteInfo that matches supplied predicate, if any.
     *
     * @param predicate Filtering predicate.
     * @return Matching remote node info, if any.
     */
    public Optional<RemoteInfo> tryGetRemoteInfo(Predicate<RemoteInfo> predicate) {
        return stream().first(predicate);
    }

    private DatabaseStream<RemoteInfo> stream() {
        return storageManager.stream(remoteInfos, RemoteInfo.class);
    }
}
