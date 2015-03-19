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
import java.util.function.Predicate;
import org.elasticlib.common.exception.NodeAlreadyTrackedException;
import org.elasticlib.common.exception.UnknownNodeException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RemoteInfo;
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
     * Creates a new RemoteInfo if it does not exist, does nothing otherwise.
     *
     * @param info RemoteInfo to save.
     */
    public void saveRemoteInfo(RemoteInfo info) {
        remoteInfos.put(storageManager.currentTransaction(), entry(info.getGuid()), entry(info));
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
     */
    public void deleteRemoteInfo(String key) {
        if (Guid.isValid(key)) {
            OperationStatus status = remoteInfos.delete(storageManager.currentTransaction(), entry(new Guid(key)));
            if (status == OperationStatus.SUCCESS) {
                return;
            }
        }
        boolean found = stream().any((cursor, info) -> {
            if (info.getName().equals(key)) {
                cursor.delete();
                return true;
            }
            return false;
        });
        if (!found) {
            throw new UnknownNodeException();
        }
    }

    /**
     * Deletes info of all unreachable nodes.
     */
    public void deleteUnreachableRemoteInfos() {
        stream().each((cursor, info) -> {
            if (!info.isReachable()) {
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

    private DatabaseStream<RemoteInfo> stream() {
        return storageManager.stream(remoteInfos, RemoteInfo.class);
    }
}
