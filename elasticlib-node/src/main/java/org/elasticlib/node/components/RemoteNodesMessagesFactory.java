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

import static java.util.Collections.emptyList;
import java.util.List;
import java.util.Optional;
import static java.util.stream.Collectors.toList;
import org.elasticlib.common.model.RemoteInfo;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.RepositoryStats;
import org.elasticlib.node.manager.message.NewRepositoryEvent;
import org.elasticlib.node.manager.message.RepositoryAvailable;
import org.elasticlib.node.manager.message.RepositoryChangeMessage;
import org.elasticlib.node.manager.message.RepositoryRemoved;
import org.elasticlib.node.manager.message.RepositoryUnavailable;

/**
 * Builds messages related to remote nodes events.
 */
public class RemoteNodesMessagesFactory {

    /**
     * Provides the list of messages to send following a remote node info creation.
     *
     * @param info Created remote node info.
     * @return Corresponding messages to send.
     */
    public List<RepositoryChangeMessage> createMessages(RemoteInfo info) {
        if (!info.isReachable()) {
            return emptyList();
        }
        return info.listRepositoryInfos()
                .stream()
                .filter(RepositoryInfo::isOpen)
                .map(x -> new RepositoryAvailable(x.getDef().getGuid()))
                .collect(toList());
    }

    /**
     * Provides the list of messages to send following a remote node info deletion.
     *
     * @param info Deleted remote node info.
     * @return Corresponding messages to send.
     */
    public List<RepositoryChangeMessage> deleteMessages(RemoteInfo info) {
        if (!info.isReachable()) {
            return emptyList();
        }
        return info.listRepositoryInfos()
                .stream()
                .filter(RepositoryInfo::isOpen)
                .map(x -> new RepositoryUnavailable(x.getDef().getGuid()))
                .collect(toList());
    }

    /**
     * Provides the list of messages to send following a remote node info update.
     *
     * @param before Info before the update.
     * @param after Info after the update.
     * @return Corresponding messages to send.
     */
    public List<RepositoryChangeMessage> updateMessages(RemoteInfo before, RemoteInfo after) {
        if (before.isReachable() && !after.isReachable()) {
            return deleteMessages(before);
        }
        if (!before.isReachable() && after.isReachable()) {
            return createMessages(after);
        }
        if (before.isReachable() && after.isReachable()) {
            return before.listRepositoryInfos()
                    .stream()
                    .map(info -> message(info, after))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toList());
        }
        return emptyList();
    }

    private static Optional<RepositoryChangeMessage> message(RepositoryInfo before, RemoteInfo updated) {
        Optional<RepositoryInfo> after = updated.listRepositoryInfos()
                .stream()
                .filter(x -> x.getDef().getGuid().equals(before.getDef().getGuid()))
                .findFirst();

        if (!after.isPresent()) {
            return Optional.of(new RepositoryRemoved(before.getDef().getGuid()));
        }
        if (hasBeenOpened(before, after.get())) {
            return Optional.of(new RepositoryAvailable(before.getDef().getGuid()));
        }
        if (hasBeenClosed(before, after.get())) {
            return Optional.of(new RepositoryUnavailable(before.getDef().getGuid()));
        }
        if (hasNewEvent(before, after.get())) {
            return Optional.of(new NewRepositoryEvent(before.getDef().getGuid()));
        }
        return Optional.empty();
    }

    private static boolean hasBeenOpened(RepositoryInfo before, RepositoryInfo after) {
        return !before.isOpen() && after.isOpen();
    }

    private static boolean hasBeenClosed(RepositoryInfo before, RepositoryInfo after) {
        return before.isOpen() && !after.isOpen();
    }

    private static boolean hasNewEvent(RepositoryInfo before, RepositoryInfo after) {
        return before.isOpen() && after.isOpen() && eventsCount(before) < eventsCount(after);
    }

    private static long eventsCount(RepositoryInfo info) {
        RepositoryStats stats = info.getStats();
        return stats.getCreations() + stats.getUpdates() + stats.getDeletions();
    }
}
