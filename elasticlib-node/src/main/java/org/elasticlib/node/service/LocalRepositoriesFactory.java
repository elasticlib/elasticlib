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
package org.elasticlib.node.service;

import java.nio.file.Path;
import org.elasticlib.common.config.Config;
import org.elasticlib.node.manager.message.MessageManager;
import org.elasticlib.node.manager.task.TaskManager;
import org.elasticlib.node.repository.LocalRepository;
import org.elasticlib.node.repository.Repository;

/**
 * Creates and open local repositories.
 */
public class LocalRepositoriesFactory {

    private final Config config;
    private final TaskManager taskManager;
    private final MessageManager messageManager;

    /**
     * Constructor.
     *
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @param messageManager Messaging infrastructure manager.
     */
    public LocalRepositoriesFactory(Config config, TaskManager taskManager, MessageManager messageManager) {
        this.config = config;
        this.taskManager = taskManager;
        this.messageManager = messageManager;
    }

    /**
     * Creates a new repository.
     *
     * @param path The repository path.
     * @return Created repository.
     */
    public Repository create(Path path) {
        return LocalRepository.create(path, config, taskManager, messageManager);
    }

    /**
     * Open an existing repository.
     *
     * @param path The repository path.
     * @return Opened repository.
     */
    public Repository open(Path path) {
        return LocalRepository.open(path, config, taskManager, messageManager);
    }
}
