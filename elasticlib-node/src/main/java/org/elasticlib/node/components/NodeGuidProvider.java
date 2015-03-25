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

import org.elasticlib.common.hash.Guid;
import org.elasticlib.node.dao.AttributesDao;

/**
 * Provides local node GUID.
 */
public class NodeGuidProvider {

    private final AttributesDao attributesDao;
    private Guid guid;

    /**
     * Constructor.
     *
     * @param attributesDao Attributes DAO.
     */
    public NodeGuidProvider(AttributesDao attributesDao) {
        this.attributesDao = attributesDao;
    }

    /**
     * Loads the local node GUID.
     */
    public void start() {
        guid = attributesDao.guid();
    }

    /**
     * Actually does nothing.
     */
    public void stop() {
        // Nothing to do.
    }

    /**
     * @return The GUID of the local node.
     */
    public Guid guid() {
        return guid;
    }
}
