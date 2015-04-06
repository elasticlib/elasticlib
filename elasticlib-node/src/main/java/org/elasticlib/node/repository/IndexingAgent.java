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
package org.elasticlib.node.repository;

import java.io.IOException;
import java.io.InputStream;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.node.dao.CurSeqsDao;

/**
 * An agent that performs indexing from a repository to its internal index.
 */
class IndexingAgent extends Agent {

    private final Repository repository;
    private final Index index;

    /**
     * Constructor.
     *
     * @param repository Tracked repository.
     * @param index Tracked repository index.
     * @param curSeqsDao Agent sequences DAO.
     */
    public IndexingAgent(Repository repository, Index index, CurSeqsDao curSeqsDao) {
        super("indexation-" + repository.getDef().getGuid(), repository, curSeqsDao, "index");

        this.repository = repository;
        this.index = index;
    }

    @Override
    protected boolean process(Event event) {
        RevisionTree tree = repository.getTree(event.getContent());
        if (tree.isDeleted()) {
            index.delete(tree.getContent());
            return true;

        } else {
            try (InputStream inputStream = repository.getContent(tree.getContent(), 0, Long.MAX_VALUE)) {
                index.index(tree, inputStream);
                return true;

            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }
}
