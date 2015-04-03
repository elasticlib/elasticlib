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

import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.DigestOutputStream;

/**
 * Represents a content staging session.
 */
class StagingSession {

    private final Guid sessionId;
    private final DigestOutputStream digest;

    /**
     * Constructor.
     *
     * @param sessionId Staging session identifier.
     * @param digest Current digest.
     */
    public StagingSession(Guid sessionId, DigestOutputStream digest) {
        this.sessionId = sessionId;
        this.digest = digest;
    }

    /**
     * @return The identifier of this session.
     */
    public Guid getSessionId() {
        return sessionId;
    }

    /**
     * @return Current digest of this session.
     */
    public DigestOutputStream getDigest() {
        return digest;
    }
}
