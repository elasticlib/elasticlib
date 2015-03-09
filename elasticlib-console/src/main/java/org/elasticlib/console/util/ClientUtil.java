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
package org.elasticlib.console.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import static java.util.stream.Collectors.toSet;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.Revision;
import org.elasticlib.console.exception.RequestFailedException;

/**
 * Client utilities.
 */
public final class ClientUtil {

    private static final String HTTP_SCHEME = "http://";

    private ClientUtil() {
    }

    /**
     * Safely parses supplied argument as a Hash.
     *
     * @param arg A command line argument.
     * @return Parsed hash.
     */
    public static Hash parseHash(String arg) {
        if (!Hash.isValid(arg)) {
            throw new RequestFailedException("Invalid hash");
        }
        return new Hash(arg);
    }

    /**
     * Safely parses supplied argument as an URI.
     *
     * @param arg A command line argument.
     * @return Parsed URI.
     */
    public static URI parseUri(String arg) {
        if (!arg.startsWith(HTTP_SCHEME)) {
            arg = HTTP_SCHEME + arg;
        }
        try {
            return new URI(arg);

        } catch (URISyntaxException e) {
            throw new RequestFailedException("Invalid URI", e);
        }
    }

    /**
     * Lists revisions hashes of supplied head.
     *
     * @param head A list of revisions.
     * @return Their revisions hashes.
     */
    public static Set<Hash> revisions(List<Revision> head) {
        return head.stream()
                .map(Revision::getRevision)
                .collect(toSet());
    }

    /**
     * Resolves the GUID of a repository, using supplied client.
     *
     * @param client Node client.
     * @param repository Repository name or encoded GUID.
     * @return GUID of this repository.
     */
    public static Guid resolveRepositoryGuid(Client client, String repository) {
        return client.repositories()
                .get(repository)
                .getInfo()
                .getDef()
                .getGuid();
    }
}
