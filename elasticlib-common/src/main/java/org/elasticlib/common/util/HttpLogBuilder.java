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
package org.elasticlib.common.util;

import static java.lang.String.join;
import static java.lang.System.lineSeparator;
import java.util.List;
import java.util.Map;

/**
 * Builds log messages for HTTP requests and responses.
 */
public class HttpLogBuilder {

    private static final String SPACE = " ";
    private static final String DASH = " - ";
    private static final String COLON = ": ";
    private static final String COMMA = ",";
    private static final String REQUEST_PREFIX = "> ";
    private static final String RESPONSE_PREFIX = "< ";
    private static final String NOTIFICATION_PREFIX = "* ";
    private static final String MORE = "...more...";
    private static final String REQUEST_ENTITY_NOTE = "With request entity";
    private static final String RESPONSE_ENTITY_NOTE = "With response entity";

    private final long id;

    /**
     * Constructor.
     *
     * @param id Request or response identifier.
     */
    public HttpLogBuilder(long id) {
        this.id = id;
    }

    /**
     * Builds log message for a HTTP request.
     *
     * @param method HTTP method.
     * @param uri Requested URI.
     * @param headers Request headers.
     * @return A formatted log message.
     */
    public String request(String method, String uri, Map<String, List<String>> headers) {
        StringBuilder builder = prefix(new StringBuilder(), id, REQUEST_PREFIX)
                .append(method)
                .append(SPACE)
                .append(uri)
                .append(lineSeparator());

        printPrefixedHeaders(builder, id, REQUEST_PREFIX, headers);

        return builder.toString();
    }

    /**
     * Builds log message for a HTTP response.
     *
     * @param status HTTP status code.
     * @param message HTTP status message.
     * @param headers Response headers.
     * @return A formatted log message.
     */
    public String response(int status, String message, Map<String, List<String>> headers) {
        StringBuilder builder = prefix(new StringBuilder(), id, RESPONSE_PREFIX)
                .append(status)
                .append(DASH)
                .append(message)
                .append(lineSeparator());

        printPrefixedHeaders(builder, id, RESPONSE_PREFIX, headers);

        return builder.toString();
    }

    private static StringBuilder prefix(StringBuilder builder, long id, String prefix) {
        return builder.append(Long.toString(id))
                .append(SPACE)
                .append(prefix);
    }

    private static void printPrefixedHeaders(StringBuilder builder,
                                             long id,
                                             String prefix,
                                             Map<String, List<String>> headers) {
        headers.entrySet()
                .stream()
                .sorted((x, y) -> x.getKey().compareToIgnoreCase(y.getKey()))
                .forEach(header -> {
                    prefix(builder, id, prefix)
                    .append(header.getKey())
                    .append(COLON)
                    .append(join(COMMA, header.getValue()))
                    .append(lineSeparator());
                });
    }

    /**
     * Builds log message for a HTTP resquest body.
     *
     * @param entity Body data.
     * @param entitySize Data length to display.
     * @param hasMore If supplied data is truncated.
     * @return A formatted log message.
     */
    public String requestEntity(byte[] entity, int entitySize, boolean hasMore) {
        return entity(entity, entitySize, hasMore, REQUEST_ENTITY_NOTE);
    }

    /**
     * Builds log message for a HTTP response body.
     *
     * @param entity Body data.
     * @param entitySize Data length to display.
     * @param hasMore If supplied data is truncated.
     * @return A formatted log message.
     */
    public String responseEntity(byte[] entity, int entitySize, boolean hasMore) {
        return entity(entity, entitySize, hasMore, RESPONSE_ENTITY_NOTE);
    }

    private String entity(byte[] entity, int entitySize, boolean hasMore, String note) {
        return prefix(new StringBuilder(), id, NOTIFICATION_PREFIX)
                .append(note)
                .append(lineSeparator())
                .append(new String(entity, 0, entitySize))
                .append(hasMore ? MORE : "")
                .append(lineSeparator())
                .toString();
    }
}
