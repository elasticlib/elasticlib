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
package org.elasticlib.common.client;

import java.util.List;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.json.JsonReading;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.model.CommandResult;

/**
 * Client internal utilities.
 */
final class ClientUtil {

    private ClientUtil() {
    }

    /**
     * Ensure response HTTP status is 2xx.
     *
     * @param response HTTP response to check.
     */
    public static void ensureSuccess(Response response) {
        try {
            checkStatus(response);

        } finally {
            response.close();
        }
    }

    /**
     * Reads a mappable from the body of the supplied response.
     *
     * @param <T> Mappable type.
     * @param response An HTTP response.
     * @param clazz Actual class of the expected mappable.
     * @return A new Mappable instance.
     */
    public static <T extends Mappable> T read(Response response, Class<T> clazz) {
        try {
            JsonObject json = checkStatus(response).readEntity(JsonObject.class);
            return JsonReading.read(json, clazz);

        } finally {
            response.close();
        }
    }

    /**
     * Reads a list of mappable from the body of the supplied response.
     *
     * @param <T> Mappable type.
     * @param response An HTTP response.
     * @param clazz Actual class of the expected mappables.
     * @return A list of mappables.
     */
    public static <T extends Mappable> List<T> readAll(Response response, Class<T> clazz) {
        try {
            JsonArray array = checkStatus(response).readEntity(JsonArray.class);
            return JsonReading.readAll(array, clazz);

        } finally {
            response.close();
        }
    }

    /**
     * Reads a command result from the body of the supplied response.
     *
     * @param response An HTTP response.
     * @return A new CommandResult instance.
     */
    public static CommandResult result(Response response) {
        return read(response, CommandResult.class);
    }

    /**
     * Checks response HTTP status is 2xx. Fails otherwise.
     *
     * @param response An HTTP response.
     * @return The response instance.
     */
    public static Response checkStatus(Response response) {
        if (response.getStatus() >= 400) {
            if (response.hasEntity() && response.getMediaType().isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
                throw JsonReading.read(response.readEntity(JsonObject.class), NodeException.class);
            }
            throw new ProcessingException(response.getStatusInfo().getReasonPhrase());
        }
        return response;
    }
}
