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
package org.elasticlib.common.json;

import com.google.common.base.Charsets;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.json.schema.Schema;
import org.elasticlib.common.model.AgentInfo;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.IndexEntry;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.common.model.ReplicationDef;
import org.elasticlib.common.model.ReplicationInfo;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.RepositoryStats;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;

/**
 * Provides schemas used to write/read/validate Mappable instances as JSON.
 */
final class SchemaProvider {

    private static final Map<Class<?>, Schema> SCHEMAS = new HashMap<>();

    static {
        register(CommandResult.class,
                 StagingInfo.class,
                 ContentInfo.class,
                 Revision.class,
                 RevisionTree.class,
                 Event.class,
                 IndexEntry.class,
                 RepositoryDef.class,
                 ReplicationDef.class,
                 AgentInfo.class,
                 RepositoryStats.class,
                 RepositoryInfo.class,
                 ReplicationInfo.class,
                 NodeDef.class,
                 NodeInfo.class,
                 NodeException.class);
    }

    private SchemaProvider() {
    }

    private static void register(Class<?>... classes) {
        for (Class<?> clazz : classes) {
            SCHEMAS.put(clazz, Schema.read(readJson(clazz.getSimpleName() + ".json")));
        }
    }

    private static JsonObject readJson(String filename) {
        Class<?> clazz = SchemaProvider.class;
        String resource = clazz.getPackage().getName().replace(".", "/") + "/" + filename;
        try (InputStream inputStream = clazz.getClassLoader().getResourceAsStream(resource);
                Reader reader = new InputStreamReader(inputStream, Charsets.UTF_8);
                javax.json.JsonReader jsonReader = Json.createReader(reader)) {

            return jsonReader.readObject();

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Provides the schema associated with supplied class, or one of its parents classes.
     *
     * @param clazz A class.
     * @return Associated schema.
     */
    public static Schema getSchema(Class<?> clazz) {
        Schema schema = SCHEMAS.get(clazz);
        if (schema != null) {
            return schema;
        }
        Class<?> superClazz = clazz.getSuperclass();
        if (superClazz != null) {
            return getSchema(superClazz);
        }
        throw new AssertionError("No defined schema for " + clazz);
    }
}
