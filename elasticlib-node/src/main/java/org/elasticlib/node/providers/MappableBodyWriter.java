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
package org.elasticlib.node.providers;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.json.JsonWriter;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import org.elasticlib.common.json.JsonWriting;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.yaml.YamlWriter;
import static org.elasticlib.node.providers.MessageBodyWriterUtil.isJson;
import static org.elasticlib.node.providers.MessageBodyWriterUtil.isYaml;
import static org.elasticlib.node.providers.MessageBodyWriterUtil.writerFactory;

/**
 * Custom HTTP body writer for Mappable instances. Produces either JSON or YAML. For JSON, nicely format output if
 * request contains query parameter "pretty=true".
 */
@Provider
@Produces({"application/json", "text/json", "application/yaml", "text/yaml", "*/*"})
public class MappableBodyWriter implements MessageBodyWriter<Mappable> {

    @Context
    private UriInfo uriInfo;

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        if (!Mappable.class.isAssignableFrom(type)) {
            return false;
        }
        return isJson(mediaType) || isYaml(mediaType);
    }

    @Override
    public long getSize(Mappable t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        // Deprecated by JAX-RS 2.0 and ignored by Jersey runtime.
        return -1;
    }

    @Override
    public void writeTo(Mappable t,
                        Class<?> type,
                        Type genericType,
                        Annotation[] annotations,
                        MediaType mediaType,
                        MultivaluedMap<String, Object> httpHeaders,
                        OutputStream entityStream) throws IOException {

        if (isYaml(mediaType)) {
            writeYamlTo(t, entityStream);

        } else {
            writeJsonTo(t, entityStream);
        }
    }

    private void writeYamlTo(Mappable t, OutputStream entityStream) throws IOException {
        try (YamlWriter writer = new YamlWriter(entityStream)) {
            writer.write(t);
        }
    }

    private void writeJsonTo(Mappable t, OutputStream entityStream) {
        try (JsonWriter writer = writerFactory(uriInfo).createWriter(entityStream)) {
            writer.write(JsonWriting.write(t));
        }
    }
}
