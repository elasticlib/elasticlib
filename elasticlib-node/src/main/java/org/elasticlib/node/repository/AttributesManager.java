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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import org.elasticlib.common.exception.IOFailureException;
import org.elasticlib.common.exception.InvalidRepositoryPathException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.value.Value;
import static org.elasticlib.common.value.Value.of;
import org.elasticlib.common.yaml.YamlReader;
import org.elasticlib.common.yaml.YamlWriter;
import static org.yaml.snakeyaml.DumperOptions.LineBreak.UNIX;

/**
 * Manages immutable persisted attributes of a repository, that are currently its name and GUID.
 */
class AttributesManager {

    private static final String ATTRIBUTES = "attributes.yml";
    private static final String NAME = "name";
    private static final String GUID = "guid";

    private final String name;
    private final Guid guid;

    private AttributesManager(Map<String, Value> attributes) {
        this.name = requireNonNull(attributes.get(NAME).asString());
        this.guid = requireNonNull(attributes.get(GUID).asGuid());
    }

    public static AttributesManager create(Path path) {
        Map<String, Value> attributes = new MapBuilder()
                .put(NAME, path.getFileName().toString())
                .put(GUID, Guid.random())
                .build();

        try (OutputStream output = Files.newOutputStream(path.resolve(ATTRIBUTES));
                YamlWriter writer = new YamlWriter(output, UNIX)) {
            writer.writeValue(of(attributes));

        } catch (IOException e) {
            throw new IOFailureException(e);
        }
        return new AttributesManager(attributes);
    }

    public static AttributesManager open(Path path) {
        if (!Files.exists(path.resolve(ATTRIBUTES))) {
            throw new InvalidRepositoryPathException();
        }
        try (InputStream input = Files.newInputStream(path.resolve(ATTRIBUTES));
                YamlReader reader = new YamlReader(input)) {

            Optional<Value> value = reader.readValue();
            if (!value.isPresent()) {
                throw new InvalidRepositoryPathException();
            }
            return new AttributesManager(value.get().asMap());

        } catch (IOException e) {
            throw new IOFailureException(e);
        }
    }

    public String getName() {
        return name;
    }

    public Guid getGuid() {
        return guid;
    }
}
