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
package org.elasticlib.common.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.yaml.YamlReader;
import org.elasticlib.common.yaml.YamlWriter;
import org.yaml.snakeyaml.error.YAMLException;

/**
 * Config reading and writing utils.
 */
public final class ConfigReadWrite {

    private ConfigReadWrite() {
    }

    /**
     * Reads config from the class path. Looks for a resource located in same package as supplied class, with supplied
     * file name. Fails if such resource is not a valid YAML file. Returns the empty config if such resource does not
     * exist or is empty.
     *
     * @param clazz Class used to locate the resource holding the config to read.
     * @param fileName The resource file name.
     * @return A new config instance.
     */
    public static Config readFromClassPath(Class<?> clazz, String fileName) {
        String resource = clazz.getPackage().getName().replace(".", "/") + "/" + fileName;
        try (InputStream stream = clazz.getClassLoader().getResourceAsStream(resource)) {
            if (stream == null) {
                return Config.empty();
            }
            return read(stream);

        } catch (IOException e) {
            throw new ConfigException(e);
        }
    }

    /**
     * Reads config at supplied path. Fails if file at supplied path is not a valid YAML file. Returns the empty config
     * instance if such file does not exist or is empty.
     *
     * @param path File path.
     * @return A new config instance.
     */
    public static Config read(Path path) {
        if (!Files.exists(path)) {
            return Config.empty();
        }
        try (InputStream input = Files.newInputStream(path)) {
            return read(input);

        } catch (IOException e) {
            throw new ConfigException(e);
        }
    }

    private static Config read(InputStream stream) {
        try (YamlReader reader = new YamlReader(stream)) {
            Optional<Value> value = reader.readValue();
            if (!value.isPresent()) {
                return Config.empty();
            }
            return new Config(value.get());

        } catch (IOException | YAMLException e) {
            throw new ConfigException(e);
        }
    }

    /**
     * Write supplied config at supplied path.
     *
     * @param path File path.
     * @param config Config to write.
     */
    public static void write(Path path, Config config) {
        try (OutputStream output = Files.newOutputStream(path);
                YamlWriter writer = new YamlWriter(output)) {

            writer.writeValue(config.asValue());

        } catch (IOException e) {
            throw new ConfigException(e);
        }
    }
}
