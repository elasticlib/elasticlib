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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.elasticlib.console.exception.RequestFailedException;

/**
 * Files utilities.
 */
public final class Directories {

    private static final Path USER_HOME = Paths.get(System.getProperty("user.home"));
    private static final Path HOME = USER_HOME.resolve(".elasticlib");
    private static Path workingDirectory = Paths.get(".").toAbsolutePath().normalize();

    private Directories() {
    }

    /**
     * @return The client home directory.
     */
    public static Path home() {
        return HOME;
    }

    /**
     * @return The current working directory.
     */
    public static Path workingDirectory() {
        return workingDirectory;
    }

    /**
     * Changes current working directory.
     *
     * @param path New working directory.
     */
    public static void changeWorkingDirectory(Path path) {
        Path absolute = resolve(path);
        if (!Files.exists(absolute)) {
            throw new RequestFailedException(absolute + " does not exist");
        }
        if (!Files.isDirectory(absolute)) {
            throw new RequestFailedException(absolute + " is not a directory");
        }
        workingDirectory = absolute;
    }

    /**
     * Changes current working directory to the user home directory.
     */
    public static void changeToUserHome() {
        workingDirectory = USER_HOME;
    }

    /**
     * Resolves supplied path against current working directory.
     *
     * @param path A path.
     * @return The resulting path.
     */
    public static Path resolve(Path path) {
        return path.isAbsolute() ? path : workingDirectory.resolve(path).normalize();
    }

    /**
     * Converts supplied string as a Path instance and resolves this path against current working directory.
     *
     * @param path A string representing a path.
     * @return The resulting path.
     */
    public static Path resolve(String path) {
        return resolve(Paths.get(path));
    }
}
