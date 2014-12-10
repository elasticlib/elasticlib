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
package org.elasticlib.node;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import static java.nio.file.Files.walkFileTree;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.hash.Hash;
import static org.elasticlib.node.config.NodeConfig.JE_LOCK_TIMEOUT;
import static org.elasticlib.node.config.NodeConfig.STAGING_SESSIONS_CLEANUP_ENABLED;
import static org.elasticlib.node.config.NodeConfig.STAGING_SESSIONS_CLEANUP_INTERVAL;
import static org.elasticlib.node.config.NodeConfig.STAGING_SESSIONS_MAX_SIZE;
import static org.elasticlib.node.config.NodeConfig.STAGING_SESSIONS_TIMEOUT;
import static org.elasticlib.node.config.NodeConfig.STORAGE_SYNC_ENABLED;
import static org.elasticlib.node.config.NodeConfig.STORAGE_SYNC_INTERVAL;
import static org.elasticlib.node.config.NodeConfig.TASKS_POOL_SIZE;

/**
 * Test utilities.
 */
public final class TestUtil {

    /**
     * An unknown hash.
     */
    public static final Hash UNKNOWN_HASH = new Hash("88cd962fec779a3abafa95aad8ace74cae767427");
    /**
     * A test content.
     */
    public static final TestContent LOREM_IPSUM = TestContent.of("loremIpsum.txt", "text/plain");

    private TestUtil() {
    }

    /**
     * @return Test config.
     */
    public static Config config() {
        return new Config()
                .set(TASKS_POOL_SIZE, 1)
                .set(STAGING_SESSIONS_MAX_SIZE, 10)
                .set(STAGING_SESSIONS_TIMEOUT, "10 s")
                .set(STAGING_SESSIONS_CLEANUP_ENABLED, true)
                .set(STAGING_SESSIONS_CLEANUP_INTERVAL, "10 s")
                .set(STORAGE_SYNC_ENABLED, true)
                .set(STORAGE_SYNC_INTERVAL, "10 s")
                .set(JE_LOCK_TIMEOUT, "1 min");
    }

    /**
     * Recursively delete everything under (and including) supplied path.
     *
     * @param path Path of the file/directory to delete.
     * @throws IOException If an IO error occurs.
     */
    public static void recursiveDelete(Path path) throws IOException {
        walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                if (e == null) {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;

                } else {
                    throw e;
                }
            }
        });
    }

    /**
     * Checks that supplied runnable will eventually succesfully run.
     *
     * @param runnable Runnable to execute.
     */
    public static void async(Runnable runnable) {
        int timeout = 60_000;
        int delay = 25;
        int time = 0;
        while (true) {
            try {
                wait(delay);
                time += delay;
                delay *= 4;
                if (delay > 5000) {
                    delay = 5000;
                }
                runnable.run();
                return;

            } catch (Exception e) {
                if (time >= timeout) {
                    throw new AssertionError(e);
                }
            } catch (AssertionError e) {
                if (time >= timeout) {
                    throw e;
                }
            } catch (Error e) {
                throw e;
            }
        }
    }

    private static void wait(int millis) {
        try {
            Thread.sleep(millis);

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
