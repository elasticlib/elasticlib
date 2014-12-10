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

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import javax.ws.rs.core.MediaType;

/**
 * Represents a handle on a content in a repository.
 */
public class Content implements AutoCloseable {

    private final Optional<String> fileName;
    private final MediaType mediaType;
    private final long length;
    private final InputStream inputStream;

    /**
     * Constructor.
     *
     * @param fileName Optional content file name.
     * @param mediaType Content media type.
     * @param length Content length in bytes.
     * @param inputStream Content input-stream.
     */
    public Content(Optional<String> fileName, MediaType mediaType, long length, InputStream inputStream) {
        this.fileName = fileName;
        this.mediaType = mediaType;
        this.length = length;
        this.inputStream = inputStream;
    }

    /**
     * @return Content file name, if any.
     */
    public Optional<String> getFileName() {
        return fileName;
    }

    /**
     * @return Content type.
     */
    public MediaType getContentType() {
        return mediaType;
    }

    /**
     * @return Content length in bytes.
     */
    public long getLength() {
        return length;
    }

    /**
     * @return Content input-stream.
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
