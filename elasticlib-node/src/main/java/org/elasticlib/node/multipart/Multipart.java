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
package org.elasticlib.node.multipart;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.NoSuchElementException;
import org.elasticlib.common.exception.BadRequestException;
import org.glassfish.jersey.media.multipart.ContentDisposition;
import org.jvnet.mimepull.Header;
import org.jvnet.mimepull.MIMEMessage;
import org.jvnet.mimepull.MIMEPart;

/**
 * A read-only multi-part entity. Intended to be accessed in a streaming fashion in order to avoid the entity to be
 * written in a temporary file.
 */
public class Multipart implements Closeable {

    private final InputStream entity;
    private final MIMEMessage mimeMessage;
    private int index;
    private boolean loaded;

    /**
     * Constructor.
     *
     * @param entity Raw entity.
     * @param boundary MIME boundary.
     */
    public Multipart(InputStream entity, String boundary) {
        this.mimeMessage = new MIMEMessage(entity, boundary);
        this.entity = entity;
    }

    /**
     * @return true If a subsequent call to <code>next()</code> would return the next body-part of this entity, rather
     * than throwing an exception.
     */
    public final boolean hasNext() {
        if (loaded) {
            return true;
        }
        if (mimeMessage.makeProgress()) {
            loaded = true;
            return true;
        }
        return false;
    }

    protected final MIMEPart nextMimePart() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        MIMEPart mimePart = mimeMessage.getPart(index);
        loaded = false;
        index++;

        return mimePart;
    }

    protected final String contentDispositionHeader(MIMEPart mimePart) {
        for (Header header : mimePart.getAllHeaders()) {
            if (header.getName().equalsIgnoreCase("Content-Disposition")) {
                return header.getValue();
            }
        }
        return null;
    }

    /**
     * @return The next body-part of this multi-part. Throws an unchecked exception if the end of entity has already
     * been reached.
     */
    public BodyPart next() {
        try {
            MIMEPart mimePart = nextMimePart();
            String contentDispositionHeader = contentDispositionHeader(mimePart);
            if (contentDispositionHeader != null) {
                return new BodyPart(mimePart, new ContentDisposition(contentDispositionHeader));
            }
            return new BodyPart(mimePart, null);

        } catch (ParseException e) {
            throw new BadRequestException(e);
        }
    }

    @Override
    public void close() throws IOException {
        entity.close();
    }
}
