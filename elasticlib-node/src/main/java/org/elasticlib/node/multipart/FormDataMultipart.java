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

import java.io.InputStream;
import java.text.ParseException;
import org.elasticlib.common.exception.BadRequestException;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.jvnet.mimepull.MIMEPart;

/**
 * A form-data multi-part read-only representation.
 */
public class FormDataMultipart extends Multipart {

    private final boolean fileNameFix;

    /**
     * Constructor.
     *
     * @param entity Raw entity.
     * @param boundary MIME boundary.
     * @param fileNameFix A hint intended for Jersey's content-disposition parser.
     */
    public FormDataMultipart(InputStream entity, String boundary, boolean fileNameFix) {
        super(entity, boundary);
        this.fileNameFix = fileNameFix;
    }

    @Override
    public FormDataBodyPart next() {
        try {
            MIMEPart mimePart = nextMimePart();
            String header = contentDispositionHeader(mimePart);
            FormDataContentDisposition contentDisposition = new FormDataContentDisposition(header, fileNameFix);
            return new FormDataBodyPart(mimePart, contentDisposition);

        } catch (ParseException e) {
            throw new BadRequestException(e);
        }
    }

    /**
     * Pull the next body-part, checking that its name matches the supplied one. Fails if next body-part's name does not
     * match the expected one.
     *
     * @param expectedName Expected name of this entity next body-part.
     * @return This body part.
     */
    public FormDataBodyPart next(String expectedName) {
        if (!hasNext()) {
            throw new BadRequestException();
        }
        FormDataBodyPart part = next();
        if (!part.getName().equals(expectedName)) {
            throw new BadRequestException();
        }
        return part;
    }
}
