package org.elasticlib.node.multipart;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.jvnet.mimepull.MIMEPart;

/**
 * The body-part of a form data multi-part.
 */
public class FormDataBodyPart extends BodyPart {

    private final String name;

    FormDataBodyPart(MIMEPart mimePart, FormDataContentDisposition contentDisposition) {
        super(mimePart, contentDisposition);
        name = contentDisposition.getName();
    }

    /**
     * @return The part's name.
     */
    public String getName() {
        return name;
    }
}
