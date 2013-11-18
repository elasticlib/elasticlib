package store.server.multipart;

import java.text.ParseException;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.jvnet.mimepull.MIMEMessage;
import org.jvnet.mimepull.MIMEPart;
import store.server.exception.BadRequestException;

/**
 * A form-data multi-part read-only representation.
 */
public class FormDataMultipart extends Multipart {

    private final boolean fileNameFix;

    /**
     * Constructor.
     *
     * @param mimeMessage Lower-level MIME message.
     * @param fileNameFix A hint intended for Jersey's content-disposition parser.
     */
    public FormDataMultipart(MIMEMessage mimeMessage, boolean fileNameFix) {
        super(mimeMessage);
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
            throw new BadRequestException();
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
        if (part.getName().equals(expectedName)) {
            throw new BadRequestException();
        }
        return part;
    }
}
