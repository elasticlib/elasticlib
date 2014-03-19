package store.common;

/**
 * Define types of write operations on a volume.
 */
public enum Operation {

    /**
     * A creation, that is adding (or readding) a content and some related info.
     */
    CREATE(0x01),
    /**
     * A pure info update, that is a adding some info without creating or deleting associated content.
     */
    UPDATE(0x02),
    /**
     * A content deletion. Info is updated beside.
     */
    DELETE(0x03);
    private final byte code;

    private Operation(int code) {
        this.code = (byte) code;
    }

    /**
     * Provides operation matching with supplied hexadecimal code. Fails if supplied code is unknown.
     *
     * @param code An operation code.
     * @return Corresponding operation.
     */
    public static Operation fromCode(byte code) {
        for (Operation operation : values()) {
            if (operation.code == code) {
                return operation;
            }
        }
        throw new IllegalArgumentException("0x" + Integer.toHexString(code));
    }

    /**
     * Provides operation matching with supplied string argument. Fails if supplied string is unknown.
     *
     * @param arg An operation as a string, as obtained by a call to toString().
     * @return Corresponding operation.
     */
    public static Operation fromString(String arg) {
        return Operation.valueOf(arg.toUpperCase());
    }

    /**
     * Provides operation hexadecimal code.
     *
     * @return A byte.
     */
    public byte getCode() {
        return code;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
