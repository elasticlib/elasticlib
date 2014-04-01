package store.common.bson;

final class BsonType {

    public static final byte NULL = 0x01;
    public static final byte BINARY = 0x02;
    public static final byte BOOLEAN = 0x03;
    public static final byte INTEGER = 0x04;
    public static final byte DECIMAL = 0x05;
    public static final byte STRING = 0x06;
    public static final byte DATE = 0x07;
    public static final byte OBJECT = 0x08;
    public static final byte ARRAY = 0x09;

    private BsonType() {
    }
}
