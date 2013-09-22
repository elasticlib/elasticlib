package store.server.lock;

class Counter {

    private int value;

    public void increment() {
        value++;
    }

    public void decrement() {
        value--;
    }

    public int value() {
        return value;
    }
}
