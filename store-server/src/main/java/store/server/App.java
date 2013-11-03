package store.server;

import static java.lang.Thread.currentThread;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Server starting.
 */
public final class App {

    private App() {
    }

    /**
     * Main method.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Syntax : serve <home>");
            return;
        }
        Path home = Paths.get(args[0]);
        new Server(home).start();
        try {
            currentThread().join();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
