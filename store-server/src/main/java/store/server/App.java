package store.server;

import static java.lang.Thread.currentThread;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class App {

    private App() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Syntax : serve <home>");
            return;
        }
        Path home = Paths.get(args[0]);
        new Server(home).start();
        currentThread().join();
    }
}
