package store.client;

import com.google.common.base.Optional;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.Iterator;
import java.util.List;
import store.common.Config;

public final class App {

    private App() {
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Syntax : store command arguments");
            return;
        }
        Optional<Command> command = Command.of(args[0]);
        if (!command.isPresent()) {
            System.out.println("Unsupported command : " + args[0]);
            return;
        }
        String result = command.get()
                .execute(params(args));

        System.out.println(result);
    }

    private static enum Command {

        CREATE {
            @Override
            public String execute(List<String> params) {
                Iterator<String> it = params.iterator();
                Path root = Paths.get(it.next());
                List<Path> volumes = new ArrayList<>();
                while (it.hasNext()) {
                    volumes.add(Paths.get(it.next()));
                }
                Config config = new Config(root, volumes);
                try (StoreClient client = new StoreClient()) {
                    return client.create(config);
                }
            }
        },
        PUT {
            @Override
            public String execute(List<String> params) {
                try (StoreClient client = new StoreClient()) {
                    return client.put(Paths.get(params.get(0)));
                }
            }
        },
        GET {
            @Override
            public String execute(List<String> params) {
                try (StoreClient client = new StoreClient()) {
                    return client.get(params.get(0));
                }
            }
        },
        DELETE {
            @Override
            public String execute(List<String> params) {
                try (StoreClient client = new StoreClient()) {
                    return client.delete(params.get(0));
                }
            }
        },;

        public abstract String execute(List<String> params);

        public static Optional<Command> of(String arg0) {
            for (Command command : Command.values()) {
                if (arg0.equalsIgnoreCase(command.name())) {
                    return Optional.of(command);
                }
            }
            return Optional.absent();
        }
    }

    private static List<String> params(String[] args) {
        return asList(args)
                .subList(1, args.length);
    }
}
