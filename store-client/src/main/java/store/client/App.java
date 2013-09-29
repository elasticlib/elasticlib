package store.client;

import com.google.common.base.Optional;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import static store.client.DigestUtil.digest;
import store.common.Config;
import store.common.ContentInfo;
import store.common.Digest;
import store.common.Event;

public final class App {

    private App() {
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Syntax : store command params...");
            return;
        }
        Optional<Command> command = Command.of(args[0]);
        if (!command.isPresent()) {
            System.out.println("Unsupported command : " + args[0]);
            return;
        }
        try {
            command.get().execute(params(args));

        } catch (RequestFailedException e) {
            System.out.println(e.getMessage());
        }
    }

    private static enum Command {

        CREATE {
            @Override
            public void execute(List<String> params) {
                Iterator<String> it = params.iterator();
                Path root = Paths.get(it.next());
                List<Path> volumes = new ArrayList<>();
                while (it.hasNext()) {
                    volumes.add(Paths.get(it.next()));
                }
                Config config = new Config(root, volumes);
                try (StoreClient client = new StoreClient()) {
                    client.create(config);
                }
            }
        },
        DROP {
            @Override
            public void execute(List<String> params) {
                try (StoreClient client = new StoreClient()) {
                    client.drop();
                }
            }
        },
        PUT {
            @Override
            public void execute(List<String> params) {
                try (StoreClient client = new StoreClient()) {
                    client.put(Paths.get(params.get(0)));
                }
            }
        },
        DELETE {
            @Override
            public void execute(List<String> params) {
                try (StoreClient client = new StoreClient()) {
                    client.delete(params.get(0));
                }
            }
        },
        GET {
            @Override
            public void execute(List<String> params) {
                try (StoreClient client = new StoreClient()) {
                    client.get(params.get(0));
                }
            }
        },
        INFO {
            @Override
            public void execute(List<String> params) {
                try (StoreClient client = new StoreClient()) {
                    ContentInfo info = client.info(params.get(0));
                    System.out.println("Hash : " + info.getHash());
                    System.out.println("Length : " + info.getLength());
                    for (Entry<String, Object> entry : info.getMetadata().entrySet()) {
                        System.out.println(entry.getKey() + " : " + entry.getValue());
                    }
                }
            }
        },
        LOG {
            @Override
            public void execute(List<String> params) {
                try (StoreClient client = new StoreClient()) {
                    for (Event event : client.log()) {
                        System.out.println("Hash : " + event.getHash());
                        System.out.println("Timestamp : " + event.getTimestamp());
                        System.out.println("Operation : " + event.getOperation());
                        System.out.println("Uids : " + event.getUids());
                        System.out.println();
                    }
                }
            }
        },
        DIGEST {
            @Override
            public void execute(List<String> params) {
                Digest digest = digest(Paths.get(params.get(0)));
                System.out.println("Hash : " + digest.getHash());
                System.out.println("Length : " + digest.getLength());
            }
        };

        public abstract void execute(List<String> params);

        public static Optional<Command> of(String arg0) {
            for (Command command : Command.values()) {
                if (command.name().equalsIgnoreCase(arg0)) {
                    return Optional.of(command);
                }
            }
            return Optional.absent();
        }
    }

    private static List<String> params(String[] args) {
        return Arrays.asList(args).subList(1, args.length);
    }
}
