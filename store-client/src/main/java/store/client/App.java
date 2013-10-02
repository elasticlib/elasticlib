package store.client;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import static store.client.ByteLengthFormatter.format;
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
                    System.out.println(asString(info));
                }
            }
        },
        FIND {
            @Override
            public void execute(List<String> params) {
                String query = Joiner.on(" ").join(params);
                try (StoreClient client = new StoreClient()) {
                    for (ContentInfo info : client.find(query)) {
                        System.out.println(asString(info));
                    }
                }
            }
        },
        HISTORY {
            @Override
            public void execute(List<String> params) {
                try (StoreClient client = new StoreClient()) {
                    for (Event event : client.history()) {
                        System.out.println(asString(event));
                    }
                }
            }
        },
        DIGEST {
            @Override
            public void execute(List<String> params) {
                Digest digest = digest(Paths.get(params.get(0)));
                System.out.println(asString(digest));
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

    private static String asString(ContentInfo info) {
        StringBuilder builder = new StringBuilder();
        builder.append("Hash")
                .append(comma())
                .append(info.getHash())
                .append(System.lineSeparator())
                .append("Size")
                .append(comma())
                .append(format(info.getLength()));
        for (Entry<String, Object> entry : info.getMetadata().entrySet()) {
            builder.append(entry.getKey())
                    .append(comma())
                    .append(entry.getValue());
        }
        builder.append(System.lineSeparator());
        return builder.toString();
    }

    private static String asString(Digest digest) {
        return new StringBuilder()
                .append("Hash")
                .append(comma())
                .append(digest.getHash())
                .append(System.lineSeparator())
                .append("Size")
                .append(comma())
                .append(format(digest.getLength()))
                .toString();
    }

    private static String asString(Event event) {
        return new StringBuilder()
                .append(event.getOperation().toString())
                .append(System.lineSeparator())
                .append(indent())
                .append("Hash")
                .append(comma())
                .append(event.getHash())
                .append(System.lineSeparator())
                .append(indent())
                .append("Date")
                .append(comma())
                .append(event.getTimestamp())
                .append(System.lineSeparator())
                .append(indent())
                .append(event.getUids().size() > 1 ? "Volumes" : "Volume")
                .append(comma())
                .append(Joiner.on(", ").join(event.getUids()))
                .append(System.lineSeparator())
                .toString();
    }

    private static String indent() {
        return "    ";
    }

    private static String comma() {
        return " : ";
    }

    private static List<String> params(String[] args) {
        return Arrays.asList(args).subList(1, args.length);
    }
}
