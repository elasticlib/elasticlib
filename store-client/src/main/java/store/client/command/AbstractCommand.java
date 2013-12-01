package store.client.command;

import static com.google.common.base.CaseFormat.*;

abstract class AbstractCommand implements Command {

    @Override
    public String name() {
        return UPPER_CAMEL.to(UPPER_UNDERSCORE, getClass().getSimpleName());
    }
}
