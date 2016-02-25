package akka.persistence.couchbase;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;

public class LoggingConfig {

    public LoggingConfig() {
        final ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.WARNING);
    }
}
