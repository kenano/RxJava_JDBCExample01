package utils;

/**
 * Created by kenanozdamar on 6/20/17.
 */
public class ThreadUtils {
    public static void sleep( long ms ) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ex) {
            // suppress
        }
    }

    public static void wait( Object monitor ) {
        try {
            // Assumes that the monitor is already "synchronized"
            monitor.wait();
        } catch (InterruptedException ex) {
            // suppress
        }
    }

    public static String currentThreadName() {
        return Thread.currentThread().getName();
    }
}
