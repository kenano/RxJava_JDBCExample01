package jdbc;

import rx.Observable;
import rx.util.functions.Action1;
import rx.util.functions.Func0;
import rx.util.functions.Func1;
import utils.ThreadUtils;

/**
 * Created by kenanozdamar on 6/19/17.
 */
public class JdbcExample {
    public static void main(String[] arg) {

        // Create a simple error handler that will emit the stack trace for a given exception
        Action1<Throwable> simpleErrorHandler = (throwable) ->{
            throwable.printStackTrace();
        };

        // Initialize the test Derby database...this also resets its contents
        TestDatabase.init();

        // Resource Factory function to create a TestDatabase connection and wrap it in a ConnectionSubscription
        Func0<ConnectionSubscription> resourceFactory = () -> {
            return new ConnectionSubscription(TestDatabase.createConnection());
        };

        // Observable Factory function to create the resultset that we want.
        Func1<ConnectionSubscription, Observable<String>> greekAlphabetList = (connectionSubscription) -> {
            return TestDatabase.selectGreekAlphabet(connectionSubscription);
        };

        Observable<String> t = Observable.using(resourceFactory, greekAlphabetList);
        t.subscribe(
                (letter) -> {
                    System.out.println(ThreadUtils.currentThreadName() + " - " + letter);
                },
                simpleErrorHandler,
                () -> {
                    System.out.println( "onCompleted" );
                });

    }
}
