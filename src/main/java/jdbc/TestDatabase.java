package jdbc;

import org.apache.commons.io.FileUtils;
import rx.Observable;
import utils.DataGenerator;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;


/**
 * Created by kenanozdamar on 6/19/17.
 */
public class TestDatabase {
    public static void init() {
        File databaseDirectory = new File("./pluralsightTest");

        try {
            FileUtils.deleteDirectory(databaseDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        Connection connection = null;
        try {
            DriverManager.getConnection("jdbc:derby:pluralSightTest;create=true");
            connection = DriverManager.getConnection("jdbc:derby:pluralSightTest");
        } catch (SQLException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }

        // Initialize the database with some simple information
        Statement s = null;
        try {
            s = connection.createStatement();
            String sqlStatement = "CREATE TABLE GREEK_ALPHABET ( ID BIGINT, LETTER VARCHAR(20) )";
            s.execute(sqlStatement);

            int id = 1;
            for (String nextLetter : DataGenerator.generateGreekAlphabet()) {
                sqlStatement = "INSERT INTO GREEK_ALPHABET ( ID , LETTER ) VALUES ( " + (id++) + ",\'" + nextLetter + "\' )";
                s.execute(sqlStatement);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (SQLException e) {
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    public static Connection createConnection() {
        try {
            return DriverManager.getConnection("jdbc:derby:pluralSightTest");
        } catch (SQLException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    //
    public static Observable<String> selectGreekAlphabet(ConnectionSubscription connSubscription) {

        try {
            Statement s = connSubscription.getConnection().createStatement();
            connSubscription.registerResourceForClose(s);

            ResultSet rs = s.executeQuery("SELECT LETTER FROM GREEK_ALPHABET");
            connSubscription.registerResourceForClose(rs);

            ArrayList<String> returnList = new ArrayList<>();
            while (rs.next()) {
                returnList.add(rs.getString("LETTER"));
            }

            return Observable.from(returnList);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
