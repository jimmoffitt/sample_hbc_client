package com.twitter.data.datastore;

import com.twitter.data.activityFormats.TweetGnip;
import com.twitter.data.utilities.Environment;

/*
 For sample application that streams realtime Gnip data to MySQL database.

 Schema assumed is presented here:

 HBC client app creates a singleton object of this class.
 A persisted connection is opened when created.


    Note: Written on top of MySQL 5.5. MySQL encoding set to utf8mb4 to handle whacky Tweet characters.

    MySQL needs to handle UTF-8 MB4 encoding:
    * MySQL >= 5.5
    * Set tables to 'utf8mb4' encoding.
    * jdbc Connector >= 5.1.30 (perhaps 5.1.17 and later will work?)
    * No need to specify encoding in jdbc connection string.

    This class depends on MySQL resources. Maven specs:

        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.30</version>
        </dependency>

    rewriteBatchedStatements=true //helps batch INSERT performance.



 */

//Datastore imports.
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.LinkedList;

import org.apache.log4j.Logger;

public class dbMySQL {

    //TODO: database details to be specified in config file...

    private Environment environment = null;

    static final String driver = "com.mysql.jdbc.Driver";

    private String url = "jdbc:mysql://localhost:3306/"; //Default.
    private String db = "";
    private int activity_batch_size = 10;

    //Prepared statements. Reflect database schema.
    private PreparedStatement psActivity = null;
    private PreparedStatement psActor = null;
    private PreparedStatement psHashtag = null;
    private Connection con = null;
    private LinkedList<TweetGnip> queue = new LinkedList<TweetGnip>();

    Logger logger = Logger.getLogger(dbMySQL.class);

    public dbMySQL() {}

    public void setEnvironment(Environment env) {
        environment = env;

        db = environment.dsDatabase();
        //casting
        activity_batch_size = Integer.parseInt(environment.dsBatchSize());


        url = "jdbc:mysql://" + environment.dsHost() + ":" + environment.dsPort()
                + "/" + environment.dsDatabase();
   }

    public boolean connect() {
        //Establish connection. One per object.
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, environment.dsUsername(),environment.dsPassword());
        } catch (Exception e) {
            logger.error("Error connecting to MySQL database.", e);
            return false;
        }

        //Set auto-commit to false
        try {
            con.setAutoCommit(false);
        } catch (Exception e) {
            logger.error("Error set auto-commit to false.", e);
            return false;
        }

        return true;
    }

    /*

    */
     public int getQueueSize() {
         return this.queue.size();
    }

    private boolean saveActivities() {

        boolean bSuccess = true;
        TweetGnip oActivity = null;

        //Prepare Activity INSERT.
        String INSERT_ACTIVITY = "REPLACE INTO activities (tweet_id, user_id, posted_at, body, verb) VALUES(?,?,?,?,?)";
        String INSERT_ACTOR = "REPLACE INTO actors (user_id, handle, bio, location) VALUES(?,?,?,?)";
        String INSERT_HASHTAG = "REPLACE INTO hashtags (tweet_id, hashtag) VALUES(?,?)";

        try {
            psActivity = con.prepareStatement(INSERT_ACTIVITY);
            psActor = con.prepareStatement(INSERT_ACTOR);
            psHashtag = con.prepareStatement(INSERT_HASHTAG);
            for (int i = 0; i < queue.size(); i++) {
                oActivity = queue.removeFirst();

                System.out.println(oActivity.body);

                psActivity.setLong(1, oActivity.tweet_id);
                psActivity.setLong(2, oActivity.actor.user_id);
                psActivity.setString(3, oActivity.posted_at);
                psActivity.setString(4, oActivity.body);
                psActivity.setString(5, oActivity.verb);
                psActivity.addBatch();

                //Prepare Actor INSERT.
                psActor.setLong(1, oActivity.actor.user_id);
                psActor.setString(2,oActivity.actor.handle);
                psActor.setString(3,oActivity.actor.bio);
                psActor.setString(4,oActivity.actor.location.display_name);
                psActor.addBatch();

                //Prepare Hashtag INSERT.
                for (int j = 0; j < oActivity.twitter_entities.hashtags.length - 1; j++) {
                    psHashtag.setLong(1, oActivity.tweet_id);
                    psHashtag.setString(2, oActivity.twitter_entities.hashtags[j].text);
                    psHashtag.addBatch();
                }
            }
        } catch (SQLException e) {
            return false;
        }

        try {
            psActivity.executeBatch();
            psActor.executeBatch();
            psHashtag.executeBatch();
            con.commit();
        } catch (SQLException e) {
            System.out.println("Error with update: " + e);
            System.out.println("body: " + oActivity.body);
            return false;
        }

        return bSuccess;
    }

    /**
     *
     * @param oActivity - full representation of incoming tweet.
     * @return success boolean.
     *
     * Here is where you split it up w.r.t. schema.
     *
     */
     public boolean handleActivity(TweetGnip oActivity) {

        logger.debug("Tweet time: " + oActivity.posted_at);
        //System.out.println("Tweet time: " + oActivity.posted_at);

        queue.offer(oActivity);

        if (queue.size() >= activity_batch_size) {
            saveActivities();
        }

        return true;
    }
}
