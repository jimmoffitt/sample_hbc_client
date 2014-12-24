/**
    Client app that using Twitter Hosebird Client (HBC) to stream from Gnip Powertrack.
    Data is written to a datastore (MySQL so far).
 **/
package com.twitter.data.stream;

//Twitter Hosebird Client: https://github.com/twitter/hbc
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.httpclient.auth.BasicAuth;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.RealTimeEnterpriseStreamingEndpoint;
import com.twitter.hbc.core.processor.LineStringProcessor;

//Datastore details.
import com.twitter.data.datastore.dbMySQL;
import com.twitter.data.activityFormats.TweetGnip;
import com.twitter.data.activityFormats.JSONUtils;

//Configuration details, will read from config.properties file.
import com.twitter.data.utilities.Environment;

//Provides in memory buffer for queuing activities from HBC.
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class hbcClient {

    //This Application hosts some singleton objects.
    //Datastore object: MySQL for this example.
    private dbMySQL oDB = null; //Constructor establishes db connect...

    //
    private static JSONUtils oJSON = new JSONUtils(); //Converts JSON to POJO.

    //Configuration class and logging service.
    private static Environment environment = new Environment();
    private static Logger logger = Logger.getLogger(hbcClient.class);

    public hbcClient() {
        try {

            //Create logging details.
            PropertyConfigurator.configure("log4j.properties");
            logger.info("Starting HBC Client...");

        } catch (Exception e) {
            //No logger, so don't log.
            System.out.println("Failed to start logging HBC Client..." + e);
        }
    }

    //Command-line: username, password, account, product, label
    public static void main(String[] args) {

        hbcClient oClient = new hbcClient();

        if (args.length >= 5) {
            oClient.setEnvironmentFromCL(args);
        }
        oClient.run();
    }

    /**
     * If
     * @param args
     */

    public void setEnvironmentFromCL(String[] args) {
        this.environment.setUserName(args[0]);
        this.environment.setPassword(args[1]);
        this.environment.setAccountName(args[2]);
        this.environment.setProductName(args[3]);
        this.environment.setStreamLabel(args[4]);
        //TODO: extend to include Datastore details? Or drop.
    }

    public void run() {

        logger.debug("Run method...");

        //Set-up datastore details and connect.
        oDB = new dbMySQL();
        oDB.setEnvironment(this.environment);
        oDB.connect();

        /** Declare the host you want to connect to, the endpoint, and Basic Authentication for Gnip streams */
        BasicAuth auth = new BasicAuth(this.environment.userName(), this.environment.userPassword());
        //TODO - build in support for PowerTrack Backfill.
        RealTimeEnterpriseStreamingEndpoint endpoint = new RealTimeEnterpriseStreamingEndpoint(
                this.environment.accountName(), this.environment.productName(), this.environment.streamLabel());

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100000);

        // NEW LineStringProcessor to handle Gnip formatted streaming HTTP.
        LineStringProcessor processor = new LineStringProcessor(queue);

        // Build a hosebird client just like before
        ClientBuilder builder = new ClientBuilder()
                .name("PowerTrackClient-01")
                .hosts(Constants.ENTERPRISE_STREAM_HOST)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(processor) ;

        Client client = builder.build();    // optional: use this if you want to process client events

        client.connect(); // Attempts to establish a connection.

        TweetGnip oActivity; //Creating a singleton Tweet.

        while (!client.isDone()) {
            try {
                String message = queue.take();

                // Here is where you could put it on a queue for another thread to come in and take care of the message
                oActivity = oJSON.decode(message); //JSON format --> POJO.
                oActivity.setTweetID();
                oActivity.setUserID();
                boolean result = oDB.handleActivity(oActivity);  //POJO --> MySQL.

            } catch (InterruptedException e) {
                logger.fatal("Error while unloading HBC queue..." + e);
                System.out.println(e);
            }
        }
    }
}
