package com.twitter.data.activityFormats;

//https://code.google.com/p/google-gson/
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

//Objects needed for converting JSON objects to a POJO.
//Both are Activity Stream (AS) specific!
import com.twitter.data.activityFormats.TweetGnip;
import com.twitter.data.activityFormats.TweetGnipDeserializer;
//TODO: Extend to 'original' format.

public class JSONUtils {

    TweetGnip activity = new TweetGnip();

    //Decode JSON and build a Tweet POJO.
    public TweetGnip decode(String message) {

        final GsonBuilder gsonBuilder = new GsonBuilder();

        gsonBuilder.registerTypeAdapter(TweetGnip.class, new TweetGnipDeserializer());

        Gson gson = new GsonBuilder().create();

        //decode JSON string to an POJO 'activity' object.
        try {
            activity = gson.fromJson(message, TweetGnip.class);
        } catch (Exception e) {
            System.out.println("stop");
        }

        //System.out.println(activity.toString());

        return activity;
    }


}
