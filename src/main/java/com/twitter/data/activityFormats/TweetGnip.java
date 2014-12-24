package com.twitter.data.activityFormats;

/**
 * POJO that stores an activity (tweet) in the Activity Stream object.
 *
 * Set methods can do special processing, like take tweet id string and set an numeric id.
 *
 * String passed in here come from Deserializer (which maps JSON key names to POJO attributes.
 *
 */

import com.google.gson.annotations.SerializedName;

public class TweetGnip {

    //Activity Stream format
    public static final String FORMAT = "AS";

    //Root-level attributes.
    @SerializedName("id")
    public String id;
    public long tweet_id;

    @SerializedName("postedTime")
    public String posted_at;

    public String body;
    public String verb;

    //Every tweet has one Actor.
    public Actor actor = new Actor();
    //Every tweet can have "entities", metadata arrays like hashtags, mentions, and URLs.
    public TwitterEntities twitter_entities = new TwitterEntities();
    //Gnip tweets can have extra metadata.
    public Gnip gnip = new Gnip();


    public static class Actor {
        @SerializedName("id")
        public String id;
        public long user_id;

        @SerializedName("preferredUsername")
        public String handle;

        @SerializedName("displayName")
        public String display_name;

        @SerializedName("summary")
        public String bio;

        public static class Location {
            @SerializedName("displayName")
            public String display_name;
        }
        public Location location = new Location();

        @SerializedName("utcOffset")
        public int utc_offset;

        @SerializedName("followersCount")
        public int followers_count;

        @SerializedName("friendsCount")
        public int friends_count;

        @SerializedName("statusesCount")
        public int statuses_count;

        //TODO handle languages array.
    }

    //The URL class is used by both Twitter entities and Gnip classes.
    static class URL {
        public String url;
        public String expanded_url;
    }


    //Every tweet can have a set of "entities", metadata arrays like hashtags, mentions, and URLs.
    //Add media metadata array.
    public static class TwitterEntities {

        //Metadata arrays.

        public static class Hashtag {
            public String text;
        }

        @SerializedName("hashtags")
        public Hashtag[] hashtags;

        static class UserMention {
            public String id;
            public String name;
        }

        @SerializedName("user_mentions")
        public UserMention[] mentions;

        public URL[] urls;
    }

    //Tweets coming from Gnip data services can have additional geo, rules and other metadata.
    public static class Gnip {

        public static class MatchingRule {
            public String value;
            public String tag;
        }

        @SerializedName("matching_rules")
        public MatchingRule[] matchingRules;

        public URL[] urls;
    }


    //Methods.

    public void setTweetID() {
        String[] parts = this.id.split(":");
        this.tweet_id = Long.parseLong(parts[parts.length - 1]);
    }

    public void setUserID() {
        String[] parts = this.actor.id.split(":");
        this.actor.user_id = Long.parseLong(parts[parts.length - 1]);
    }

    @Override
    public String toString() {

        /*
        System.out.println("This tweet has " + this.twitter_entities.hashtags.length + " hashtags.");
        System.out.println(this.twitter_entities.hashtags[0].text);

        System.out.println("This tweet matched " + this.gnip.matchingRules.length + " rules.");
        System.out.println(this.gnip.matchingRules[0].value);
        */

        return "AS format:" + this.tweet_id + " from:" + this.actor.handle + " tweet body:" + this.body + " at:" + this.posted_at  + " hashtags:" + this.twitter_entities.hashtags;

    }

}
