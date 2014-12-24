package com.twitter.data.activityFormats;

import java.lang.reflect.Type;

import com.google.gson.*;

/**
 * Created by jmoffitt on 11/3/14.
 */
public class TweetGnipDeserializer implements JsonDeserializer<TweetGnip> {

    @Override
    public TweetGnip deserialize(final JsonElement json, final Type typeOfT, final JsonDeserializationContext context)
            throws JsonParseException {

        JsonObject activityObject = null;

        try {
            activityObject = json.getAsJsonObject();
        }
        catch (JsonParseException e) {
            System.out.println("error");

        }


        TweetGnip activity = new TweetGnip();

        //Parse root level attributes.
        //activity.setTweetID(activityObject.get("id").getAsString());

        //Parse Actor object attributes.
        //Get actor attributes from actor JSON object.
        final JsonObject actorObject = activityObject.get("actor").getAsJsonObject();
        //activity.actor.setUserID(activityObject.get("id").getAsString());

        //Parse Twitter entities.
        final JsonObject twitterEntitiesrObject = activityObject.get("twitter_entities").getAsJsonObject();

/*

        final JsonArray hashtagArray = twitterEntitiesrObject.get("hashtags").getAsJsonArray();

        String[] hashtags = new String[hashtagArray.size()];
        for (JsonElement hashtag : hashtagArray) {
            final JsonObject hashtagObject = (JsonObject) hashtag;
            hashtags[hashtags.length] = hashtagObject.get("text").getAsString();
        }
        activity.twitter_entities.setHashtags(hashtags);
*/

        //Parse Gnip object attributes.
        //Get gnip attributes from gnip JSON object.
        final JsonObject gnipObject = activityObject.get("gnip").getAsJsonObject();
        final JsonArray ruleArray = twitterEntitiesrObject.get("matching_rules").getAsJsonArray();
        String[] ruleValues = new String[ruleArray.size()];  //TODO: handle 'complex' rules, when no values provided, just tags.
        String[] ruleTags = new String [ruleArray.size()];
        for (JsonElement rule : ruleArray) {
            final JsonObject ruleObject = (JsonObject) rule;
            ruleValues[ruleValues.length] = ruleObject.get("value").getAsString();
            ruleTags[ruleValues.length] = ruleObject.get("tag").getAsString();
        }

        return activity;

    }


}
