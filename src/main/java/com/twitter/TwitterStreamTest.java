package com.twitter;

/**
 * Created by Jude on 2014-03-31.
 */
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TwitterStreamTest {
    public static void main(String[] args) throws TwitterException {
        twitter4j.TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        TwitterListener listener = new TwitterListener();
        twitterStream.addListener(listener);
        FilterQuery fq = new FilterQuery();
        String[] query = {"벚꽃", "런닝맨", "밥"};
        fq.track(query);
        twitterStream.filter(fq);
    }
}
