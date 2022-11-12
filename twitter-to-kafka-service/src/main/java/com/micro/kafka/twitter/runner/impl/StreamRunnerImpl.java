package com.micro.kafka.twitter.runner.impl;

import com.micro.kafka.twitter.config.TwitterToKafkaServiceConfigData;
import com.micro.kafka.twitter.listener.TwitterKafkaStatusListener;
import com.micro.kafka.twitter.runner.StreamRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Optional;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets" , havingValue = "false", matchIfMissing = false)
public class StreamRunnerImpl implements StreamRunner {
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterStream twitterStream;

    public StreamRunnerImpl(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
                            TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override

    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();

    }

    @PreDestroy
    public void shutDown(){
        Optional.ofNullable(twitterStream)
                .ifPresent(TwitterStream::shutdown);
    }

    private void addFilter() {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        twitterStream.filter(new FilterQuery(keywords));
    }
}
