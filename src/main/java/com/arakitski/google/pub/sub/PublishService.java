package com.arakitski.google.pub.sub;

import com.google.common.collect.ImmutableList;
import java.io.IOException;

/**
 * Interface for the publish message to the google pubsub cloud.
 */
public interface PublishService {

  void publish(String topicName, ImmutableList<String> messageList) throws IOException;

  void addTopic(String topicName) throws IOException;

  void deleteTopic(String topicName) throws IOException;

  void addSubscription(String topicName, String subscriptionName) throws IOException;

  ImmutableList<String> readMessageFromSubscription(String subscriptionName) throws IOException;

  void deleteSubscription(String subscriptionName) throws IOException;

  ImmutableList<String> getSubscriptionList() throws IOException;

  ImmutableList<String> getTopicList() throws IOException;
}
