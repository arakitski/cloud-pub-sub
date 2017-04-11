package com.arakitski.google.pubsub;

import com.google.common.collect.ImmutableList;

import java.io.IOException;

/**
 * Interface for the read message to the google pubsub cloud.
 */
public interface SubscriptionService {

  boolean isSubscriptionExist(String subscriptionName) throws IOException;

  void addSubscription(String topicName, String subscriptionName) throws IOException;

  ImmutableList<String> readMessageList(String subscriptionName) throws IOException;

  void deleteSubscription(String subscriptionName) throws IOException;

  ImmutableList<String> getSubscriptionList() throws IOException;
}
