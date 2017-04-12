package com.arakitski.google.pubsub.api;

import com.google.common.collect.ImmutableList;

import java.io.IOException;

/**
 * Interface for the read message to the google pubsub cloud.
 */
public interface SubscriptionApiService {

  boolean isSubscriptionExist(String subscriptionName) throws IOException;

  void addSubscription(String topicName, String subscriptionName) throws IOException;

  ImmutableList<String> readMessageList(String subscriptionName) throws IOException;

  void deleteSubscription(String subscriptionName) throws IOException;

  ImmutableList<String> getSubscriptionList() throws IOException;
}
