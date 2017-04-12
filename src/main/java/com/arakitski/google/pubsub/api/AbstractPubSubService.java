package com.arakitski.google.pubsub.api;

import com.google.api.services.pubsub.Pubsub;

/**
 * Abstract class for the pubsub services.
 */
public class AbstractPubSubService {

  protected final String projectPath;
  protected final Pubsub client;

  public AbstractPubSubService(Pubsub client, String projectId) {
    this.client = client;
    projectPath = "projects/" + projectId;
  }

  /**
   * @param topicName must start with a letter, and contain only letters, numbers, '%', '~', '_',
   *                  '.' or '+'
   */
  final String buildTopicName(String topicName) {
    return projectPath + "/topics/" + topicName;
  }

  /**
   * @param subscriptionName must start with a letter, and contain only letters, numbers, '%', '~',
   *                         '_', '.' or '+'
   */
  protected final String buildSubscriptionName(String subscriptionName) {
    return projectPath + "/subscriptions/" + subscriptionName;
  }
}
