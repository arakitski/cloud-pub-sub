package com.arakitski.google.pubsub;

import com.google.common.collect.ImmutableList;
import java.io.IOException;

/**
 * Interface for the publish message to the google pubsub cloud.
 */
public interface PublishService {

  boolean isTopicExist(String topicName) throws IOException;

  void publish(String topicName, ImmutableList<String> messageList) throws IOException;

  void addTopic(String topicName) throws IOException;

  void deleteTopic(String topicName) throws IOException;

  ImmutableList<String> getTopicList() throws IOException;
}
