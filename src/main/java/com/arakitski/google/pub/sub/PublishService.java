package com.arakitski.google.pub.sub;

/**
 * Interface for the publish message to the google pubsub cloud.
 */
public interface PublishService { 
  void publish(String message);
//  ImmutableList<Topic> getTopicList();
//  void deleteTopic(String topicId);
}
