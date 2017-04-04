package com.arakitski.google.pub.sub;

import com.google.api.gax.core.ApiFuture;
import com.google.cloud.pubsub.spi.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Implementation for the {@link PublishService}
 */
public class PublishServiceImpl implements PublishService {

  private Publisher publisher;

  public PublishServiceImpl() {
    // Your Google Cloud Platform project ID
    String projectId = "test-clould-java-1111";

    // Your topic ID
    String topicId = "my-new-topic";

    // Create a new topic
//    TopicName topic = TopicName.create(projectId, topicId);
    try {
      publisher = Publisher.newBuilder(TopicName.create(projectId, topicId)).build();
    } catch (IOException e) {
      //todo
      e.printStackTrace();
    }

  }

  public void publish(String message) {
    ByteString data = ByteString.copyFromUtf8(message);
    PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
    ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
    try {
      System.out.println("published with message id: " + messageIdFuture.get());
    } catch (InterruptedException e) {
      //todo
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

//  public ImmutableList<Topic> getTopicList() {
//    ListTopicsRequest listTopicsRequest =
//        ListTopicsRequest.newBuilder()
//            .setProjectWithProjectName(ProjectName.create(projectId))
//            .build();
//    ListTopicsPagedResponse response = publisherClient.listTopics(listTopicsRequest);
//    return ImmutableList.copyOf(response.iterateAllElements());
//  }
//
//  public void deleteTopic(String topicId) {
//    TopicName topicName = TopicName.create(projectId, topicId);
//    publisherClient.deleteTopic(topicName);
//  }
}
