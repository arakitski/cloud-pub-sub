package com.arakitski.google.pub.sub;// Imports the Google Cloud client library

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.Pubsub.Projects.Topics.Delete;
import com.google.api.services.pubsub.Pubsub.Projects.Topics.Get;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.Topic;
import com.google.common.collect.ImmutableList;
import java.io.IOException;

/**
 * Based on https://github.com/GoogleCloudPlatform/cloud-pubsub-samples-java.git
 */
public class QuickstartSample {

  private static final String PROJECT_ID =
      "test-clould-java-1111";

  public static void main(String... args) throws Exception {
    Pubsub client = createClient();
  }

  private static void deleteTopic(Pubsub client) throws IOException {
    Delete delete = client.projects().topics().delete(buildTopicName("topic2"));
    System.out.println("Removed topic " + delete);
  }

  /**
   * @param topicName must start with a letter, and contain only letters, numbers, '%', '~', '_',
   * '.' or '+'
   */
  private static String buildTopicName(String topicName) {
    return "projects/" + PROJECT_ID + "/topics/" + topicName;
  }

  private static void createTopic(Pubsub client, String topicName) throws IOException {
    client.projects().topics()
        .create(buildTopicName(topicName), new Topic())
        .execute();
  }

  private static void sendMessage(Pubsub client, String topicName) throws IOException {
    PubsubMessage pubsubMessage1 = new PubsubMessage();
    pubsubMessage1.encodeData("testMessage1".getBytes("UTF-8"));
    PubsubMessage pubsubMessage2 = new PubsubMessage();
    pubsubMessage2.encodeData("testMessage2".getBytes("UTF-8"));
    PublishRequest publishRequest = new PublishRequest();
    publishRequest.setMessages(ImmutableList.of(pubsubMessage1, pubsubMessage2));
    PublishResponse response = client.projects().topics()
        .publish(buildTopicName(topicName), publishRequest).execute();
    System.out.println("messsage sended:" + response);
  }

  private static Pubsub createClient() throws IOException {
    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(PubsubScopes.all());
    }
    HttpRequestInitializer initializer =
        new RetryHttpInitializerWrapper(credential);
    return new Pubsub.Builder(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(),
        initializer)
        .build();
  }
}