package com.arakitski.google.pub.sub;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.ListSubscriptionsResponse;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * Implementation for the {@link PublishService}
 */
public class PublishServiceImpl implements PublishService {

  private static final Logger LOG =  Logger.getLogger(PublishServiceImpl.class.getName());

  private static final String PROJECT_ID = "test-clould-java-1111";
  private static final String PROJECT = "projects/" + PROJECT_ID;
  private final Pubsub client;

  public PublishServiceImpl(Pubsub client) {
    this.client = client;
  }

  @Override
  public void publish(String topicName, ImmutableList<String> messageList) throws IOException {
    Builder<PubsubMessage> pubsubMessageList = ImmutableList.builder();
    for (String message : messageList) {
      PubsubMessage pubsubMessage = new PubsubMessage();
      pubsubMessage.encodeData(message.getBytes("UTF-8"));
      pubsubMessageList.add(pubsubMessage);
    }
    PublishRequest publishRequest = new PublishRequest();
    publishRequest.setMessages(pubsubMessageList.build());
    
    PublishResponse response = client.projects().topics()
        .publish(buildTopicName(topicName), publishRequest).execute();
    LOG.info("Messsage sended:" + response);
  }

  @Override
  public void deleteTopic(String topicName) throws IOException {
    client.projects().topics().delete(buildTopicName(topicName)).execute();
    LOG.info("Removed topic " + topicName);
  }

  @Override
  public void addTopic(String topicName) throws IOException {
    client.projects().topics()
        .create(buildTopicName(topicName), new Topic())
        .execute();
  }

  @Override
  public void addSubscription(String topicName, String subscriptionName) throws IOException {
    Subscription subscription = 
        new Subscription().setTopic(buildTopicName(topicName));
    Subscription executeResult = client.projects().subscriptions()
        .create(buildSubscriptionName(subscriptionName), subscription)
        .execute();
    LOG.info("subscription created = " + executeResult);
  }

  @Override
  public void deleteSubscription(String subscriptionName) throws IOException {
    client.projects().subscriptions()
        .delete(buildSubscriptionName(subscriptionName))
        .execute();
    LOG.info("subscription " + subscriptionName + " removed.");
  }

  @Override
  public ImmutableList<String> getSubscriptionList() throws IOException {
    ListSubscriptionsResponse response = client.projects().subscriptions().list(PROJECT)
        .execute();
    List<Subscription> subscriptions = response.getSubscriptions();
    if (subscriptions != null) {
      Builder<String> subListBuilder = ImmutableList.builder();
      for (Subscription subscription : subscriptions) {
        subListBuilder.add(subscription.getName());
      }
      LOG.info("List of subscription = " + subListBuilder.build());
      return subListBuilder.build();
    }
    LOG.info("List of subscription is empty ");
    return ImmutableList.of();
  }


  @Override
  public ImmutableList<String> getTopicList() throws IOException {
    ListTopicsResponse response = client.projects().topics().list(PROJECT).execute();
    List<Topic> topics = response.getTopics();
    if (topics != null) {
      Builder<String> topicListBuilder = ImmutableList.builder();
      for (Topic subscription : topics) {
        topicListBuilder.add(subscription.getName());
      }
      LOG.info("List of subscription = " + topicListBuilder.build());
      return topicListBuilder.build();
    }
    LOG.info("List of subscription is empty ");
    return ImmutableList.of();
  }


  @Override
  public ImmutableList<String> readMessageFromSubscription(String subscriptionName) throws IOException {
    PullRequest pullRequest = new PullRequest()
        .setReturnImmediately(true)
        .setMaxMessages(1000);
    PullResponse pullResponse = client.projects().subscriptions()
        .pull(buildSubscriptionName(subscriptionName), pullRequest)
        .execute();
    if (pullResponse != null) {
      List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessages();
      if (receivedMessages != null) {
        ImmutableList.Builder<String> responseBuilder = ImmutableList.builder();
        for (ReceivedMessage receivedMessage : receivedMessages) {
          responseBuilder.add(new String(receivedMessage.getMessage().decodeData(), "UTF-8"));
        }
        LOG.info("Message received for " + subscriptionName + ":: " + responseBuilder.build());
        return responseBuilder.build();
      }
    }
    LOG.info("Message not found for sub=" + subscriptionName);
    return ImmutableList.of();
  }

  /**
   * @param topicName must start with a letter, and contain only letters, numbers, '%', '~', '_',
   * '.' or '+'
   */
  private static String buildTopicName(String topicName) {
    return PROJECT + "/topics/" + topicName;
  }

  /**
   * @param subscriptionName must start with a letter, and contain only letters, numbers, '%', '~',
   * '_', '.' or '+'
   */
  private static String buildSubscriptionName(String subscriptionName) {
    return PROJECT + "/subscriptions/" + subscriptionName;
  }
}
