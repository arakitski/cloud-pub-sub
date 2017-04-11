package com.arakitski.google.pubsub;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
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
public class PublishServiceImpl extends AbstractPubSubService implements PublishService {

  private static final Logger LOG = Logger.getLogger(PublishServiceImpl.class.getName());

  public PublishServiceImpl(Pubsub client, String projectId) {
    super(client, projectId);
  }

  @Override
  public boolean isTopicExist(String topicName) throws IOException {
    try {
      Topic topic = client.projects().topics().get(buildTopicName(topicName)).execute();
      return topic != null;
    } catch (GoogleJsonResponseException e) {
      if (e.getDetails().getCode() == 404) {
        return false;
      } else {
        throw e;
      }
    }
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
    LOG.info("Added topic " + topicName);
  }

  @Override
  public ImmutableList<String> getTopicList() throws IOException {
    ListTopicsResponse response = client.projects().topics().list(projectPath).execute();
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
}
