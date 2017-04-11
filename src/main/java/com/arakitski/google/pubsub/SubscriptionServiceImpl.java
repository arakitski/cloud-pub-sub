package com.arakitski.google.pubsub;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.*;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * Implementation for {@link SubscriptionService}
 */
public class SubscriptionServiceImpl extends AbstractPubSubService implements SubscriptionService {

    private static final Logger LOG =  Logger.getLogger(PublishServiceImpl.class.getName());

    public SubscriptionServiceImpl(Pubsub client, String projectId) {
        super(client, projectId);
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
        ListSubscriptionsResponse response = client.projects().subscriptions().list(projectPath)
                .execute();
        List<Subscription> subscriptions = response.getSubscriptions();
        if (subscriptions != null) {
            ImmutableList.Builder<String> subListBuilder = ImmutableList.builder();
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
    public ImmutableList<String> readMessageList(String subscriptionName) throws IOException {
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
}
