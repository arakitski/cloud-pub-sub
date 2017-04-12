package com.arakitski.google.pubsub;

import com.arakitski.google.pubsub.api.PublishApiServiceImpl;
import com.arakitski.google.pubsub.api.SubscriptionApiService;
import com.arakitski.google.pubsub.api.SubscriptionApiServiceImpl;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.gax.core.ApiFuture;
import com.google.api.gax.core.ApiFutureCallback;
import com.google.api.gax.core.ApiFutures;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.cloud.pubsub.spi.v1.Publisher;
import com.google.cloud.pubsub.spi.v1.Subscriber;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QuickstartSample {

  private static final Logger LOG = Logger.getLogger(SubscriptionApiServiceImpl.class.getName());

  private static final String PROJECT_ID = "test-clould-java-1111";

  public static void main(String... args) throws Exception {
    apiClientTest();
    cloudLibTest();
  }

  private static void cloudLibTest() throws IOException, InterruptedException {
    Subscriber.newBuilder(SubscriptionName.create(PROJECT_ID, "testSub"),
        (pubsubMessage, ackReplyConsumer) -> {
          LOG.info("message from cloud lib = " + pubsubMessage.getData().toString());
        }).build();
    Publisher publisher = Publisher.newBuilder(TopicName.create(PROJECT_ID, "testTopic")).build();
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    executorService.scheduleAtFixedRate(() -> {
      String message = "message-CLOUD-" + UUID.randomUUID();
      ApiFuture<String> messageIdFuture = publisher.publish(
          PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(message)).build());
      ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback<String>() {
        public void onSuccess(String messageId) {
          LOG.info("published with message id: " + messageId);
        }

        public void onFailure(Throwable t) {
          LOG.warning("failed to publish: " + t);
        }
      });
    }, 1, 2, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(15);
  }

  private static void apiClientTest() throws IOException, InterruptedException {
    Pubsub pubsub = createClient();
    // create topic and add 2 subscription for this
    PublishApiServiceImpl publishService = new PublishApiServiceImpl(pubsub, PROJECT_ID);
    SubscriptionApiService supService = new SubscriptionApiServiceImpl(pubsub, PROJECT_ID);
    if (!publishService.isTopicExist("topic")) {
      publishService.addTopic("topic");
    }
    if (!supService.isSubscriptionExist("sub1")) {
      supService.addSubscription("topic", "sub1");

    }
    if (!supService.isSubscriptionExist("sub2")) {
      supService.addSubscription("topic", "sub2");
    }

    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
    // add api publisher and subscribers
    executorService.scheduleAtFixedRate(() -> {
      try {
        publishService.publish("topic",
            ImmutableList.of("message1-" + UUID.randomUUID(), "message2-" + UUID.randomUUID()));
      } catch (IOException e) {
        LOG.log(Level.WARNING, e.getMessage());
      }
    }, 1, 2, TimeUnit.SECONDS);
    executorService.scheduleAtFixedRate(() -> {
      try {
        supService.readMessageList("sub1");
      } catch (IOException e) {
        LOG.log(Level.WARNING, e.getMessage());
      }
    }, 2, 1, TimeUnit.SECONDS);
    executorService.scheduleAtFixedRate(() -> {
      try {
        supService.readMessageList("sub2");
      } catch (IOException e) {
        LOG.log(Level.WARNING, e.getMessage());
      }
    }, 1, 5, TimeUnit.SECONDS);
    executorService.awaitTermination(15, TimeUnit.SECONDS);
    executorService.shutdown();

    supService.getSubscriptionList();
    publishService.getTopicList();
    publishService.deleteTopic("topic");
    supService.deleteSubscription("sub1");
    supService.deleteSubscription("sub2");
    assert (!supService.getSubscriptionList().contains("sub1"));
    assert (!supService.getSubscriptionList().contains("sub2"));
    assert (!publishService.getTopicList().contains("topic"));
  }

  private static Pubsub createClient() throws IOException {
    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(PubsubScopes.all());
    }
    GoogleCredential finalCredential = credential;
    return new Pubsub.Builder(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(),
        httpRequest -> {
          // set 2 minutes timeout
          httpRequest.setReadTimeout(2 * 60000);
          httpRequest.setInterceptor(finalCredential);
        })
        .build();
  }
}