package com.arakitski.google.pubsub;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.common.collect.ImmutableList;
import java.io.IOException;

public class QuickstartSample {

  private static final String PROJECT_ID = "test-clould-java-1111";
  
  public static void main(String... args) throws Exception {
    Pubsub pubsub = createClient();
    PublishServiceImpl publishService = new PublishServiceImpl(pubsub, PROJECT_ID);
    SubscriptionService supService = new SubscriptionServiceImpl(pubsub, PROJECT_ID);
    publishService.addTopic("topic");

    supService.addSubscription("topic", "sub1");
    publishService.publish("topic", ImmutableList.of("test1", "test2", "test3", "test335"));
    supService.addSubscription("topic", "sub2");
    publishService.publish("topic", ImmutableList.of("test3", "test4"));
    supService.readMessageList("sub1");
    supService.readMessageList("sub2");
    publishService.publish("topic", ImmutableList.of("test6", "test7"));
    supService.readMessageList("sub1");
    supService.readMessageList("sub2");

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