package com.arakitski.google.pubsub;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.common.collect.ImmutableList;
import java.io.IOException;

public class QuickstartSample {

  public static void main(String... args) throws Exception {
    Pubsub pubsub = createClient();
    PublishServiceImpl publishService = new PublishServiceImpl(pubsub);
    publishService.addTopic("topic");

    publishService.addSubscription("topic", "sub1");
    publishService.publish("topic", ImmutableList.of("test1", "test2", "test3", "test335"));
    publishService.addSubscription("topic", "sub2");
    publishService.publish("topic", ImmutableList.of("test3", "test4"));
    publishService.readMessageFromSubscription("sub1");
    publishService.readMessageFromSubscription("sub2");
    publishService.publish("topic", ImmutableList.of("test6", "test7"));
    publishService.readMessageFromSubscription("sub1");
    publishService.readMessageFromSubscription("sub2");

    publishService.getSubscriptionList();
    publishService.getTopicList();
    publishService.deleteTopic("topic");
    publishService.deleteSubscription("sub1");
    publishService.deleteSubscription("sub2");
    assert (!publishService.getSubscriptionList().contains("sub1"));
    assert (!publishService.getSubscriptionList().contains("sub2"));
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