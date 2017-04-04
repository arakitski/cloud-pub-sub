package com.arakitski.google.pub.sub;// Imports the Google Cloud client library

public class QuickstartSample {

  public static void main(String... args) throws Exception {
    PublishServiceImpl publishService = new PublishServiceImpl();
    publishService.publish("Message");
  }
}