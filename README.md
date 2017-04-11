# cloud-pub-sub
The application launches 1 thread for publishing messages to Google Pub / Sub and 2 threads for reading.

To run this application, you should:
  - Create pub sub application https://console.cloud.google.com/cloudpubsub
  - Enable api for the new project https://console.cloud.google.com/apis/dashboard
  - Change the value of Main#PROJECT_ID to your project id
  - Set the value of the system variable GOOGLE_APPLICATION_CREDENTIALS, see https://developers.google.com/identity/protocols/application-default-credentials
