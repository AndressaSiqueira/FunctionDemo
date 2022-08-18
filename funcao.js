const fdk = require('@fnproject/fdk');
const common = require("oci-common")
const streaming = require("oci-streaming")
const os = require("oci-objectstorage");
const fs = require("fs");

fdk.handle(async function (input) {
 try {
const provider = new common.ConfigFileAuthenticationDetailsProvider();
const client = new os.ObjectStorageClient({
  authenticationDetailsProvider: provider
});

 const putObjectRequest = {
      namespaceName: "id3kyspkytmr",
      bucketName: "bucket",
      putObjectBody: input,
      objectName: object,
      contentLength: stats.size
    };
    
    const putObjectResponse = await client.putObject(putObjectRequest);
    console.log("Put Object executed successfully" + putObjectResponse);

    console.log("Fetch the object created");
    const getObjectRequest = {
      objectName: object,
      bucketName: "bucket",
      namespaceName: "id3kyspkytmr"
    };
    const getObjectResponse = await client.getObject(getObjectRequest);
    console.log("Get Object executed successfully.");
