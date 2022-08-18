const common = require("oci-common");
const st = require("oci-streaming"); // OCI SDK package for OSS

const ociConfigFile = "/home/ANDRESSA_D/.oci/";
const ociMessageEndpointForStream = "https://cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com";
const ociStreamOcid = "ocid1.stream.oc1.sa-saopaulo-1.amaaaaaatsbrckqabmvpnvoa2nhp664uyojoq4j75npbh3aato5rajd5epaq";

// provide authentication for OCI and OSS
const provider = new common.ConfigFileAuthenticationDetailsProvider(ociConfigFile, ociProfileName);
  
const consumerGroupName = "exampleGroup";
const consumerGroupInstanceName = "exampleInstance-1";

async function main() {
  // OSS client to produce and consume messages from a Stream in OSS
  const client = new st.StreamClient({ authenticationDetailsProvider: provider });
  client.endpoint = ociMessageEndpointForStream;

  // A cursor can be created as part of a consumer group.
  // Committed offsets are managed for the group, and partitions
  // are dynamically balanced amongst consumers in the group.
  console.log("Starting a simple message loop with a group cursor");
  const groupCursor = await getCursorByGroup(client, ociStreamOcid, consumerGroupName, consumerGroupInstanceName);
  await consumerMsgLoop(client, ociStreamOcid, groupCursor);
}

main().catch((err) => {
    console.log("Error occurred: ", err);
}); 

async function consumerMsgLoop(client, streamId, initialCursor) {
    let cursor = initialCursor;
    for (var i = 0; i < 10; i++) {
      const getRequest = {
        streamId: streamId,
        cursor: cursor,
        limit: 2
      };
      const response = await client.getMessages(getRequest);
      console.log("Read %s messages.", response.items.length);
      for (var message of response.items) {
        if (message.key !== null)  {         
            console.log("%s: %s",
            Buffer.from(message.key, "base64").toString(),
            Buffer.from(message.value, "base64").toString());
        }
       else{
            console.log("Null: %s",
                Buffer.from(message.value, "base64").toString() );
       }
      }
      // getMessages is a throttled method; clients should retrieve sufficiently large message
      // batches, as to avoid too many http requests.
      await delay(2);
      cursor = response.opcNextCursor;
    }
  }
  

async function getCursorByGroup(client, streamId, groupName, instanceName) {
    console.log("Creating a cursor for group %s, instance %s.", groupName, instanceName);
    const cursorDetails = {
      groupName: groupName,
      instanceName: instanceName,
      type: st.models.CreateGroupCursorDetails.Type.TrimHorizon,
      commitOnGet: true
    };
    const createCursorRequest = {
      createGroupCursorDetails: cursorDetails,
      streamId: streamId
    };
    const response = await client.createGroupCursor(createCursorRequest);
    return response.cursor.value;
}

async function delay(s) {
    return new Promise(resolve => setTimeout(resolve, s * 1000));
}
