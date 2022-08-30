const fdk=require('@fnproject/fdk');
const { Kafka } = require('kafkajs');
const { Partitioners } = require('kafkajs')


fdk.handle(async function(input){
  console.log('HttpRequestQueue Request', input);

  const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092'],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: ['brztechcloud01/oracleidentitycloudservice/ANDRESSA.DOS.SIQUEIRA@ORACLE.COM/ocid1.streampool.oc1.sa-saopaulo-1.amaaaaaatsbrckqavzplcfc3337wdz3rvoewswjoto6zyzyr63dz3eav226q'],
      password: ['<B5#A]vLrc(jRVVg<#Op']
    },
  })

  const producer = kafka.producer({
 allowAutoTopicCreation: true,
 createPartitioner: Partitioners.DefaultPartitioner })
 
  await producer.connect()

  try{
    await producer.send({
      topic: 'embra-topic',
      messages: [
        { value: input },
      ],
    })
    return input
  }
  catch(error){
    console.log('Kafka error:', error);
    return error
  }
})
