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
      username: ['tenancy/federação(se houver)/username/ocid do streaming pool'],
      password: ['authtoken']
    },
  })

  const producer = kafka.producer({
 allowAutoTopicCreation: true,
 createPartitioner: Partitioners.DefaultPartitioner })
 
  await producer.connect()

  try{
    await producer.send({
      topic: 'nome do topico do streaming',
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
