const fdk=require('@fnproject/fdk');
const { Kafka } = require('kafkajs')

fdk.handle(async function(input){
  logger.info('HttpRequestQueue Request', input);

  const kafka = new Kafka({
    clientId: 'my-app',
    brokers: [process.env['STREAMING_BOOTSTRAP']],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: process.env['KAFKA_USR'],
      password: process.env['KAFKA_PASSWD']
    },
  })

  const producer = kafka.producer({
    allowAutoTopicCreation: true,
  })

  await kafka.connect();

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
    logger.info('Kafka error:', error);
    return error
  }
})
