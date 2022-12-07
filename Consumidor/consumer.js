const { Kafka } = require('kafkajs');
const util = require("util");
const log4js = require('log4js');
const logger = log4js.getLogger("Consumer");
logger.level = "debug";


fnconsumer()

async function fnconsumer() {
    
    // Configure Kafka Client
    const kafka = new Kafka({
        clientId: 'my-app',
        brokers: ['cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092'],
        ssl: true,
        sasl: {
            mechanism: 'plain',
            username: 'tenancy/federação(se houver)/username/ocid do streaming pool',
            password: '<seu auth token>'
        },
    });

    // Create a consumer group
    const consumer = kafka.consumer({
        groupId: 'obpevent-consumer-group'
    });


    await consumer.connect()
    logger.info("Successfully connected to Stream");

    // Subscribe to topic => topic is eventName
    await consumer.subscribe({ topic: 'embra-topic', fromBeginning: true });

    // Start consuming messages from stream
    await consumer.run({
        eachMessage: async ({
            topic,
            partition,
            message
        }) => {
            logger.info(message.value.toString())
        },
    });
}




