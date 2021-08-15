const {Kafka, logLevel} =require('kafkajs');

const kafka = new Kafka({
    clientId: 'admin',
    brokers:['localhost:9092'],
    logLevel:logLevel.INFO,
    retry:{
        retries:3
    }
});

const consumer = kafka.consumer({groupId:'nodeKafka1'});

const run = async () =>{
    await consumer.connect();
    await consumer.subscribe({
        topic: process.env.TOPIC
    })
    await consumer.run({
        eachMessage:async ({ topic, partition, message }) => {
            console.log({
                value: message.value.toString(),
            })
        }
    })
}

run().catch(error=>{
    console.log(error);
    process.exit();
})