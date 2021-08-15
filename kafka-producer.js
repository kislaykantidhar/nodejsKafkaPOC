const {Kafka, logLevel} =require('kafkajs');

const kafka = new Kafka({
    clientId: 'admin',
    brokers:['localhost:9092'],
    logLevel:logLevel.INFO,
    retry:{
        retries:3
    }
});
const producer = kafka.producer();
const sendMessages=async (message)=>{
    const num= `${parseInt(Math.random()*10)}`;
    await producer.connect();
    await producer.send({
        topic: process.env.TOPIC,
        messages:[
            {value:num}
        ]
    })
    await producer.disconnect();
}

sendMessages();