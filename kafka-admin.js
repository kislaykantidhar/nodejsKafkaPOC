const {Kafka, logLevel} =require('kafkajs');
const util = require('util');

const kafka = new Kafka({
    clientId: 'admin',
    brokers:['localhost:9092'],
    logLevel:logLevel.INFO,
    retry:{
        retries:3
    }
});

const admin = kafka.admin();

const listTopics = async ()=>{
    await admin.connect();
    const topicList = await admin.listTopics();
    await admin.disconnect();
    console.log(topicList);
}

const createTopics = async ()=>{
    admin.connect();
    const succcess = await admin.createTopics({
        topics: [{
            topic:process.env.TOPIC,
            replicationFactor:1,
            numPartitions:5
        }],
        waitForLeaders:true
    })  
    console.log(succcess);
    admin.disconnect();
}

const describeTopics = async () =>{
    admin.connect();
    const description = await admin.fetchTopicMetadata({
        topics:[process.env.TOPIC]
    })
    console.log(util.inspect(description, false, null, true))
    admin.disconnect();
}

const deleteTopics = async ()=>{
    try{
        admin.connect();
        await admin.deleteTopics({
            topics: [process.env.TOPIC],
            timeout:1000
        })
        admin.disconnect();
    }
    catch(error)
    {
        console.log(error);
        admin.disconnect();
    }
}
switch(process.env.FUNC)
{
    case "listTopics":
        listTopics();
        break;

    case "createTopics":
        createTopics();
        break;

    case "describeTopics":
        describeTopics();
        break;

    case "deleteTopics":
        deleteTopics();
        break;

    default:
        console.log("enter function name");
        break;
}