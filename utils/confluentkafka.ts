import { Kafka } from "kafkajs";
import { connectToDatabase } from "./connectToDatabase";
import type { clickData } from "../types/clickData";

const kafka = new Kafka({ brokers: ["pkc-619z3.us-east1.gcp.confluent.cloud:9092"] })
const consumer = kafka.consumer({ groupId: "nodejs-group-1" });

export async function consumerFunc() {
    await consumer.connect();
    await consumer.subscribe({ topic: "topic_0", fromBeginning: true });

    const { db } = await connectToDatabase();
    const collection = await db.collection("clicks");

    const BATCH_SIZE = 5;
    let batchArray: clickData[] = [];

    await consumer.run({
        eachMessage: async ({ message }) => {
            //batch of messages
            console.log(message);
            const clickData: clickData = JSON.parse(message.value?.toString() || "");
            console.log(clickData);
            //store in mongodb
            //store num of clicks depending on href
            
            //object: { href: "", clicks: "" }
            batchArray.push(clickData);

            if (batchArray.length >= BATCH_SIZE) {
                console.log("batchSize big");
                batchArray = [];
                await processBatch(batchArray, collection);
            }
        }
    });

    process.on("SIGINT", async () => {
        await consumer.disconnect();
        process.exit(0);
    }) 
}

async function processBatch(batch: clickData[], collection: any) {
            //store in mongodb
            //store num of clicks depending on href
            //object: { href: "", clicks: "" }
    const bulk = batch.map((data) => ({
        updateOne: {
            filter: { href: data.href},
            update: { $inc: { quantity: data.quantity } },
            upsert: true 
        },
    }))

    //bulk store:
    const result = await collection.bulkWrite(bulk);
    console.log(result);
}
