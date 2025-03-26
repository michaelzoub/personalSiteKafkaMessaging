import { consumerFunc } from "./utils/confluentkafka";

console.log("Hello via Bun!");

//run:
consumerFunc();

Bun.serve({
    port: 3000,
    fetch(req) {
        const url = new URL(req.url);
        //api ednpoint:
        if (url.pathname == "/kafkaconsumer") {

        }

        return new Response("");
    }
})