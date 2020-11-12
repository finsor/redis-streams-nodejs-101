const redis = require("redis");

class RedisWriter {
  constructor(host, port, stream) {
    this.redisClient = redis.createClient({ port: port, host: host });
    this.stream = stream;
  }

  write(obj) {
    const message = this.objectToMessage(obj);

    this.redisClient.xadd(this.stream, "*", ...message, function (err) {
      if (err) {
        console.log(err);
      }
    });
  }

  objectToMessage(obj) {
    const message = [];

    for (var key in obj) {
      message.push(key);
      message.push(obj[key]);
    }

    return message;
  }
}

class RedisReader {
  constructor(host, port, group, stream, consumer, processMessage) {
    this.redisClient = redis.createClient({ port: port, host: host });
    this.group = group;
    this.stream = stream;
    this.consumer = consumer;
    this.processMessage = processMessage;
    this.stopRequested = false;

    this.createGroupForStream();
  }

  consume() {
    this.stopRequested = false;
    this.processMessages("0"); // pending: messages that were read before but not ack-ed
    this.processMessages(">"); // unread messages
  }

  stop() {
    this.stopRequested = true;
  }

  processMessages(id) {
    this.redisClient.xreadgroup(
      "GROUP",
      this.group,
      this.consumer,
      "BLOCK",
      500, // 0 to block until at least 1 message arrives
      "STREAMS",
      this.stream,
      id,
      (err, stream) => {
        console.log("in callback");

        if (err) {
          console.log(err);
        }

        if (stream) {
          // Assuming single stream, otherwise it's just a for loop.
          const messages = stream[0][1];

          messages.forEach((message) => {
            const messageObject = this.messageToObject(message);

            this.processMessage(messageObject);

            this.redisClient.xack(this.stream, this.group, messageObject.id);
          });
        }

        // Read more new messages.
        if (id === ">" && !this.stopRequested) {
          this.processMessages(">");
        }
      }
    );
  }

  createGroupForStream() {
    console.log(
      `Attempting to create group "${this.group}" to consume stream "${this.stream}"`
    );
    this.redisClient.xgroup(
      "CREATE",
      this.stream,
      this.group,
      "0",
      "MKSTREAM",
      (err) => {
        if (err) {
          if (err.code == "BUSYGROUP") {
            console.log(`Group "${this.group}" already exists`);
          } else {
            console.log(err);
            process.exit();
          }
        } else {
          console.log(`Created group ${this.group} for stream ${this.stream}`);
        }
      }
    );
  }

  messageToObject(message) {
    const id = message[0];
    const values = message[1];
    let messageObject = { id: id };

    for (let i = 0; i < values.length; i = i + 2) {
      let key = values[i];
      let val = values[i + 1];

      messageObject[key] = val;
    }

    return messageObject;
  }
}

const host = "localhost";
const port = 6379;
const streamName = "myStream";
writer = new RedisWriter(host, port, streamName);
reader = new RedisReader(
  host,
  port,
  "mygroup",
  streamName,
  "consumer-1",
  (message) => {
    console.log("my message:", message);
  }
);

reader.consume();

// Send messages, process them and then stop reading.
const messageCount = 3;

for (let index = 1; index <= messageCount; index++) {
  setTimeout(
    () => writer.write({ key1: "value1", key2: "value2" }),
    index * 1000
  );
}

setTimeout(() => reader.stop(), (messageCount + 1) * 1000);
