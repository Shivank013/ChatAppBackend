const { Server } = require("socket.io");
const Redis = require("ioredis");
const prismaClient = require("./prisma");
const { produceMessage } = require("./kafka");

const pub = new Redis({
  host: "redis-3e3dde48-sharmashivank905-chat-app.a.aivencloud.com",
  port: 10947,
  username: "default",
  password: "AVNS__brlTnNydlYJxIDk_dW",
});

const sub = new Redis({
  host: "redis-3e3dde48-sharmashivank905-chat-app.a.aivencloud.com",
  port: 10947,
  username: "default",
  password: "AVNS__brlTnNydlYJxIDk_dW",
});

class SocketService {
  constructor() {
    console.log("Init Socket Service...");
    this._io = new Server({
      cors: {
        allowedHeaders: ["*"],
        origin: "*",
      },
    });
    this.liveUsers = 0; // Initialize live user count
    sub.subscribe("MESSAGES");
  }

  initListeners() {
    const io = this.io;
    console.log("Init Socket Listeners...");

    io.on("connect", (socket) => {
      console.log(`New Socket Connected`, socket.id);
      this.liveUsers++; // Increment live user count
      io.emit("liveUsers", this.liveUsers); // Send updated count to all clients

      socket.on("event:message", async ({ message, from }) => {
        const messageObject = {
          message: message,
          from: from
        };
        // publish this message to redis
        await pub.publish("MESSAGES", JSON.stringify(messageObject));
      });

      socket.on("disconnect", () => {
        console.log(`Socket Disconnected`, socket.id);
        this.liveUsers--; // Decrement live user count
        io.emit("liveUsers", this.liveUsers); // Send updated count to all clients
      });
    });

    sub.on("message", async (channel, message) => {
      if (channel === "MESSAGES") {
        const parsedMessage = JSON.parse(message);
        console.log("Received message from redis :", parsedMessage.message);
        console.log("From user :", parsedMessage.from);

        io.emit("event:message", { message: parsedMessage.message, from: parsedMessage.from });

        await produceMessage(message);
        console.log("Message Produced to Kafka Broker");
      }
    });
  }

  get io() {
    return this._io;
  }
}

module.exports = SocketService;
