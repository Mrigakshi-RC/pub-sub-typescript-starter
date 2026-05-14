import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";

async function main() {
  console.log("Starting Peril server...");
  printServerHelp();
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection successful...");

  const publishCh = await conn.createConfirmChannel()

  process.on('exit', (code) => {
    console.log('Process exit event with code: ', code);
    conn.close();
  });

  await declareAndBind(conn, ExchangePerilTopic, GameLogSlug, "game_logs.*", SimpleQueueType.Durable)

  while (true) {
    const inputArr = await getInput();
    if (inputArr.length === 0) continue;
    switch (inputArr[0]) {
      case "pause":
        console.log("Sending a pause message");
        publishJSON(publishCh, ExchangePerilDirect, PauseKey, { isPaused: true });
        break;
      case "resume":
        console.log("Sending a pause message");
        publishJSON(publishCh, ExchangePerilDirect, PauseKey, { isPaused: false });
        break;
      case "quit":
        console.log("Exiting:");
        process.exit(1);
      default:
        console.log("I don't understand your command")
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
