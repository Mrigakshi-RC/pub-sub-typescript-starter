import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection successful...");

  const channel = await conn.createConfirmChannel()
  publishJSON(channel, ExchangePerilDirect, PauseKey, { isPaused: true })

  process.on('exit', (code) => {
    console.log('Process exit event with code: ', code);
    conn.close();
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
