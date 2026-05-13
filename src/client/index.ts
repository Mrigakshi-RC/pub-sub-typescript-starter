import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/declareandbind.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection successful...");

  process.on('exit', (code) => {
    console.log('Process exit event with code: ', code);
    conn.close();
  });

  const username = await clientWelcome();
  const [channel, queue] = await declareAndBind(conn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient)

  const gameState = new GameState(username)
  while (true) {
    const inputArr = await getInput();
    if (inputArr.length === 0) continue;
    try {
      switch (inputArr[0]) {
        case "spawn":
          commandSpawn(gameState, inputArr);
          break;
        case "move":
          commandMove(gameState, inputArr);
          console.log("Move command processed.");
          break;
        case "status":
          commandStatus(gameState);
          break;
        case "help":
          printClientHelp();
          break;
        case "spam":
          console.log("Spamming not allowed yet!")
          break;
        case "quit":
          printQuit()
          process.exit(1);
        default:
          console.log("I don't understand your command")
      }
    }
    catch (err) {
      console.log("Error processing your command:", err instanceof Error ? err.message : err);
    }

  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
