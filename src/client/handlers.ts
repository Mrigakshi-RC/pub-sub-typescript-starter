import type { ConfirmChannel } from "amqplib";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { publishGameLog } from "./index.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState): AckType => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(gs: GameState, ch: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove): Promise<AckType> => {
    const outcome = handleMove(gs, move);
    switch (outcome) {
      case MoveOutcome.Safe:
        console.log(`Moved ${move.units.length} units to ${move.toLocation}`);
        process.stdout.write("> ");
        return AckType.Ack;
      case MoveOutcome.MakeWar:
        try {
          await publishJSON(ch, ExchangePerilTopic, `${WarRecognitionsPrefix}.${gs.getUsername()}`, {
            attacker: move.player,
            defender: gs.getPlayerSnap(),
          })
          console.log(`Moved ${move.units.length} units to ${move.toLocation}`);
          process.stdout.write("> ");
          return AckType.Ack;
        }
        catch (err) {
          console.error("Failed to publish war recognition:", err);
          return AckType.NackRequeue;
        }

      case MoveOutcome.SamePlayer:
      default:
        console.log(`Move discarded: ${outcome}`);
        process.stdout.write("> ");
        return AckType.NackDiscard; // Invalid/Other -> Discard (No Requeue!)
    }
  };
}

export function handlerWar(
  gs: GameState, ch: ConfirmChannel
): (war: RecognitionOfWar) => Promise<AckType> {
  return async (war: RecognitionOfWar): Promise<AckType> => {
    try {
      const outcome = handleWar(gs, war);
      switch (outcome.result) {
        case WarOutcome.NotInvolved:
          return AckType.NackRequeue;
        case WarOutcome.NoUnits:
          return AckType.NackDiscard;
        case WarOutcome.OpponentWon:
        case WarOutcome.YouWon:
          try {
            publishGameLog(gs, ch, `${outcome.winner} won a war against ${outcome.loser}`)
            return AckType.Ack;
          }
          catch (err) {
            return AckType.NackRequeue;
          }
        case WarOutcome.Draw:
          try {
            publishGameLog(gs, ch, `A war between ${outcome.attacker} and ${outcome.defender} resulted in a draw`)
            return AckType.Ack;
          }
          catch (err) {
            return AckType.NackRequeue;
          }
        default:
          console.error("Unknown war outcome");
          return AckType.NackDiscard;
      }
    }
    catch (err) {
      console.error("Error handling war recognition:", err);
      return AckType.NackDiscard;
    } finally {
      process.stdout.write("> ");
    }
  };
}