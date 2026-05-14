import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState): AckType => {
    handlePause(gs,ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(gs: GameState): (move: ArmyMove) => AckType {
  return (move: ArmyMove): AckType => {
    const outcome=handleMove(gs, move);
    switch (outcome) {
      case MoveOutcome.Safe:
      case MoveOutcome.MakeWar:
        console.log(`Moved ${move.units.length} units to ${move.toLocation}`);
        process.stdout.write("> ");
        return AckType.Ack; // Valid move -> Ack

      case MoveOutcome.SamePlayer:
      default:
        console.log(`Move discarded: ${outcome}`);
        process.stdout.write("> ");
        return AckType.NackDiscard; // Invalid/Other -> Discard (No Requeue!)
    }
  };
}