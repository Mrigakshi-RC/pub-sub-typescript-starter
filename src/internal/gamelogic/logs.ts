import { appendFile } from "fs/promises";

export interface GameLog {
  currentTime: Date;
  message: string;
  username: string;
}

const logsFile = "game.log";
const writeToDiskSleep = 1000;

function block(ms: number) {
  const end = Date.now() + ms;
  while (Date.now() < end) { }
}

export async function writeLog(gameLog: GameLog): Promise<void> {
  console.log("received game log...");
  block(writeToDiskSleep);

  let date = new Date(gameLog.currentTime);

  if (isNaN(date.getTime())) {
    console.error(`Received invalid date value: ${gameLog.currentTime}`);
    date = new Date();
  }

  const timestamp = date.toISOString();
  const logEntry = `${timestamp} ${gameLog.username}: ${gameLog.message}\n`;

  try {
    await appendFile(logsFile, logEntry, { flag: "a" });
  } catch (err) {
    throw new Error(`could not write to logs file: ${err}`);
  }
}
