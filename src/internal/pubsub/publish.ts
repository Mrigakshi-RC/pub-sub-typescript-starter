import { encode } from "@msgpack/msgpack";
import type { ConfirmChannel } from "amqplib";

export async function publishJSON<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {
    const jsonString = JSON.stringify(value);
    const content = Buffer.from(jsonString);
    ch.publish(exchange,
        routingKey,
        content,
        { contentType: "application/json" },)
}

export async function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void>{
    const encoded: Uint8Array = encode(value);
    const msgPackBuffer = Buffer.from(encoded.buffer, encoded.byteOffset, encoded.byteLength);
    ch.publish(exchange,
        routingKey,
        msgPackBuffer,
        { contentType: "application/x-msgpack" },)
}