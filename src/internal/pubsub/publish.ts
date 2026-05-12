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