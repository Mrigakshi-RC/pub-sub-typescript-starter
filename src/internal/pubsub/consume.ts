import { decode } from "@msgpack/msgpack";
import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
    Durable,
    Transient,
}

export enum AckType {
    Ack,
    NackRequeue,
    NackDiscard
}

export async function declareAndBind(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
    const channel = await conn.createChannel();
    const queue = await channel.assertQueue(queueName, { durable: queueType === SimpleQueueType.Durable, autoDelete: queueType === SimpleQueueType.Transient, exclusive: queueType === SimpleQueueType.Transient, arguments: { "x-dead-letter-exchange": "peril_dlx" } });
    channel.bindQueue(queueName, exchange, key);
    return [channel, queue];
}

export function deserializeJSON<T>(content: Buffer): T {
  return JSON.parse(content.toString()) as T;
}

export function deserializeMsgPack<T>(content: Buffer): T {
  return decode(content) as T;
}

export async function subscribe<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    routingKey: string,
    simpleQueueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
    deserializer: (data: Buffer) => T,
): Promise<void> {
    const [channel, queue] = await declareAndBind(conn, exchange, queueName, routingKey, simpleQueueType);
    await channel.consume(queue.queue, async (msg: amqp.ConsumeMessage | null) => {
        if (msg) {
            let outcome;
            try {
                const data = deserializer(msg.content)
                outcome = await handler(data);
                if (outcome === AckType.Ack) {
                    channel.ack(msg);
                    console.log("Message Acknowledged");
                } else if (outcome === AckType.NackRequeue) {
                    channel.nack(msg, false, true);
                    console.log("Message Nacked and Requeued");
                } else if (outcome === AckType.NackDiscard) {
                    channel.nack(msg, false, false);
                    console.log("Message Nacked and Discarded");
                } else {
                    console.warn("Handler returned unknown AckType:", outcome);
                    channel.nack(msg, false, false);
                    console.log("Message Nacked and Discarded");
                }
            } catch (err) {
                console.error("Failed to parse message content:", err);
                channel.nack(msg, false, false);
            }
        }
    });
}