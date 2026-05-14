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

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => AckType,
): Promise<void> {
    const [channel, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);
    channel.consume(queue.queue, (msg: amqp.ConsumeMessage | null) => {
        if (msg) {
            const content = msg.content.toString();
            let outcome;
            try {
                const data = JSON.parse(content) as T;
                outcome = handler(data);
            } catch (err) {
                console.error("Failed to parse message content as JSON:", err);
                channel.nack(msg, false, false);
            }

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
        }
    });
}