import assert from "assert";
import type {
  IConnection,
  ISubscriber,
  ISubscriptionManager,
  IdentifiedOperationRequest,
  OperationRequest,
  ISubscriptionEvent
} from "./types";
import { prefixRedisKey } from "./helpers";
import { getRedisClient } from "./helpers/redis";

interface RedisSubscriptionManagerOptions {
  /**
   * IORedis client instance
   */
  redisClientHost: string;
  /**
   * Optional function that can get subscription name from event
   *
   * Default is (event: ISubscriptionEvent) => event.event
   *
   * Useful for multi-tenancy
   */
  getSubscriptionNameFromEvent?: (event: ISubscriptionEvent) => string;
  /**
   * Optional function that can get subscription name from subscription connection
   *
   * Default is (name: string, connection: IConnection) => name
   *
   * Useful for multi-tenancy
   */
  getSubscriptionNameFromConnection?: (name: string, connection: IConnection) => string;
}

interface RedisSubscriber {
  connection: IConnection;
  operation: OperationRequest;
  event: string;
  subscriptionId: string;
  operationId: string;
}

/**
 * RedisSubscriptionManager
 *
 * Stores all subsrciption information in redis store
 *
 * Record types:
 *
 * subscription:
 *  key: `[app prefix]:subscription:[connectionId]:[operationId]:{[eventName]}` (where eventName is a keyslot)
 *  value: RedisSubscriber (this is always unique per client)
 *
 * subscriptionOperation:
 *  key: `[app prefix]:subscriptionOperation:[connectionId]:[operationId]`
 *  value: eventName
 *
 * connectionSubscriptionsList:
 *  key: `[app prefix]:connectionSubscriptionsList:[connectionId]`
 *  value: redis list of subscription keys corresponding to connectionId
 *
 * eventSubscriptionsList:
 *  key: `[app prefix]:eventSubscriptionsList:${eventName}`
 *  value: redis list of subscription keys corresponding to eventName
 */
export class RedisSubscriptionManager implements ISubscriptionManager {
  private redisClientHost: string;

  private getSubscriptionNameFromEvent: (event: ISubscriptionEvent) => string;

  private getSubscriptionNameFromConnection: (name: string, connection: IConnection) => string;

  constructor({
    redisClientHost,
    getSubscriptionNameFromEvent = event => event.event,
    getSubscriptionNameFromConnection = name => name
  }: RedisSubscriptionManagerOptions) {
    assert.ok(
      redisClientHost == null || typeof redisClientHost === "string",
      "Please provide redisClient as an instance of ioredis.Redis"
    );

    this.redisClientHost = redisClientHost;
    this.getSubscriptionNameFromEvent = getSubscriptionNameFromEvent;
    this.getSubscriptionNameFromConnection = getSubscriptionNameFromConnection;
  }

  subscribersByEvent = (
    event: ISubscriptionEvent
  ): AsyncIterable<ISubscriber[]> & AsyncIterator<ISubscriber[]> => {
    let offset = 0;
    const name = this.getSubscriptionNameFromEvent(event);

    return {
      next: async () => {
        const redisClient = await getRedisClient(this.redisClientHost);
        const keys = await redisClient.lrange(
          prefixRedisKey(`eventSubscriptionsList:${name}`),
          offset,
          offset + 50
        );

        offset += 50;

        if (keys.length === 0) {
          return { value: [], done: true };
        }
        const subscribers = (await redisClient.mget(...keys)).map((sub: string | null) =>
          sub ? JSON.parse(sub) : null
        );
        return { value: subscribers, done: false };
      },
      [Symbol.asyncIterator]() {
        return this;
      }
    };
  };

  subscribe = async (
    names: string[],
    connection: IConnection,
    operation: IdentifiedOperationRequest
  ): Promise<void> => {
    const redisClient = await getRedisClient(this.redisClientHost);
    const subscriptionId = this.generateSubscriptionId(connection.id, operation.operationId);

    // we can only subscribe to one subscription in GQL document
    if (names.length !== 1) {
      throw new Error("Only one active operation per event name is allowed");
    }
    let [eventName] = names;
    eventName = this.getSubscriptionNameFromConnection(eventName, connection);

    const subscriptionOperationKey = prefixRedisKey(`subscriptionOperation:${subscriptionId}`);
    const subscriptionKey = prefixRedisKey(`subscription:${subscriptionId}:{${eventName}}`);

    await Promise.all([
      redisClient.set(
        subscriptionKey,
        JSON.stringify({
          connection,
          operation,
          event: eventName,
          subscriptionId,
          operationId: operation.operationId
        } as RedisSubscriber),
        "EX",
        7200
      ),
      redisClient.set(subscriptionOperationKey, eventName, "EX", 7200),
      redisClient.lpush(prefixRedisKey(`eventSubscriptionsList:${eventName}`), subscriptionKey),
      redisClient.lpush(
        prefixRedisKey(`connectionSubscriptionsList:${connection.id}`),
        subscriptionKey
      )
    ]);
  };

  unsubscribe = async () => {
    /*
      Seems like this method is no longer used (it is invoked only in tests)
      `unsubscribeOperation` is used instead
    */
  };

  unsubscribeOperation = async (connectionId: string, operationId: string) => {
    const redisClient = await getRedisClient(this.redisClientHost);
    const subscriptionId = this.generateSubscriptionId(connectionId, operationId);

    const subscriptionOperationKey = prefixRedisKey(`subscriptionOperation:${subscriptionId}`);
    const eventName = await redisClient.get(subscriptionOperationKey);
    const subscriptionKey = prefixRedisKey(`subscription:${subscriptionId}:{${eventName}}`);

    let subscriber;
    const result = await redisClient.get(subscriptionKey);
    if (result) {
      subscriber = JSON.parse(result);
      await Promise.all([
        redisClient.del(subscriptionOperationKey),
        redisClient.del(subscriptionKey),
        redisClient.lrem(
          prefixRedisKey(`eventSubscriptionsList:${subscriber.event}`),
          0,
          subscriptionKey
        ),
        redisClient.lrem(
          prefixRedisKey(`connectionSubscriptionsList:${subscriber.connection.id}`),
          0,
          subscriptionKey
        )
      ]);
    }
  };

  unsubscribeAllByConnectionId = async (connectionId: string) => {
    const redisClient = await getRedisClient(this.redisClientHost);
    let done = false;
    const limit = 50;
    let offset = 0;
    const subscriptionListKey = prefixRedisKey(`connectionSubscriptionsList:${connectionId}`);

    do {
      const keys = await redisClient.lrange(subscriptionListKey, offset, offset + limit);
      offset += limit;

      if (!keys || keys.length === 0) {
        done = true;
      } else {
        await Promise.all(
          keys.map(async (key: string | null) => {
            if (key) {
              let subscriber;
              const result = await redisClient.get(key);
              if (result) {
                subscriber = JSON.parse(result);
                const subscriptionId = this.generateSubscriptionId(
                  connectionId,
                  subscriber.operationId
                );
                const subscriptionOperationKey = prefixRedisKey(
                  `subscriptionOperation:${subscriptionId}`
                );
                await Promise.all([
                  redisClient.del(subscriptionOperationKey),
                  redisClient.lrem(subscriptionListKey, 0, key),
                  redisClient.lrem(
                    prefixRedisKey(`eventSubscriptionsList:${subscriber.event}`),
                    0,
                    key
                  )
                ]);
              }
            }
          })
        );
        await redisClient.del(...keys);
      }
    } while (!done);
    await redisClient.del(subscriptionListKey);
  };

  generateSubscriptionId = (connectionId: string, operationId: string): string => {
    return `${connectionId}:${operationId}`;
  };
}
