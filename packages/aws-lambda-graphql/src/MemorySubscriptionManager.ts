import type {
  IConnection,
  ISubscriber,
  ISubscriptionEvent,
  ISubscriptionManager,
  OperationRequest
} from "./types";

interface MemorySubscriptionManagerOptions {
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

export class MemorySubscriptionManager implements ISubscriptionManager {
  private subscriptions: Map<string, ISubscriber[]>;

  private getSubscriptionNameFromEvent: (event: ISubscriptionEvent) => string;

  private getSubscriptionNameFromConnection: (name: string, connection: IConnection) => string;

  constructor({
    getSubscriptionNameFromEvent = event => event.event,
    getSubscriptionNameFromConnection = name => name
  }: MemorySubscriptionManagerOptions = {}) {
    this.subscriptions = new Map();
    this.getSubscriptionNameFromEvent = getSubscriptionNameFromEvent;
    this.getSubscriptionNameFromConnection = getSubscriptionNameFromConnection;
  }

  subscribersByEvent = (
    event: ISubscriptionEvent
  ): AsyncIterable<ISubscriber[]> & AsyncIterator<ISubscriber[]> => {
    // Safely get subscribers with better error handling
    let subscribers: ISubscriber[];
    try {
      const name = this.getSubscriptionNameFromEvent(event);
      const subscriptions = this.subscriptions.get(name) || [];
      subscribers = subscriptions.filter(subscriber => subscriber.event === name);
    } catch (error) {
      // Handle any errors during initialization
      const iterable = {
        next: () => Promise.reject(error),
        return: () => Promise.resolve({ done: true, value: undefined as any }),
        throw: (e: any) => Promise.reject(e),
        [Symbol.asyncIterator]: function () {
          return this;
        }
      };
      return iterable;
    }

    let done = false;

    const iterable = {
      next: () => {
        if (done) {
          return Promise.resolve({ done: true, value: undefined as any });
        }
        done = true;
        return Promise.resolve({ done: false, value: subscribers });
      },
      return: () => {
        done = true;
        return Promise.resolve({ done: true, value: undefined as any });
      },
      throw: (error: any) => {
        done = true;
        return Promise.reject(error);
      },
      [Symbol.asyncIterator]: function () {
        return this;
      }
    };

    return iterable;
  };

  subscribe = async (
    names: string[],
    connection: IConnection,
    operation: OperationRequest & { operationId: string }
  ): Promise<void> => {
    names.forEach(n => {
      const name = this.getSubscriptionNameFromConnection(n, connection);
      const subscriptions = this.subscriptions.get(name);
      const subscription = {
        connection,
        operation,
        event: name,
        operationId: operation.operationId
      };

      if (subscriptions == null) {
        this.subscriptions.set(name, [subscription]);
      } else if (!subscriptions.find(s => s.connection.id === connection.id)) {
        subscriptions.push({
          connection,
          operation,
          event: name,
          operationId: operation.operationId
        });
      }
    });
  };

  unsubscribe = async (subscriber: ISubscriber) => {
    const subscriptions = this.subscriptions.get(subscriber.event);

    if (subscriptions) {
      this.subscriptions.set(
        subscriber.event,
        subscriptions.filter(s => s.connection.id !== subscriber.connection.id)
      );
    }
  };

  unsubscribeOperation = async (connectionId: string, operationId: string) => {
    this.subscriptions.forEach((subscribers, event) => {
      this.subscriptions.set(
        event,
        subscribers.filter(
          subscriber =>
            subscriber.connection.id !== connectionId && subscriber.operationId !== operationId
        )
      );
    });
  };

  unsubscribeAllByConnectionId = (connectionId: string) => {
    for (const key of this.subscriptions.keys()) {
      this.subscriptions.set(
        key,
        this.subscriptions.get(key)!.filter(s => s.connection.id === connectionId)
      );
    }

    return Promise.resolve();
  };
}
