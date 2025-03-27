import type { ISubscriptionEvent } from "./types";
import { PubSubEngine } from "graphql-subscriptions";

/**
 * Array PubSub works as local PubSub that is already fed with all the events that were published
 *
 * Each time you call asyncIterator it will create an iterator that iterates over events
 *
 * This PubSub instance is used internally in event processor to simulate
 * source of events. Basically it acts as you were publishing events,
 * which were loaded from event store.
 *
 * If event payload is string, then it's parsed from JSON, otherwise it's used
 * as is and sent to your GraphQL schema.
 */
export class ArrayPubSub extends PubSubEngine {
  private events: ISubscriptionEvent[];

  constructor(events: ISubscriptionEvent[]) {
    super();
    this.events = events;
  }

  async publish(): Promise<void> {
    throw new Error("ArrayPubSub is read only");
  }

  async subscribe(triggerName: string, onMessage: Function, options: object): Promise<number> {
    throw new Error("Please do not use this PubSub implementation");
  }

  async unsubscribe(subId: number): Promise<void> {
    throw new Error("Please do not use this PubSub implementation");
  }

  // Create a custom iterator class that satisfies AsyncIterator interface
  asyncIterator<T>(eventNames: string | string[]): AsyncIterator<T> {
    const names = Array.isArray(eventNames) ? eventNames : [eventNames];

    const filteredEvents = this.events
      .filter(event => names.includes(event.event))
      .map(event =>
        typeof event.payload === "string" ? JSON.parse(event.payload) : event.payload
      );

    let i = 0;

    // Return a standards-compliant AsyncIterator that doesn't use Symbol.asyncIterator
    return {
      async next() {
        if (i >= filteredEvents.length) {
          return { done: true, value: undefined };
        }
        return { done: false, value: filteredEvents[i++] as T };
      },
      async return() {
        return { done: true, value: undefined };
      },
      async throw(error: any) {
        throw error;
      }
    };
  }
}
