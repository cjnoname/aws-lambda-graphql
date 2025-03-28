import { type ApolloServerOptions } from "@apollo/server";
import assert from "assert";
import type {
  APIGatewayProxyResult,
  APIGatewayProxyEvent,
  Context as LambdaContext,
  Handler as LambdaHandler
} from "aws-lambda";
import { isAsyncIterable } from "iterall";
import type { ExecutionResult } from "graphql";
import { PubSub } from "graphql-subscriptions";
import type {
  APIGatewayWebSocketEvent,
  IConnectionManager,
  IContext,
  IEventProcessor,
  ISubscriptionManager,
  IdentifiedOperationRequest,
  IConnection,
  OperationRequest
} from "./types";
import { extractEndpointFromEvent, parseOperationFromEvent } from "./helpers";
import {
  SERVER_EVENT_TYPES,
  isGQLConnectionInit,
  isGQLStopOperation,
  isGQLDisconnect
} from "./protocol";
import { formatMessage } from "./formatMessage";
import type { ExecutionParams } from "./execute";
import { execute } from "./execute";

type Options = Pick<
  ApolloServerOptions<any>,
  "fieldResolver" | "rootValue" | "schema" | "validationRules"
>;

interface ExtraGraphQLOptions extends Options {
  $$internal: IContext["$$internal"];
  context?: object | ((contextParams: IContext) => object | Promise<object>);
}

export interface WebSocketServerConfig<TServer extends object, TEventHandler extends LambdaHandler>
  extends Options {
  /**
   * Connection manager takes care of
   *  - registering/unregistering WebSocket connections
   *  - sending data to connections
   */
  connectionManager: IConnectionManager;
  context?: object | ((contextParams: IContext) => object | Promise<object>);
  eventProcessor: IEventProcessor<TServer, TEventHandler>;
  /**
   * Use to report errors from web socket handler
   */
  onError?: (err: any) => void;
  /**
   * Subscriptions manager takes care of
   *  - registering/unregistering connection's subscribed operations
   */
  subscriptionManager: ISubscriptionManager;
  subscriptions?: {
    onOperation?: (
      message: OperationRequest,
      params: ExecutionParams,
      connection: IConnection
    ) => Promise<ExecutionParams> | ExecutionParams;
    onOperationComplete?: (connection: IConnection, operationId: string) => void;
    /**
     * onWebsocketConnect is called when the Websocket connection is initialized ($connect route).
     * Return an object to set a context to your connection object saved in the database e.g. for saving authentication details.
     * This is especially useful to get authentication details (API GW authorizers only run in $connect route)
     *
     */
    onWebsocketConnect?: (
      connection: IConnection,
      event: APIGatewayWebSocketEvent,
      context: LambdaContext
    ) => Promise<boolean | { [key: string]: any }> | boolean | { [key: string]: any };
    /**
     * onConnect is called when the GraphQL connection is initialized (connection_init message).
     * Return an object to set a context to your connection object saved in the database e.g. for saving authentication details.
     *
     * NOTE: This is not the websocket $connect route, see onWebsocketConnect for the $connect route
     *
     */
    onConnect?: (
      messagePayload: { [key: string]: any } | undefined | null,
      connection: IConnection,
      event: APIGatewayWebSocketEvent,
      context: LambdaContext
    ) => Promise<boolean | { [key: string]: any }> | boolean | { [key: string]: any };
    onDisconnect?: (connection: IConnection) => void;
    /**
     * If connection is not initialized on GraphQL operation, wait for connection to be initialized
     * Or throw prohibited connection error
     *
     */
    waitForInitialization?: {
      /**
       * How many times should we try to determine connection state?
       *
       * Default is 10
       */
      retryCount?: number;
      /**
       * How long should we wait until we try determine connection state again?
       *
       * Default is 50ms
       */
      timeout?: number;
    };

    /**
     * If specified, the connection endpoint will be registered with this value as opposed to extracted from the event payload
     *
     */
    connectionEndpoint?: string;
  };
}

export class WebSocketServer<TEventHandler extends LambdaHandler = any> {
  private connectionManager: IConnectionManager;

  private eventProcessor: any;

  private onError: (err: any) => void;

  private subscriptionManager: ISubscriptionManager;

  private subscriptionOptions: WebSocketServerConfig<
    WebSocketServer,
    TEventHandler
  >["subscriptions"];

  private apolloConfig: Options;

  constructor({
    connectionManager,
    context,
    eventProcessor,
    onError,
    subscriptionManager,
    subscriptions,
    ...restConfig
  }: WebSocketServerConfig<WebSocketServer, TEventHandler>) {
    assert.ok(
      connectionManager,
      "Please provide connectionManager and ensure it implements IConnectionManager"
    );
    assert.ok(
      eventProcessor,
      "Please provide eventProcessor and ensure it implements IEventProcessor"
    );
    assert.ok(
      subscriptionManager,
      "Please provide subscriptionManager and ensure it implements ISubscriptionManager"
    );
    assert.ok(typeof onError === "function" || onError == null, "onError must be a function");
    assert.ok(
      subscriptions == null || typeof subscriptions === "object",
      "Property subscriptions must be an object"
    );

    this.apolloConfig = restConfig;
    this.connectionManager = connectionManager;
    this.eventProcessor = eventProcessor;
    this.onError = onError || (err => console.error(err));
    this.subscriptionManager = subscriptionManager;
    this.subscriptionOptions = subscriptions;
  }

  public getConnectionManager(): IConnectionManager {
    return this.connectionManager;
  }

  public getSubscriptionManager(): ISubscriptionManager {
    return this.subscriptionManager;
  }

  public async createGraphQLServerOptions(
    event: APIGatewayProxyEvent,
    context: LambdaContext,
    internal?: Omit<IContext["$$internal"], "connectionManager" | "subscriptionManager">
  ): Promise<ExtraGraphQLOptions> {
    const $$internal: IContext["$$internal"] = {
      // this provides all other internal params
      // that are assigned in web socket handler
      ...internal,
      connectionManager: this.connectionManager,
      subscriptionManager: this.subscriptionManager
    };

    const gqlContext = {
      event,
      lambdaContext: context,
      $$internal,
      ...($$internal.connection && $$internal.connection.data
        ? $$internal.connection.data.context
        : {})
    };
    return {
      ...this.apolloConfig,
      context: gqlContext,
      $$internal
    };
  }

  public createWebSocketHandler(): (
    event: APIGatewayWebSocketEvent,
    context: LambdaContext
  ) => Promise<APIGatewayProxyResult> | TEventHandler {
    return async (event, context) => {
      if (this.isWebSocketEvent(event)) {
        console.info("isWebSocketEvent", event);
        return await this.handleWebSocketEvent(event, context);
      } else {
        console.info("isStreamEvent", event);
        return await this.eventProcessor.createHandler(this)(event, context);
      }
    };
  }

  private isWebSocketEvent(event: any) {
    return (
      "requestContext" in event &&
      "connectionId" in event.requestContext &&
      "routeKey" in event.requestContext
    );
  }

  /**
   * WebSocket handler is responsible for processing AWS API Gateway v2 events
   */
  private handleWebSocketEvent = async (
    event: APIGatewayWebSocketEvent,
    context: LambdaContext
  ): Promise<APIGatewayProxyResult> => {
    try {
      // based on routeKey, do actions
      switch (event.requestContext.routeKey) {
        case "$connect": {
          const { onWebsocketConnect, connectionEndpoint } = this.subscriptionOptions || {};

          // register connection
          // if error is thrown during registration, connection is rejected
          // we can implement some sort of authorization here
          const endpoint = connectionEndpoint || extractEndpointFromEvent(event);

          const connection = await this.connectionManager.registerConnection({
            endpoint,
            connectionId: event.requestContext.connectionId
          });

          let newConnectionContext = {};

          if (onWebsocketConnect) {
            try {
              const result = await onWebsocketConnect(connection, event, context);

              if (result === false) {
                throw new Error("Prohibited connection!");
              } else if (result !== null && typeof result === "object") {
                newConnectionContext = result;
              }
            } catch (err) {
              const errorResponse = formatMessage({
                type: SERVER_EVENT_TYPES.GQL_ERROR,
                payload: { message: err.message }
              });

              await this.connectionManager.unregisterConnection(connection);

              return {
                body: errorResponse,
                statusCode: 401
              };
            }
          }

          // set connection context which will be available during graphql execution
          const connectionData = {
            ...connection.data,
            context: newConnectionContext
          };

          await this.connectionManager.setConnectionData(connectionData, connection);

          return {
            body: "",
            headers: event.headers?.["Sec-WebSocket-Protocol"]?.includes("graphql-transport-ws")
              ? {
                  "Sec-WebSocket-Protocol": "graphql-transport-ws"
                }
              : undefined,
            statusCode: 200
          };
        }
        case "$disconnect": {
          const { onDisconnect } = this.subscriptionOptions || {};
          // this event is called eventually by AWS APIGateway v2
          // we actually don't care about a result of this operation because client is already
          // disconnected, it is meant only for clean up purposes
          // hydrate connection
          const connection = await this.connectionManager.hydrateConnection(
            event.requestContext.connectionId
          );

          if (onDisconnect) {
            onDisconnect(connection);
          }

          await this.connectionManager.unregisterConnection(connection);

          return {
            body: "",
            statusCode: 200
          };
        }
        case "$default": {
          // here we are processing messages received from a client
          // if we respond here and the route has integration response assigned
          // it will send the body back to client, so it is easy to respond with operation results
          const { connectionId } = event.requestContext;
          const {
            onConnect,
            onOperation,
            onOperationComplete,
            waitForInitialization: {
              retryCount: waitRetryCount = 10,
              timeout: waitTimeout = 50
            } = {}
          } = this.subscriptionOptions || {};

          // parse operation from body
          const operation = parseOperationFromEvent(event);

          // hydrate connection
          let connection = await this.connectionManager.hydrateConnection(connectionId, {
            retryCount: 1,
            timeout: waitTimeout
          });

          if (isGQLConnectionInit(operation)) {
            let newConnectionContext = operation.payload;

            if (onConnect) {
              try {
                const result = await onConnect(operation.payload, connection, event, context);

                if (result === false) {
                  throw new Error("Prohibited connection!");
                } else if (result !== null && typeof result === "object") {
                  newConnectionContext = result;
                }
              } catch (err) {
                const errorResponse = formatMessage({
                  type: SERVER_EVENT_TYPES.GQL_ERROR,
                  payload: { message: err.message }
                });

                await this.connectionManager.sendToConnection(connection, errorResponse);
                await this.connectionManager.closeConnection(connection);

                return {
                  body: errorResponse,
                  statusCode: 401
                };
              }
            }

            // set connection context which will be available during graphql execution
            const connectionData = {
              ...connection.data,
              context: {
                ...connection.data?.context,
                ...newConnectionContext
              },
              isInitialized: true
            };

            await this.connectionManager.setConnectionData(connectionData, connection);

            // send GQL_CONNECTION_INIT message to client
            const response = formatMessage({
              type: SERVER_EVENT_TYPES.GQL_CONNECTION_ACK
            });

            await this.connectionManager.sendToConnection(connection, response);

            return {
              body: response,
              statusCode: 200
            };
          }

          // wait for connection to be initialized
          connection = await (async () => {
            let freshConnection: IConnection = connection;

            if (freshConnection.data.isInitialized) {
              return freshConnection;
            }

            for (let i = 0; i <= waitRetryCount; i++) {
              freshConnection = await this.connectionManager.hydrateConnection(connectionId);

              if (freshConnection.data.isInitialized) {
                return freshConnection;
              }

              // wait for another round
              await new Promise(r => setTimeout(r, waitTimeout));
            }

            return freshConnection;
          })();

          if (!connection.data.isInitialized) {
            // refuse connection which did not send GQL_CONNECTION_INIT operation
            const errorResponse = formatMessage({
              type: SERVER_EVENT_TYPES.GQL_ERROR,
              payload: { message: "Prohibited connection!" }
            });

            await this.connectionManager.sendToConnection(connection, errorResponse);
            await this.connectionManager.closeConnection(connection);

            return {
              body: errorResponse,
              statusCode: 401
            };
          }

          if (isGQLStopOperation(operation)) {
            // unsubscribe client
            if (onOperationComplete) {
              onOperationComplete(connection, operation.id);
            }
            const response = formatMessage({
              id: operation.id,
              type: SERVER_EVENT_TYPES.GQL_COMPLETE
            });

            await this.connectionManager.sendToConnection(connection, response);

            await this.subscriptionManager.unsubscribeOperation(connection.id, operation.id);

            return {
              body: response,
              statusCode: 200
            };
          }

          if (isGQLDisconnect(operation)) {
            // unregisterConnection will be handled by $disconnect, return straightaway
            return {
              body: "",
              statusCode: 200
            };
          }

          const pubSub = new PubSub();
          // following line is really redundant but we need to
          // this makes sure that if you invoke the event
          // and you use Context creator function
          // then it'll be called with $$internal context according to spec
          const options = await this.createGraphQLServerOptions(event, context, {
            // this allows createGraphQLServerOptions() to append more extra data
            // to context from connection.data.context
            connection,
            operation,
            pubSub,
            registerSubscriptions: true
          });
          const result = await execute({
            ...options,
            schema: options.schema!,
            connection,
            connectionManager: this.connectionManager,
            event,
            lambdaContext: context,
            onOperation,
            operation,
            pubSub,
            // tell execute to register subscriptions
            registerSubscriptions: true,
            subscriptionManager: this.subscriptionManager
          });

          if (!isAsyncIterable(result)) {
            // send response to client so it can finish operation in case of query or mutation
            if (onOperationComplete) {
              onOperationComplete(
                connection,
                (operation as IdentifiedOperationRequest).operationId
              );
            }
            const response = formatMessage({
              id: (operation as IdentifiedOperationRequest).operationId,
              payload: result as ExecutionResult,
              type: SERVER_EVENT_TYPES.GQL_NEXT
            });
            await this.connectionManager.sendToConnection(connection, response);
            return {
              body: response,
              statusCode: 200
            };
          }
          // this is just to make sure
          // when you deploy this using serverless cli
          // then integration response is not assigned to $default route
          // so this won't make any difference
          // but the sendToConnection above will send the response to client
          // so client'll receive the response for his operation
          return {
            body: "",
            statusCode: 200
          };
        }
        default: {
          throw new Error(`Invalid event ${(event.requestContext as any).routeKey} received`);
        }
      }
    } catch (e) {
      this.onError(e);

      return {
        body: e.message || "Internal server error",
        statusCode: 500
      };
    }
  };
}
