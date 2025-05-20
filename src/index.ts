import { randomUUIDv7, serve } from "bun";
import SturdyWebSocket from "sturdy-websocket";

import { logger } from "./logger";

const BYBIT_SUBSCRIBED_TOPICS: Record<string, number> = {};
const BYBIT_TOPICS_SNAPSHOTS: Record<string, any> = {};
const CONNECTED_CLIENTS = new Set<string>();

const bybitWs = new SturdyWebSocket("wss://stream.bybit.com/v5/public/linear");

const unsubscribeBybit = (topics: string[]) => {
  const toUnsubscribe: string[] = [];

  topics.forEach((topic) => {
    if (BYBIT_SUBSCRIBED_TOPICS[topic]) {
      BYBIT_SUBSCRIBED_TOPICS[topic]--;

      if (BYBIT_SUBSCRIBED_TOPICS[topic] === 0) {
        delete BYBIT_SUBSCRIBED_TOPICS[topic];
        delete BYBIT_TOPICS_SNAPSHOTS[topic];
        toUnsubscribe.push(topic);
      }
    }
  });

  if (toUnsubscribe.length > 0) {
    logger.info(`Unsubscribing Bybit from ${toUnsubscribe.length} topics`);
    bybitWs.send(JSON.stringify({ op: "unsubscribe", args: toUnsubscribe }));
  }
};

const server = serve<{ id: string; topics: string[] }, any>({
  port: process.env.PORT ?? 3000,
  routes: {
    "/health": new Response("OK"),
    "/ws": (req, server) => {
      if (server.upgrade(req, { data: { id: randomUUIDv7(), topics: [] } })) {
        return;
      }

      return new Response("Upgrade failed", { status: 500 });
    },
    "/*": new Response("Not Found", { status: 404 }),
  },
  websocket: {
    idleTimeout: 30_000,
    open(ws) {
      CONNECTED_CLIENTS.add(ws.data.id);
      logger.info(`Client connected: ${ws.data.id}`);
    },
    close(ws) {
      CONNECTED_CLIENTS.delete(ws.data.id);
      logger.info(`Client disconnected: ${ws.data.id}`);
      unsubscribeBybit(ws.data.topics);
    },
    message(ws, message) {
      if (typeof message !== "string") {
        logger.debug("Received non-string message");
        logger.debug(typeof message);
        return;
      }

      if (message.includes('"ping"')) {
        logger.debug(`Received ping from ${ws.data.id}`);
        const [, reqId] = /req_id=(\d+)/.exec(message) || [];
        ws.send(reqId ? `{"op":"pong","req_id":"${reqId}"}` : `{"op":"pong"}`);
        return;
      }

      try {
        const data: { op: string; args: string[]; req_id: string } =
          JSON.parse(message);

        if (data.op === "subscribe") {
          logger.info(
            `Client ${ws.data.id} subscribed to ${data.args.length} topics`,
          );

          const toSubscribe: string[] = [];

          data.args.forEach((topic) => {
            ws.subscribe(topic);
            ws.data.topics.push(topic);

            if (BYBIT_SUBSCRIBED_TOPICS[topic]) {
              BYBIT_SUBSCRIBED_TOPICS[topic]++;
            } else {
              toSubscribe.push(topic);
              BYBIT_SUBSCRIBED_TOPICS[topic] = 1;
            }

            if (BYBIT_TOPICS_SNAPSHOTS[topic]) {
              ws.send(
                JSON.stringify({
                  topic,
                  type: "snapshot",
                  data: BYBIT_TOPICS_SNAPSHOTS[topic],
                }),
              );
            }
          });

          if (toSubscribe.length > 0 && bybitWs.readyState === WebSocket.OPEN) {
            logger.info(`Subscribing to ${toSubscribe.length} Bybit topics`);
            bybitWs.send(
              JSON.stringify({ op: "subscribe", args: toSubscribe }),
            );
          }
        }

        if (data.op === "unsubscribe") {
          logger.info(
            `Client ${ws.data.id} unsubscribed from ${data.args.length} topics`,
          );

          data.args.forEach((topic) => {
            ws.unsubscribe(topic);
            const idx = ws.data.topics.indexOf(topic);
            if (idx !== -1) ws.data.topics.splice(idx, 1);
          });

          unsubscribeBybit(data.args);
        }
      } catch {
        logger.error(`Invalid message format from ${ws.data.id}`);
        logger.debug(message);
        ws.send(JSON.stringify({ error: "Invalid message format" }));
      }
    },
  },
});

let bybitPingInterval: NodeJS.Timeout | undefined;
let bybitPongReceived = true;
let bybitPongTimeout: NodeJS.Timeout | undefined;

const resetPongTimeout = () => {
  if (bybitPongTimeout) clearTimeout(bybitPongTimeout);
  bybitPongReceived = false;
  bybitPongTimeout = setTimeout(() => {
    if (!bybitPongReceived) bybitWs.reconnect();
  }, 5000);
};

const pingObString = `{"op":"ping"}`;
const onOpen = () => {
  const topics = Object.keys(BYBIT_SUBSCRIBED_TOPICS);

  if (topics.length > 0) {
    bybitWs.send(JSON.stringify({ op: "subscribe", args: topics }));
  }

  bybitPingInterval = setInterval(() => {
    bybitWs.send(pingObString);
    resetPongTimeout();
  }, 10_000);
};

bybitWs.addEventListener("open", onOpen);
bybitWs.addEventListener("reopen", onOpen);

const onClose = () => {
  logger.warn(`Bybit connection closed`);

  for (const key in BYBIT_TOPICS_SNAPSHOTS) {
    delete BYBIT_TOPICS_SNAPSHOTS[key];
  }

  if (bybitPingInterval) {
    clearInterval(bybitPingInterval);
    bybitPingInterval = undefined;
  }

  if (bybitPongTimeout) {
    clearTimeout(bybitPongTimeout);
    bybitPongTimeout = undefined;
  }
};

bybitWs.addEventListener("close", onClose);
bybitWs.addEventListener("down", onClose);

const handleOrderBookUpdate = (data: Record<string, any>) => {
  const orderBook = data.data as Record<string, string[][]>;
  const snapshot = BYBIT_TOPICS_SNAPSHOTS[data.topic] as Record<
    string,
    string[][]
  >;

  for (const key in orderBook) {
    const side = key as "a" | "b";
    const orders = orderBook[side];

    orders.forEach((order) => {
      const index = snapshot[side].findIndex((o) => o[0] === order[0]);
      const amount = parseFloat(order[1]);

      if (index === -1 && amount > 0) {
        snapshot[side].push(order);
        return;
      }

      if (index !== -1 && amount === 0) {
        snapshot[side].splice(index, 1);
        return;
      }

      if (index !== -1 && amount > 0) {
        snapshot[side][index][1] = order[1];
      }
    });
  }
};

bybitWs.addEventListener("message", (event) => {
  if (event.data.includes("pong")) {
    bybitPongReceived = true;
    if (bybitPongTimeout) clearTimeout(bybitPongTimeout);
    return;
  }

  try {
    const data = JSON.parse(event.data);

    if (typeof data.topic === "string") {
      // forward all bybit messages asap
      server.publish(data.topic, event.data);

      // receiving a snapshot, we will store it in memory
      if (data.type === "snapshot") {
        BYBIT_TOPICS_SNAPSHOTS[data.topic] = data.data;
      }
      // receiving a tickers updat
      else if (data.topic.includes("tickers.") && data.type === "delta") {
        if (BYBIT_TOPICS_SNAPSHOTS[data.topic]) {
          Object.assign(BYBIT_TOPICS_SNAPSHOTS[data.topic], data.data);
        }
      }
      // receiving a orderbook delta, we will update the orderbook in memory
      else if (data.topic.includes("orderbook.") && data.type === "delta") {
        handleOrderBookUpdate(data);
      }
    }
  } catch {
    // do nothing, we didnt receive JSON string
    logger.debug("Received non-JSON message");
    logger.debug(event.data);
  }
});
