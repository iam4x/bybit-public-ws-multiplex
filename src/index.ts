import { randomUUIDv7, serve } from "bun";
import SturdyWebSocket from "sturdy-websocket";

import { logger } from "./logger";

const BYBIT_SUBSCRIBED_TOPICS: Record<string, number> = {};
const BYBIT_TOPICS_SNAPSHOTS: Record<string, any> = {};
const CONNECTED_CLIENTS = new Set<string>();

const bybitWs = new SturdyWebSocket("wss://stream.bybit.com/v5/public/linear");

const unsubscribeBybitTimeouts: Array<NodeJS.Timeout> = [];
const unsubscribeBybit = (topics: string[]) => {
  unsubscribeBybitTimeouts.push(
    setTimeout(() => {
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
        bybitWs.send(
          JSON.stringify({ op: "unsubscribe", args: toUnsubscribe }),
        );
      }
    }, 30_000),
  );
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

let clearTickerUpdatesBacklogTimeout: NodeJS.Timeout | undefined;

const clearTickerUpdatesBacklog = () => {
  const toSend: Record<string, Record<string, any>> = {};

  tickerUpdatesBacklog.forEach((update, idx) => {
    if (toSend[update.data.symbol]) {
      toSend[update.data.symbol].cs = update.data.cs;
      toSend[update.data.symbol].ts = update.data.ts;
      Object.assign(toSend[update.data.symbol].data, update.data);
    } else {
      toSend[update.data.symbol] = update;
    }

    // if the update is the last one for this symbol, we send it
    if (tickerUpdatesBacklog.lastIndexOf(update) === idx) {
      server.publish(update.topic, JSON.stringify(toSend[update.data.symbol]));
    }
  });

  logger.debug(
    `Cleared ${tickerUpdatesBacklog.length} backlog entries into ${Object.keys(toSend).length} updates`,
  );

  // clear backlog array
  tickerUpdatesBacklog.length = 0;

  // schedule ticker update in 50ms
  clearTickerUpdatesBacklogTimeout = setTimeout(
    () => clearTickerUpdatesBacklog(),
    50,
  );
};

const pingObString = `{"op":"ping"}`;
const onOpen = () => {
  const topics = Object.keys(BYBIT_SUBSCRIBED_TOPICS);

  if (topics.length > 0) {
    bybitWs.send(
      `{"op":"subscribe","args":[${topics.map((t) => `"${t}"`).join(",")}]}`,
    );
  }

  bybitPingInterval = setInterval(() => {
    bybitWs.send(pingObString);
    resetPongTimeout();
  }, 10_000);

  clearTickerUpdatesBacklogTimeout = setTimeout(
    () => clearTickerUpdatesBacklog(),
    50,
  );
};

bybitWs.addEventListener("open", onOpen);
bybitWs.addEventListener("reopen", onOpen);

const onClose = () => {
  logger.warn(`Bybit connection closed`);

  Object.keys(BYBIT_TOPICS_SNAPSHOTS).forEach((topic) => {
    delete BYBIT_TOPICS_SNAPSHOTS[topic];
  });

  if (bybitPingInterval) {
    clearInterval(bybitPingInterval);
    bybitPingInterval = undefined;
  }

  if (bybitPongTimeout) {
    clearTimeout(bybitPongTimeout);
    bybitPongTimeout = undefined;
  }

  if (unsubscribeBybitTimeouts.length > 0) {
    unsubscribeBybitTimeouts.forEach((timeout) => clearTimeout(timeout));
    unsubscribeBybitTimeouts.length = 0;
  }

  if (clearTickerUpdatesBacklogTimeout) {
    clearTimeout(clearTickerUpdatesBacklogTimeout);
    clearTickerUpdatesBacklogTimeout = undefined;
  }
};

bybitWs.addEventListener("close", onClose);
bybitWs.addEventListener("down", onClose);

const tickerUpdatesBacklog: Record<string, any>[] = [];
const handleTickerUpdate = (data: Record<string, any>) => {
  tickerUpdatesBacklog.push(data);

  if (BYBIT_TOPICS_SNAPSHOTS[data.topic]) {
    Object.assign(BYBIT_TOPICS_SNAPSHOTS[data.topic], data.data);
  }
};

const handleOrderBookUpdate = (data: Record<string, any>) => {
  const orderBook = data.data as Record<string, string[][]>;
  const snapshot = BYBIT_TOPICS_SNAPSHOTS[data.topic] as Record<
    string,
    string[][]
  >;

  Object.entries(orderBook).forEach(([side, orders]) => {
    if (side !== "a" && side !== "b") return;

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
  });
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
      // receiving a tickers update, we will batch those in 50ms updates
      if (data.topic.includes("tickers.") && data.type === "delta") {
        handleTickerUpdate(data);
        return;
      }

      // receiving a snapshot, we will store it in memory
      if (data.type === "snapshot") {
        BYBIT_TOPICS_SNAPSHOTS[data.topic] = data.data;
      }
      // receiving a orderbook delta, we will update the orderbook in memory
      else if (data.topic.includes("orderbook.") && data.type === "delta") {
        handleOrderBookUpdate(data);
      }

      // forward all bybit messages that weren't caught before
      server.publish(data.topic, event.data);
    }
  } catch {
    // do nothing, we didnt receive JSON string
    logger.debug("Received non-JSON message");
    logger.debug(event.data);
  }
});
