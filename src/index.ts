import { randomUUIDv7, serve } from "bun";
import SturdyWebSocket from "sturdy-websocket";

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
    open(ws) {
      CONNECTED_CLIENTS.add(ws.data.id);
    },
    close(ws) {
      CONNECTED_CLIENTS.delete(ws.data.id);
      setTimeout(() => unsubscribeBybit(ws.data.topics), 30_000);
    },
    message(ws, message) {
      try {
        const data: { op: string; args: string[]; req_id: string } = JSON.parse(
          message as string,
        );

        if (data.op === "ping") {
          ws.send(JSON.stringify({ op: "pong", req_id: data.req_id }));
          return;
        }

        if (data.op === "subscribe") {
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
            bybitWs.send(
              JSON.stringify({ op: "subscribe", args: toSubscribe }),
            );
          }
        }

        if (data.op === "unsubscribe") {
          data.args.forEach((topic) => {
            ws.unsubscribe(topic);
            ws.data.topics.splice(ws.data.topics.indexOf(topic), 1);
          });

          setTimeout(() => unsubscribeBybit(data.args), 30_000);
        }
      } catch {
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

const onOpen = () => {
  const topics = Object.keys(BYBIT_SUBSCRIBED_TOPICS);

  if (topics.length > 0) {
    bybitWs.send(JSON.stringify({ op: "subscribe", args: topics }));
  }

  bybitPingInterval = setInterval(() => {
    bybitWs.send(JSON.stringify({ op: "ping" }));
    resetPongTimeout();
  }, 10_000);
};

bybitWs.addEventListener("open", onOpen);
bybitWs.addEventListener("reopen", onOpen);

const onClose = () => {
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
};

bybitWs.addEventListener("close", onClose);
bybitWs.addEventListener("down", onClose);

bybitWs.addEventListener("message", (event) => {
  if (event.data.includes("pong")) {
    bybitPongReceived = true;
    if (bybitPongTimeout) clearTimeout(bybitPongTimeout);
    return;
  }

  try {
    const data = JSON.parse(event.data);

    if (typeof data.topic === "string") {
      server.publish(data.topic, event.data);

      if (data.type === "snapshot") {
        BYBIT_TOPICS_SNAPSHOTS[data.topic] = data.data;
        return;
      }

      if (data.topic.startsWith("tickers.") && data.type === "update") {
        if (BYBIT_TOPICS_SNAPSHOTS[data.topic]) {
          Object.assign(BYBIT_TOPICS_SNAPSHOTS[data.topic], data.data);
        }
      }

      if (data.topic.startsWith("orderbook.") && data.type === "delta") {
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

            if (amount === 0) {
              snapshot[side].splice(index, 1);
              return;
            }

            snapshot[side][index][1] = order[1];
          });
        });
      }
    }
  } catch {
    // do nothing, we didnt receive JSON string
  }
});
