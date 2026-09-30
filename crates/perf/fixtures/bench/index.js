// The celld-perf workload fixture. Every route takes its parameters from
// the query string, so one deployment serves every scenario.
//
//   /noop                     a stateless Worker that returns at once
//   /cpu?iters=N              a stateless Worker that spins N iterations
//   /do/<op>?cell=NAME&...    one operation on the Bench cell NAME
//   /ws?cell=NAME             a hibernatable WebSocket on the cell NAME
//
// Each cell counts its acknowledged writes in `n`, so a verification sweep
// can compare the count with what the load generator saw acknowledged.
import { DurableObject } from "cloudflare:workers";

const BLOB_ROW = "x".repeat(64 * 1024);

export class Bench extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.storage = ctx.storage;
    this.sql = ctx.storage.sql;
    this.sql.exec("CREATE TABLE IF NOT EXISTS rows (id INTEGER PRIMARY KEY, body TEXT)");
    this.sql.exec("CREATE TABLE IF NOT EXISTS blobs (id INTEGER PRIMARY KEY, body TEXT)");
    // A client "ping" is answered without waking the cell.
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1]);
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
    const op = url.pathname.split("/")[2] ?? "noop";
    const param = (name, fallback) => Number(url.searchParams.get(name) ?? fallback);
    switch (op) {
      case "noop":
        return new Response("ok");
      case "read":
        return Response.json({ n: (await this.storage.get("n")) ?? 0 });
      case "write": {
        const n = ((await this.storage.get("n")) ?? 0) + 1;
        await this.storage.put({ n, v: "w".repeat(param("bytes", 100)) });
        return Response.json({ n });
      }
      case "state":
        return Response.json({ n: (await this.storage.get("n")) ?? 0 });
      case "sql": {
        const rows = param("rows", 10);
        const body = "r".repeat(param("bytes", 100));
        this.storage.transactionSync(() => {
          for (let i = 0; i < rows; i++) {
            this.sql.exec("INSERT INTO rows(body) VALUES (?)", body);
          }
        });
        return Response.json({ rows });
      }
      case "sqlread": {
        const rows = this.sql
          .exec("SELECT id, body FROM rows ORDER BY id DESC LIMIT ?", param("rows", 10))
          .toArray();
        return Response.json({ rows: rows.length });
      }
      case "blob": {
        // Grow the database to at least `kb` KiB, once; later calls only read.
        const want = Math.ceil(param("kb", 1024) / 64);
        const have = this.sql.exec("SELECT count(*) AS c FROM blobs").one().c;
        this.storage.transactionSync(() => {
          for (let i = have; i < want; i++) {
            this.sql.exec("INSERT INTO blobs(id, body) VALUES (?, ?)", i, BLOB_ROW);
          }
        });
        return Response.json({ kb: Math.max(have, want) * 64 });
      }
      case "alarm": {
        const at = Date.now() + param("in", 1000);
        await this.storage.put("alarm_at", at);
        await this.storage.setAlarm(at);
        return Response.json({ at });
      }
      case "alarmstat":
        return Response.json({
          fired: (await this.storage.get("alarm_fired")) ?? 0,
          lateSum: (await this.storage.get("alarm_late_sum")) ?? 0,
          lateMax: (await this.storage.get("alarm_late_max")) ?? 0,
        });
      case "rpc": {
        const depth = param("depth", 1);
        if (depth <= 0) {
          return new Response("ok");
        }
        const name = `${url.searchParams.get("cell")}/${depth}`;
        const next = this.env.BENCH.get(this.env.BENCH.idFromName(name));
        return next.fetch(`http://bench/do/rpc?cell=${encodeURIComponent(name)}&depth=${depth - 1}`);
      }
      default:
        return new Response(`unknown op ${op}`, { status: 400 });
    }
  }

  async alarm() {
    const at = (await this.storage.get("alarm_at")) ?? Date.now();
    const late = Math.max(0, Date.now() - at);
    await this.storage.put({
      alarm_fired: ((await this.storage.get("alarm_fired")) ?? 0) + 1,
      alarm_late_sum: ((await this.storage.get("alarm_late_sum")) ?? 0) + late,
      alarm_late_max: Math.max((await this.storage.get("alarm_late_max")) ?? 0, late),
    });
  }

  // "b:..." fans out to every socket on the cell; "w:..." writes, then
  // echoes; anything else echoes without touching storage.
  async webSocketMessage(ws, message) {
    const text = typeof message === "string" ? message : new TextDecoder().decode(message);
    if (text.startsWith("b:")) {
      for (const socket of this.ctx.getWebSockets()) {
        socket.send(text);
      }
    } else if (text.startsWith("w:")) {
      const n = ((await this.storage.get("n")) ?? 0) + 1;
      await this.storage.put("n", n);
      ws.send(text);
    } else {
      ws.send(text);
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/noop") {
      return new Response("ok");
    }
    if (url.pathname === "/cpu") {
      // Iterations, not a clock: a Worker's clock does not advance while
      // it computes.
      const iters = Number(url.searchParams.get("iters") ?? 10000);
      let acc = 0;
      for (let i = 0; i < iters; i++) {
        acc = (acc * 31 + i) % 1000003;
      }
      return Response.json({ acc });
    }
    const cell = url.searchParams.get("cell") ?? "default";
    if (url.pathname.startsWith("/do/") || url.pathname === "/ws") {
      return env.BENCH.get(env.BENCH.idFromName(cell)).fetch(request);
    }
    return new Response("not found", { status: 404 });
  },
};
