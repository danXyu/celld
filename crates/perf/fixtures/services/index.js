// The celld-perf services fixture: one route per platform service. The
// load generator appends `cell=NAME`; here it names a KV key, an R2 key,
// or nothing. Queue delivery and Workflow completion are timed inside the
// node and folded into the Stats cell, which `/stats?kind=` reads.
//
//   /kv/put?bytes=N   /kv/get   /kv/list?limit=N
//   /d1/run           /d1/batch?n=N          /d1/read?limit=N
//   /r2/put?bytes=N   /r2/get   /r2/list?limit=N
//   /queue/send?bytes=N
//   /wf/create?steps=N
//   /stats?kind=queue|workflow
import { DurableObject, WorkflowEntrypoint } from "cloudflare:workers";

let schemaReady = false;

async function ensureSchema(db) {
  if (!schemaReady) {
    await db.exec("CREATE TABLE IF NOT EXISTS rows (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT)");
    schemaReady = true;
  }
}

function stats(env) {
  return env.STATS.get(env.STATS.idFromName("stats"));
}

export class Stats extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    const kind = url.searchParams.get("kind") ?? "none";
    const current = (await this.ctx.storage.get(kind)) ?? { count: 0, sum_ms: 0, max_ms: 0 };
    if (url.pathname === "/record") {
      current.count += Number(url.searchParams.get("count") ?? 1);
      current.sum_ms += Number(url.searchParams.get("sum") ?? 0);
      current.max_ms = Math.max(current.max_ms, Number(url.searchParams.get("max") ?? 0));
      await this.ctx.storage.put(kind, current);
    }
    return Response.json({
      ...current,
      mean_ms: current.count ? current.sum_ms / current.count : 0,
    });
  }
}

export class Flow extends WorkflowEntrypoint {
  async run(event, step) {
    const steps = event.payload.steps ?? 1;
    for (let i = 0; i < steps; i++) {
      await step.do(`step ${i}`, async () => i);
    }
    await step.do("report", async () => {
      const ms = Date.now() - event.payload.t;
      await stats(this.env).fetch(`http://stats/record?kind=workflow&sum=${ms}&max=${ms}`);
      return ms;
    });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.searchParams.get("cell") ?? "key";
    const number = (name, fallback) => Number(url.searchParams.get(name) ?? fallback);
    switch (url.pathname) {
      case "/kv/put":
        await env.KV.put(key, "k".repeat(number("bytes", 100)));
        return new Response("ok");
      case "/kv/get":
        return Response.json({ bytes: ((await env.KV.get(key)) ?? "").length });
      case "/kv/list": {
        const listed = await env.KV.list({ limit: number("limit", 100) });
        return Response.json({ keys: listed.keys.length });
      }
      case "/d1/run":
        await ensureSchema(env.DB);
        await env.DB.prepare("INSERT INTO rows (body) VALUES (?)").bind("d".repeat(number("bytes", 100))).run();
        return new Response("ok");
      case "/d1/batch": {
        await ensureSchema(env.DB);
        const statement = env.DB.prepare("INSERT INTO rows (body) VALUES (?)");
        const n = number("n", 10);
        await env.DB.batch(Array.from({ length: n }, () => statement.bind("b")));
        return Response.json({ n });
      }
      case "/d1/read": {
        await ensureSchema(env.DB);
        const { results } = await env.DB
          .prepare("SELECT id, body FROM rows ORDER BY id DESC LIMIT ?")
          .bind(number("limit", 20))
          .all();
        return Response.json({ rows: results.length });
      }
      case "/r2/put":
        await env.FILES.put(key, "r".repeat(number("bytes", 1024)));
        return new Response("ok");
      case "/r2/get": {
        const object = await env.FILES.get(key);
        return Response.json({ bytes: object ? (await object.arrayBuffer()).byteLength : 0 });
      }
      case "/r2/list": {
        const listed = await env.FILES.list({ limit: number("limit", 100) });
        return Response.json({ objects: listed.objects.length });
      }
      case "/queue/send":
        await env.JOBS.send({ t: Date.now(), pad: "q".repeat(number("bytes", 100)) });
        return new Response("ok");
      case "/wf/create": {
        const instance = await env.FLOWS.create({ params: { steps: number("steps", 1), t: Date.now() } });
        return Response.json({ id: instance.id });
      }
      case "/stats":
        return stats(env).fetch(`http://stats/get?kind=${url.searchParams.get("kind")}`);
      default:
        return new Response("not found", { status: 404 });
    }
  },

  async queue(batch, env) {
    const now = Date.now();
    let sum = 0;
    let max = 0;
    for (const message of batch.messages) {
      const lag = now - message.body.t;
      sum += lag;
      max = Math.max(max, lag);
      message.ack();
    }
    await stats(env).fetch(
      `http://stats/record?kind=queue&count=${batch.messages.length}&sum=${sum}&max=${max}`,
    );
  },
};
