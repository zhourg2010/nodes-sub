// routes/free.ts — 免费节点池的三个入口。
//
//   GET  {ADMIN_PATH}/free           后台面板:池子现状 + 各源战报 + 手动抓一轮的按钮
//   POST {ADMIN_PATH}/free/harvest   手动触发一轮抓取(登录态鉴权)
//   GET  /free/pool                  给本地实测端拉池子用(PUSH_KEY 鉴权)
//   GET  /free/verify                最近几轮实测的通过率(PUSH_KEY 鉴权)
//   POST /free/verify                收客户端实测回来的一轮结果(PUSH_KEY 鉴权)
//
// 为什么拉池子的接口要鉴权:池子里存的是完整的分享链接,含 uuid / 密码。虽然这些节点
// 本来就是公开来源抓的,但把一个"聚合了几千条可用节点的接口"挂在公网上无鉴权,等于替
// 别人做了个免费的聚合服务,白白消耗这些节点的带宽,也会让我们这个域名很快被盯上。
// 复用 PUSH_KEY 而不是新开一个密钥:本地端本来就有它,不用再配一份。

import { isAuthed, isPushKeyed } from "../auth.ts";
import { harvestAll } from "../free/harvest.ts";
import {
  type CheckResult,
  CHECK_ROUNDS,
  checkSummary,
  freeStoreEnabled,
  getPool,
  poolStats,
  prune,
  saveChecks,
} from "../free/store.ts";
import { freePanel } from "../free/ui.ts";

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

/** 后台面板 + 手动抓取。 */
export async function handleFreeAdmin(req: Request, url: URL): Promise<Response> {
  if (!await isAuthed(req)) return new Response("Unauthorized", { status: 401 });

  if (req.method === "POST" && url.pathname.endsWith("/harvest")) {
    const report = await harvestAll();
    return json(report);
  }
  if (req.method === "POST" && url.pathname.endsWith("/prune")) {
    const n = await prune();
    return json({ ok: true, pruned: n });
  }

  const stats = await poolStats();
  return new Response(freePanel(stats), {
    headers: { "content-type": "text/html; charset=utf-8" },
  });
}

/**
 * 给本地实测端拉池子。
 *
 * 返回的是**未经实测**的候选节点 —— 免费节点里大部分是死的,这一点没法在服务端解决:
 * Deno Deploy 上没有代理内核,拨不了这些节点,判断不了活没活。活性、速度、以及那道严格的
 * 美国 GeoIP 核实,全部由本地端(nodepipe 或客户端里的 mihomo)负责。这里只管"把候选
 * 攒齐、去重、限流"。
 *
 * 查询参数:
 *   limit     最多返回几条,默认 500
 *   perCred   每套凭据最多几条,默认 3(见 free/identity.ts 里 CF 扇出那段)
 *   protos    逗号分隔的协议白名单,不填就是全部
 *   days      只要最近几天还出现过的,默认 7
 *   order     popular(默认,反复出现过的优先) | stale(最久没测的优先,没测过的排最前)
 *   format    uris(默认,每行一条分享链接) | base64(整段 base64,跟 /push 的格式一致) | json
 *
 * **实测要用 order=stale。** 默认的 popular 排序是确定性的,每轮都返回同一批 ——
 * 跑七轮等于把同样那几十条测了七遍,池子里其余几千条一次都轮不到,而且从结果上
 * 完全看不出来,只会显得"池子里就这些节点"。
 *
 * format=json 的每一行都带 check 字段(测过几次 / 通了几次 / 最后一次 / 延迟中位数),
 * 界面上的筛选条件全从它来。uris 和 base64 两种格式只出链接,不带这些。
 */
export async function handleFreePool(req: Request, url: URL): Promise<Response> {
  if (!isPushKeyed(req)) return new Response("Unauthorized", { status: 401 });
  if (!freeStoreEnabled) {
    return json({ error: "未配置 DATABASE_URL,免费池没有存储后端" }, 503);
  }

  const q = url.searchParams;
  // 认不出的 order 一律当 popular:实测那条路显式传 stale,传错了退回默认顺序
  // 也只是测得不均匀,不该 400 把整轮实测卡住。
  const order = q.get("order") === "stale" ? "stale" : "popular";
  const rows = await getPool({
    order,
    // 下限也要夹。原来只有 Math.min(…, 5000),负数会原样传进 SQL,
    // Postgres 对 LIMIT -5 是直接报错("LIMIT must not be negative")、整个接口 500。
    // 接口本身有 PUSH_KEY 鉴权,不是安全问题,但没道理让一个手滑的参数把接口打挂。
    limit: Math.min(Math.max(1, Number(q.get("limit") ?? 500) || 500), 5000),
    perCred: Math.max(1, Number(q.get("perCred") ?? 3) || 3),
    protos: (q.get("protos") ?? "").split(",").map((s) => s.trim()).filter(Boolean),
    freshDays: Math.max(1, Number(q.get("days") ?? 7) || 7),
  });

  const format = q.get("format") ?? "uris";
  if (format === "json") return json({ count: rows.length, nodes: rows });

  const text = rows.map((r) => r.uri).join("\n") + (rows.length ? "\n" : "");
  if (format === "base64") {
    return new Response(btoa(text), { headers: { "content-type": "text/plain; charset=utf-8" } });
  }
  return new Response(text, { headers: { "content-type": "text/plain; charset=utf-8" } });
}

/**
 * /free/verify —— 客户端实测结果的收发口。
 *
 * Deno Deploy 上没有代理内核,拨不了节点,所以验证只能在客户端做:Spigot 用池子里的
 * 节点单独起一个 mihomo 探针进程(不碰用户自己那份配置),逐个 `/proxies/{name}/delay`,
 * 测完把结果 POST 回这里。服务端只负责存和汇总。
 *
 * POST 请求体:
 *   { "results": [ { "uriHash": "...", "ok": true, "latencyMs": 240 },
 *                  { "uriHash": "...", "ok": false, "err": "timeout" } ] }
 *
 * 轮号由服务端分配,客户端不用管。只保留最近 CHECK_ROUNDS 轮。
 *
 * GET 返回每轮的通过率(新的在前),给客户端界面显示"前几轮什么样",
 * 不然用户每跑一轮只能看见这一轮的数字,看不出池子是在变好还是变差。
 */
export async function handleFreeVerify(req: Request): Promise<Response> {
  if (!isPushKeyed(req)) return new Response("Unauthorized", { status: 401 });

  if (req.method === "GET") {
    if (!freeStoreEnabled) {
      return json({ error: "未配置 DATABASE_URL,免费池没有存储后端" }, 503);
    }
    const sum = await checkSummary();
    return json({ keepRounds: CHECK_ROUNDS, ...(sum ?? { rounds: [], latestRound: 0 }) });
  }

  if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405 });

  // 先校验请求体,再看存储后端在不在。
  // 400 是"你发的东西不对",503 是"我这边没配好" —— 前者优先。反过来的话,
  // 调用方在没配 DATABASE_URL 的环境里永远只看到 503,看不出自己的格式还错着。
  let body: unknown;
  try {
    body = await req.json();
  } catch {
    return json({ error: "请求体不是合法 JSON" }, 400);
  }

  const raw = (body as { results?: unknown })?.results;
  if (!Array.isArray(raw)) return json({ error: "缺少 results 数组" }, 400);
  if (raw.length === 0) return json({ error: "results 是空的,没什么可存的" }, 400);
  // 一轮最多这么多条。池子上限 5000,给一倍余量;再多多半是调用方写错了循环。
  if (raw.length > 10000) return json({ error: `一次最多 10000 条,收到 ${raw.length}` }, 400);

  const results: CheckResult[] = [];
  for (const [i, item] of raw.entries()) {
    const o = item as Record<string, unknown>;
    const h = typeof o?.uriHash === "string" ? o.uriHash.trim() : "";
    // uri_hash 是 SHA-256 的十六进制,64 位。形状不对就直接拒,别等外键去挡 ——
    // 那样错误信息是一句 Postgres 的外键报错,看不出是第几条、错在哪。
    if (!/^[0-9a-f]{64}$/.test(h)) {
      return json({ error: `第 ${i} 条的 uriHash 不是 64 位十六进制:${JSON.stringify(o?.uriHash)}` }, 400);
    }
    if (typeof o.ok !== "boolean") {
      return json({ error: `第 ${i} 条的 ok 必须是 true/false,收到 ${JSON.stringify(o.ok)}` }, 400);
    }
    const ms = o.latencyMs;
    if (ms != null && (typeof ms !== "number" || !Number.isFinite(ms) || ms < 0)) {
      return json({ error: `第 ${i} 条的 latencyMs 不是非负数字:${JSON.stringify(ms)}` }, 400);
    }
    results.push({
      uriHash: h,
      ok: o.ok,
      latencyMs: typeof ms === "number" ? ms : null,
      err: typeof o.err === "string" ? o.err : "",
    });
  }

  if (!freeStoreEnabled) {
    return json({ error: "未配置 DATABASE_URL,免费池没有存储后端" }, 503);
  }

  const r = await saveChecks(results);
  return json({
    ok: true,
    round: r.round,
    received: results.length,
    // saved 可能小于 received:库里已经没有的节点(被 prune 掉了)会被跳过
    saved: r.saved,
    skipped: results.length - r.saved,
    prunedRounds: r.prunedRounds,
    keepRounds: CHECK_ROUNDS,
  });
}
