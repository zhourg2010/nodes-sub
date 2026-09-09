// free/store.ts — 免费节点池的存储层。Neon Postgres。
//
// 关于"服务器端的 SQLite"
// ----------------------
// 这个服务跑在 Deno Deploy 上,**没有可持久化的文件系统**:isolate 是无状态的,冷启动
// 会换一台机器,写到本地的 .db 文件下一次请求就不见了。所以服务器端的 SQLite 文件这条路
// 在当前部署形态下走不通 —— 不是麻烦,是根本存不住。
//
// 能持久化的只有两样:Deno KV(已经用来存节点池和设备)和 Neon Postgres(已经用来归档
// 访问日志,连接串就是现成的 DATABASE_URL)。这里选 Neon,原因:
//   - 免费池是几千行带多个维度的记录,要按 cred_id 分组、按 last_seen 排序、按来源统计,
//     这是关系型查询,KV 的键值扫描做起来很别扭
//   - DATABASE_URL 已经配好了,不用新开任何基础设施
//   - 抓取战报要留历史,方便看某个源是从哪天开始坏的
//
// 本地那份 SQLite(nodepipe/node_cache.py)保持不动,它记的是另一件事(节点在历次推送里
// 的出现次数),两者不冲突。
//
// 没配 DATABASE_URL 时整个模块降级成"什么都不做":抓取会照跑并把结果报给调用方,
// 只是不落库。跟 db.ts 的降级策略一致,不会因为免费池把订阅主链路带崩。

import { neon } from "jsr:@neon/serverless";

const DATABASE_URL = Deno.env.get("DATABASE_URL") ?? "";
export const freeStoreEnabled = !!DATABASE_URL;
// deno-lint-ignore no-explicit-any
const sql: any = freeStoreEnabled ? neon(DATABASE_URL) : null;

export interface FreeNode {
  uriHash: string;
  uri: string;
  proto: string;
  name: string;
  server: string;
  port: number;
  endpointId: string;
  credId: string;
  sourceId: string;
}

/**
 * 一个节点在**保留的那几轮**里的实测战绩。
 *
 * 为什么不是"最近 N 轮全通"这种说法:每一轮只测池子的一小批(客户端一轮几十到几百条,
 * 池子是几千条),所以"第 5 轮"和"第 6 轮"测的根本不是同一批节点。对单个节点有意义的
 * 只有"它自己被测过几次、其中通了几次"。
 */
export interface CheckStat {
  /** 被测过几次。0 = 从来没测过 */
  checked: number;
  /** 其中通了几次 */
  ok: number;
  /** 最后一次的结果。没测过是 null —— 跟 false 不是一回事,不能混 */
  lastOk: boolean | null;
  /** 最后一次测的时间,'MM-DD HH:MM'。没测过是空串 */
  lastTs: string;
  /** 通的那几次的延迟中位数。一次都没通过是 null */
  medianMs: number | null;
}

export interface PoolRow extends FreeNode {
  firstSeen: string;
  lastSeen: string;
  seenCount: number;
  check: CheckStat;
}

let ready = false;
async function ensureTables(): Promise<void> {
  if (!sql || ready) return;
  await sql`CREATE TABLE IF NOT EXISTS free_node (
    uri_hash    TEXT PRIMARY KEY,
    uri         TEXT NOT NULL,
    proto       TEXT NOT NULL,
    name        TEXT NOT NULL DEFAULT '',
    server      TEXT NOT NULL,
    port        INTEGER NOT NULL,
    endpoint_id TEXT NOT NULL,
    cred_id     TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    seen_count  INTEGER NOT NULL DEFAULT 1
  )`;
  // cred_id 上的索引是给"每套凭据最多取几条"那个限流用的(见 identity.ts 里 CF 扇出那段)
  await sql`CREATE INDEX IF NOT EXISTS idx_free_cred ON free_node (cred_id)`;
  await sql`CREATE INDEX IF NOT EXISTS idx_free_last ON free_node (last_seen DESC)`;
  // 每轮抓取的战报。留着是为了能回答"这个源是从哪天开始不出货的"——免费源说没就没,
  // 没有历史的话只能看到"今天为止都是 0",看不出是今天坏的还是坏了半个月。
  await sql`CREATE TABLE IF NOT EXISTS free_harvest (
    id        BIGSERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_id TEXT NOT NULL,
    ok        BOOLEAN NOT NULL,
    parsed    INTEGER NOT NULL DEFAULT 0,
    kept      INTEGER NOT NULL DEFAULT 0,
    err       TEXT NOT NULL DEFAULT ''
  )`;
  await sql`CREATE INDEX IF NOT EXISTS idx_free_harvest_ts ON free_harvest (ts DESC)`;
  // 客户端实测回传的结果。**只留最近 CHECK_ROUNDS 轮**,见 saveChecks。
  //
  // 为什么按"轮"而不是按天数留:免费池是几千上万条,按天留的话跑得勤就爆、跑得少就
  // 什么都没有;按轮留则不管你多久跑一次,表的行数上限恒定 = 池子大小 × 轮数。
  //
  // ON DELETE CASCADE:prune() 清掉很久没出现的节点时,它的验证记录跟着走,
  // 不留一堆指向已经不存在的节点的孤儿行。
  await sql`CREATE TABLE IF NOT EXISTS free_check (
    uri_hash   TEXT NOT NULL REFERENCES free_node(uri_hash) ON DELETE CASCADE,
    round      BIGINT NOT NULL,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    ok         BOOLEAN NOT NULL,
    latency_ms INTEGER,
    err        TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (uri_hash, round)
  )`;
  await sql`CREATE INDEX IF NOT EXISTS idx_free_check_round ON free_check (round DESC)`;
  ready = true;
}

/** URI 的 SHA-256,当主键。URI 本身可能上千字符(vmess 的 base64),不适合直接做索引键。 */
export async function hashUri(uri: string): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(uri));
  return Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join("");
}

// 一条 INSERT 塞多少行。Neon 走 HTTP,一次请求一个语句,所以用 unnest 批量插比
// 一行一条快几个数量级;但单条语句的参数体积也不能无限大,分批。
const CHUNK = 500;

/**
 * 批量写入/更新。已经存在的(同一条 URI)只更新 last_seen 和 seen_count。
 *
 * seen_count 是个有用的稳定性信号:一个节点连着十几轮抓取都在,说明背后的机器是长期
 * 在跑的;只出现过一次的多半是别人测试完就撤了。跟 node_cache.py 里 appearances 一个意思。
 */
export async function upsertNodes(nodes: FreeNode[]): Promise<number> {
  if (!sql || nodes.length === 0) return 0;
  await ensureTables();
  let n = 0;
  for (let i = 0; i < nodes.length; i += CHUNK) {
    const b = nodes.slice(i, i + CHUNK);
    await sql`
      INSERT INTO free_node (uri_hash, uri, proto, name, server, port, endpoint_id, cred_id, source_id)
      SELECT * FROM unnest(
        ${b.map((x) => x.uriHash)}::text[],
        ${b.map((x) => x.uri)}::text[],
        ${b.map((x) => x.proto)}::text[],
        ${b.map((x) => x.name)}::text[],
        ${b.map((x) => x.server)}::text[],
        ${b.map((x) => x.port)}::int[],
        ${b.map((x) => x.endpointId)}::text[],
        ${b.map((x) => x.credId)}::text[],
        ${b.map((x) => x.sourceId)}::text[]
      )
      ON CONFLICT (uri_hash) DO UPDATE SET
        last_seen  = now(),
        seen_count = free_node.seen_count + 1,
        source_id  = EXCLUDED.source_id`;
    n += b.length;
  }
  return n;
}

export async function recordHarvest(
  sourceId: string,
  ok: boolean,
  parsed: number,
  kept: number,
  err: string,
): Promise<void> {
  if (!sql) return;
  await ensureTables();
  await sql`INSERT INTO free_harvest (source_id, ok, parsed, kept, err)
            VALUES (${sourceId}, ${ok}, ${parsed}, ${kept}, ${err.slice(0, 500)})`;
}

function toRow(r: Record<string, unknown>): PoolRow {
  return {
    uriHash: String(r.uri_hash),
    uri: String(r.uri),
    proto: String(r.proto),
    name: String(r.name ?? ""),
    server: String(r.server),
    port: Number(r.port),
    endpointId: String(r.endpoint_id),
    credId: String(r.cred_id),
    sourceId: String(r.source_id),
    firstSeen: String(r.first_seen),
    lastSeen: String(r.last_seen),
    seenCount: Number(r.seen_count),
    check: {
      // LEFT JOIN 没命中时这几列都是 null。checked 落成 0 而不是 null:
      // 下游按"测过几次"筛的时候,0 是个能直接比较的值,null 得每处都先判一次。
      checked: Number(r.chk_n ?? 0),
      ok: Number(r.chk_ok ?? 0),
      // 没测过是 null,**不是 false** —— 混起来的话"没测过"会被当成"测过且不通",
      // 一整池还没轮到的节点会被判死刑。
      lastOk: r.chk_last_ok == null ? null : Boolean(r.chk_last_ok),
      lastTs: r.chk_last_ts == null ? "" : String(r.chk_last_ts),
      medianMs: r.chk_median == null ? null : Number(r.chk_median),
    },
  };
}

/**
 * 取哪一批。
 *
 * - `popular` 反复出现过的优先。给"看池子里都有什么"用。
 * - `stale`   **最久没测的优先,没测过的排最前**。给实测用。
 *
 * `stale` 是必须的,不是可选项:池子是几千条,客户端一轮只测几十到几百条。按 `popular`
 * 取的话每一轮拿到的是**同一批**(排序是确定性的),测七轮等于把同样那批测了七遍,
 * 其余的一次都轮不到 —— 而且从结果上完全看不出来,只会显得"池子里只有这些节点"。
 */
export type PoolOrder = "popular" | "stale";

/**
 * 取节点池。
 *
 * perCred 是**这个函数存在的理由**:每套凭据最多取几条。不限的话,一个源的 CF 扇出
 * (同一套凭据 × 一千多个边缘 IP)会把整个返回集占满,别的源一条都排不进来。
 *
 * 每行都带上它自己的实测战绩(见 CheckStat)。没测过的那些 LEFT JOIN 不命中,
 * checked 是 0 —— 调用方能分得清"测过且不通"和"还没轮到"。
 */
export async function getPool(
  opts: {
    limit?: number;
    perCred?: number;
    protos?: string[];
    freshDays?: number;
    order?: PoolOrder;
  } = {},
): Promise<PoolRow[]> {
  if (!sql) return [];
  await ensureTables();
  const limit = opts.limit ?? 2000;
  const perCred = opts.perCred ?? 3;
  const freshDays = opts.freshDays ?? 7;
  const protos = opts.protos ?? [];
  const stale = opts.order === "stale";

  const rows = await sql`
    WITH chk AS (
      SELECT uri_hash,
             count(*)::int AS n,
             count(*) FILTER (WHERE ok)::int AS n_ok,
             -- 排序用的"多久没碰过它了"取 max(ts):stale 问的是时间。
             max(ts) AS last_ts,
             -- 给人看的那对(结果 + 时间)必须来自**同一行**,都按轮号取最大那条。
             -- 分开取的话(结果按轮、时间按 max(ts)),时间戳乱序时会显示成
             -- "05:29 通了",而 05:29 那条记的其实是不通 —— 一个对不上还查不出的界面。
             -- 用 array_agg 而不是窗口函数,是因为这里已经 GROUP BY 了。
             (array_agg(ok ORDER BY round DESC))[1] AS last_ok,
             to_char((array_agg(ts ORDER BY round DESC))[1], 'MM-DD HH24:MI') AS last_ts_txt,
             percentile_disc(0.5) WITHIN GROUP (ORDER BY latency_ms)
               FILTER (WHERE ok AND latency_ms IS NOT NULL)::int AS median_ms
      FROM free_check GROUP BY uri_hash
    )
    SELECT * FROM (
      SELECT n.*,
             c.n           AS chk_n,
             c.n_ok        AS chk_ok,
             c.last_ok     AS chk_last_ok,
             c.last_ts_txt AS chk_last_ts,
             c.median_ms   AS chk_median,
             c.last_ts     AS chk_last_at,
             row_number() OVER (
               PARTITION BY n.cred_id ORDER BY n.seen_count DESC, n.last_seen DESC
             ) AS rn
      FROM free_node n
      LEFT JOIN chk c ON c.uri_hash = n.uri_hash
      WHERE n.last_seen >= now() - (${freshDays}::int * interval '1 day')
        AND (${protos.length === 0}::boolean OR n.proto = ANY(${protos}::text[]))
    ) t
    WHERE rn <= ${perCred}
    -- stale 为假时第一个排序键对每一行都是 NULL,全部并列,直接落到后面两个键,
    -- 也就是原来的 popular 顺序。写成一条查询而不是两条,免得两边改一处漏一处。
    ORDER BY (CASE WHEN ${stale}::boolean THEN chk_last_at END) ASC NULLS FIRST,
             seen_count DESC, last_seen DESC
    LIMIT ${limit}`;
  return (rows as Record<string, unknown>[]).map(toRow);
}

export interface PoolStats {
  total: number;
  creds: number;
  byProto: Array<{ proto: string; n: number }>;
  bySource: Array<{ source: string; n: number; last: string }>;
  recent: Array<{ ts: string; source: string; ok: boolean; parsed: number; kept: number; err: string }>;
}

export async function poolStats(): Promise<PoolStats | null> {
  if (!sql) return null;
  await ensureTables();
  const total = await sql`SELECT count(*)::int AS n, count(DISTINCT cred_id)::int AS c FROM free_node`;
  const byProto = await sql`SELECT proto, count(*)::int AS n FROM free_node GROUP BY proto ORDER BY n DESC`;
  const bySource = await sql`
    SELECT source_id AS source, count(*)::int AS n, to_char(max(last_seen),'MM-DD HH24:MI') AS last
    FROM free_node GROUP BY source_id ORDER BY n DESC`;
  const recent = await sql`
    SELECT to_char(ts,'MM-DD HH24:MI') AS ts, source_id AS source, ok, parsed, kept, err
    FROM free_harvest ORDER BY ts DESC LIMIT 40`;
  return {
    total: total[0]?.n ?? 0,
    creds: total[0]?.c ?? 0,
    byProto: byProto as PoolStats["byProto"],
    bySource: bySource as PoolStats["bySource"],
    recent: recent as PoolStats["recent"],
  };
}

/**
 * 保留多少轮验证结果。超过的整轮删掉。
 *
 * 定 7 是因为它足够看出趋势(一个节点是"一直好"还是"最近才坏"),又不至于让表
 * 涨到需要操心的量级:池子上限约 16k 条,7 轮就是十来万行 —— 对 Postgres 是小表。
 */
export const CHECK_ROUNDS = 7;

export interface CheckResult {
  uriHash: string;
  ok: boolean;
  /** 通过时的延迟(毫秒)。不通就留空,**不要填 0** —— 0 会被当成"零延迟"参与排序 */
  latencyMs?: number | null;
  err?: string;
}

/**
 * 存一轮验证结果,并把超过 CHECK_ROUNDS 轮的老数据整轮删掉。
 *
 * 轮号用 `max(round)+1`。这里假设**同时只有一轮在跑** —— 验证本身就是客户端串行
 * 拨号,几千个节点跑一轮要很久,并发跑两轮没有意义。真撞上了,主键 (uri_hash, round)
 * 会挡住重复写入而不是把两轮数据混成一轮。
 */
export async function saveChecks(
  results: CheckResult[],
): Promise<{ round: number; saved: number; prunedRounds: number }> {
  if (!sql || results.length === 0) return { round: 0, saved: 0, prunedRounds: 0 };
  await ensureTables();

  const r = await sql`SELECT coalesce(max(round), 0)::int + 1 AS next FROM free_check`;
  const round = r[0]?.next ?? 1;

  let saved = 0;
  for (let i = 0; i < results.length; i += CHUNK) {
    const b = results.slice(i, i + CHUNK);
    // 只写 free_node 里确实存在的那些 —— 客户端手里的池子可能比库里旧,
    // 塞一个已经被 prune 掉的 uri_hash 会撞外键把整批打回。
    const ins = await sql`
      INSERT INTO free_check (uri_hash, round, ok, latency_ms, err)
      SELECT t.h, ${round}::bigint, t.ok, t.ms, t.err
      FROM unnest(
        ${b.map((x) => x.uriHash)}::text[],
        ${b.map((x) => x.ok)}::boolean[],
        ${b.map((x) => (x.ok && x.latencyMs != null ? Math.round(x.latencyMs) : null))}::int[],
        ${b.map((x) => (x.err ?? "").slice(0, 200))}::text[]
      ) AS t(h, ok, ms, err)
      WHERE EXISTS (SELECT 1 FROM free_node n WHERE n.uri_hash = t.h)
      ON CONFLICT (uri_hash, round) DO NOTHING
      RETURNING 1`;
    saved += (ins as unknown[]).length;
  }

  // 整轮删。留 CHECK_ROUNDS 轮,包含刚写的这一轮。
  const del = await sql`
    WITH d AS (DELETE FROM free_check WHERE round <= ${round - CHECK_ROUNDS} RETURNING round)
    SELECT count(DISTINCT round)::int AS n FROM d`;

  return { round, saved, prunedRounds: del[0]?.n ?? 0 };
}

export interface CheckSummary {
  /** 每轮一行,新的在前 */
  rounds: Array<{ round: number; ts: string; total: number; ok: number; medianMs: number | null }>;
  /** 最近一轮里通过的节点数 / 参与的节点数 */
  latestRound: number;
}

/** 每轮的通过率,新的在前。走 GET /free/verify 出去,给客户端界面显示历轮趋势。 */
export async function checkSummary(): Promise<CheckSummary | null> {
  if (!sql) return null;
  await ensureTables();
  const rounds = await sql`
    SELECT round::int AS round,
           to_char(max(ts), 'MM-DD HH24:MI') AS ts,
           count(*)::int AS total,
           count(*) FILTER (WHERE ok)::int AS ok,
           percentile_disc(0.5) WITHIN GROUP (ORDER BY latency_ms)
             FILTER (WHERE ok AND latency_ms IS NOT NULL)::int AS "medianMs"
    FROM free_check GROUP BY round ORDER BY round DESC`;
  const rs = rounds as CheckSummary["rounds"];
  return { rounds: rs, latestRound: rs[0]?.round ?? 0 };
}

/** 清掉很久没再出现的节点。免费源的节点寿命普遍很短,不清的话表会一直涨。 */
export async function prune(keepDays = 30): Promise<number> {
  if (!sql) return 0;
  await ensureTables();
  const r = await sql`
    WITH d AS (DELETE FROM free_node WHERE last_seen < now() - (${keepDays}::int * interval '1 day') RETURNING 1)
    SELECT count(*)::int AS n FROM d`;
  await sql`DELETE FROM free_harvest WHERE ts < now() - interval '90 days'`;
  // free_check 不用在这儿清:外键是 ON DELETE CASCADE,节点没了记录跟着没;
  // 轮数上限由 saveChecks 每次写入时保证。
  return r[0]?.n ?? 0;
}
