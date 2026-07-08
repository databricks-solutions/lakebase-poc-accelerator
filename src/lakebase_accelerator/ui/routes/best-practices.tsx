import { createFileRoute } from "@tanstack/react-router";
import {
  Database,
  Network,
  Gauge,
  RefreshCw,
  Search,
  Activity,
  KeyRound,
  ShieldCheck,
  FlaskConical,
  type LucideIcon,
} from "lucide-react";

import { PageHeader } from "@/components/page-header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export const Route = createFileRoute("/best-practices")({
  component: BestPracticesPage,
});

interface Practice {
  title: string;
  body: string;
  code?: string;
}

interface Section {
  icon: LucideIcon;
  title: string;
  practices: Practice[];
}

// Curated starting set (from the Lakebase OLTP guide + autoscaling skills).
// Extend / replace as the team's best-practice collection is finalized.
const SECTIONS: Section[] = [
  {
    icon: Database,
    title: "Indexing",
    practices: [
      {
        title: "Index the columns you filter and join on",
        body: "Create B-tree indexes on equality/range predicates in WHERE, JOIN, and ORDER BY. Unindexed OLTP lookups become sequential scans that get dramatically slower as the table grows.",
      },
      {
        title: "Order composite index columns: equality first, then range",
        body: "For multi-column predicates, put equality columns before range/sort columns so the index can be used end-to-end.",
        code: "CREATE INDEX idx_sales_cust_date ON sales (customer_sk, sold_date);",
      },
      {
        title: "Don't over-index",
        body: "Every index slows writes and consumes storage. Drop indexes that pg_stat_user_indexes shows as never scanned after a representative workload.",
      },
      {
        title: "ANALYZE after bulk loads / syncs",
        body: "Refresh planner statistics after large data movements so the planner chooses index scans.",
        code: "ANALYZE schema.table;",
      },
    ],
  },
  {
    icon: Network,
    title: "Connections & pooling",
    practices: [
      {
        title: "Use a connection pooler",
        body: "OLTP apps open many short connections; route them through a pooler so you don't exhaust Postgres backends. Max connections scale with the endpoint's max CU.",
      },
      {
        title: "Set a statement timeout",
        body: "Bound worst-case latency and protect the database from runaway queries.",
        code: "SET statement_timeout = '5s';",
      },
      {
        title: "Keep transactions short",
        body: "Long-running transactions hold locks and bloat MVCC. Commit promptly; avoid idle-in-transaction sessions.",
      },
    ],
  },
  {
    icon: FlaskConical,
    title: "Load testing: psycopg vs pgbench",
    practices: [
      {
        title: "The two tests measure two different ceilings",
        body: "The psycopg test runs from the app backend as a real Python client (thread pool + SQLAlchemy pool, count-based: concurrency × total executions) — it tells you what your application can drive. The pgbench test is submitted as a Databricks Job on a dedicated cluster using the native C pgbench binary (duration-based: clients, threads, seconds) — it tells you Lakebase's server-side headroom. pgbench will report higher QPS and lower latency; that's the efficient client, not a fairer test. Report both.",
      },
      {
        title: "Keep concurrency below the connection limit",
        body: "Every concurrent slot opens a real Postgres backend. Max connections scale with RAM (~209 per Autoscaling CU); a Provisioned CU is ~8× larger, so a provisioned CU_1 (~16 GB) allows ~1,600. Postgres reserves some and each synced table uses up to 16, so treat the usable number as lower. Exceed it and the excess connections are refused — that surfaces as a low success rate, not query errors.",
        code: `Capacity            RAM      ~max_connections
Autoscale 0.5 CU    1 GB     104
Autoscale 1 CU      2 GB     209
Autoscale 8 CU      16 GB    1,678
Provisioned CU_1    ~16 GB   ~1,600   (≈ 8 Autoscaling CU)
16 CU / 32+ CU      32+ GB   3,357 / 4,000 (cap)

Confirm live with:  SHOW max_connections;`,
      },
      {
        title: "Match the psycopg and pgbench knobs",
        body: "Set the same client count in both, keep pgbench threads (-j) at or below the cluster's cores, and choose psycopg total executions so its run lasts about as long as the pgbench duration (concurrency × 200–500).",
        code: `pgbench                psycopg
-c clients        =    concurrency_level   (concurrent connections — set equal)
-j jobs                (none)              keep ≤ cluster cores (8–16)
-T seconds        ≈    total_executions    duration-based vs count-based

total_executions ≈ concurrency × 200–500   → ~30–60 s run, stable percentiles

Suggested sweep (CU_1):
  pgbench  -c 25/50/100/200  -j 8/8/16/16  -T 60
  psycopg  concurrency 25/50/100/200,  total 7.5k/15k/30k/60k`,
      },
      {
        title: "Sweep concurrency to find the knee",
        body: "Raise concurrency in steps (25 → 50 → 100 → 200). Throughput climbs then flattens while p95/p99 keep rising — that flat-QPS / rising-latency point is saturation. Past it, adding workers only adds queuing latency, not throughput.",
      },
      {
        title: "Remember the psycopg client is GIL-bound",
        body: "The Python runner executes under the GIL, so effective parallelism ≈ QPS × avg latency, often far below the level you set (e.g. 10,000 requested can behave like ~250). If success rate stays high but QPS stops rising as you add concurrency, you've hit the client, not Lakebase — trust pgbench for the platform ceiling.",
      },
    ],
  },
  {
    icon: KeyRound,
    title: "OAuth token lifecycle & pooling (Databricks Apps)",
    practices: [
      {
        title: "OAuth tokens are Postgres passwords — and expire at 60 minutes",
        body: "Lakebase uses your Databricks OAuth token as the Postgres password. It has a hard 60-minute expiry that Postgres enforces only at login: already-open connections keep working, but any new connection opened after expiry fails. The classic symptom is an app that works perfectly for an hour, then throws auth errors at minute 61.",
      },
      {
        title: "Own your connection pool — the built-in PgBouncer doesn't support OAuth",
        body: "Lakebase's built-in PgBouncer pooler only works with native Postgres password roles, not OAuth. For an OAuth app you must manage your own pool (e.g. psycopg3's async ConnectionPool) and handle token rotation yourself — the platform won't do it for you.",
      },
      {
        title: "Mint a fresh token on every new physical connection",
        body: "Subclass the psycopg connection and override connect() to mint a fresh token and inject it as the password. Pass it as the pool's connection_class so every newly-opened connection is born with a valid token. This covers new/evicted connections but not ones already open.",
        code: `class OAuthConnection(psycopg.Connection):
    @classmethod
    def connect(cls, conninfo="", **kwargs):
        cred = WorkspaceClient().database.generate_database_credential(...)
        kwargs["password"] = cred.token
        return super().connect(conninfo, **kwargs)

pool = ConnectionPool(conninfo, connection_class=OAuthConnection,
                      min_size=2, max_size=...)`,
      },
      {
        title: "Proactively rotate the whole pool before expiry",
        body: "Run a background watcher for the app's lifetime: ~5 minutes before the token expires, build a second pool with fresh tokens, atomically swap the global pool reference, then close the old pool with wait=True so in-flight requests drain. Result: zero-downtime rotation, no auth errors at minute 61.",
      },
      {
        title: "Don't trust the SDK's token expiry — add a buffer",
        body: "The SDK's validity check has no safety margin and can hand back a nearly-expired token. Decode the token yourself and subtract a 60–300s buffer when scheduling rotation.",
      },
      {
        title: "Size the pool to your users, not the Postgres ceiling",
        body: "Because OAuth bypasses PgBouncer, every pool slot is a real Postgres backend process. Keep min_size small (1–3) so scale-to-zero can still kick in; set max_size to peak concurrent users. Capacity guide: ~1,600 connections at 8 CU, ~3,300 at 16 CU. Also mind the 24-hour idle timeout and 3-day max connection lifetime.",
      },
    ],
  },
  {
    icon: ShieldCheck,
    title: "Caching & concurrent writes (agentic apps)",
    practices: [
      {
        title: "Cache hot read paths with LRU + TTL",
        body: "Repeated lookups of slow-changing data (e.g. thread/session metadata) shouldn't hit the database every request. An LRU+TTL cache with a 3–5 min TTL and 256–1024 entries can cut that load by orders of magnitude. Invalidate after writes; use cachetools.TTLCache when you need per-key invalidation (cache_clear() wipes everything).",
        code: `from cachetools import TTLCache
cache = TTLCache(maxsize=256, ttl=300)  # 5 min
# after a write: cache.pop(key, None)   # surgical invalidation`,
      },
      {
        title: "Make concurrent writes safe with upserts (newer-wins)",
        body: "Parallel agent turns / tool callbacks can write the same key nearly simultaneously; a naive INSERT makes one fail. Use INSERT … ON CONFLICT DO UPDATE with a WHERE clause so only the newer row wins. For retried/at-least-once events, use ON CONFLICT DO NOTHING keyed on an idempotency key.",
        code: `INSERT INTO agent_turns (thread_id, turn_index, message, updated_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (thread_id, turn_index)
DO UPDATE SET message = EXCLUDED.message, updated_at = EXCLUDED.updated_at
WHERE agent_turns.updated_at < EXCLUDED.updated_at;`,
      },
      {
        title: "Declare the unique constraint before you deploy",
        body: "ON CONFLICT requires a pre-existing unique constraint/index on the conflict columns. Without it, unit tests pass (no concurrency) but you get a runtime failure on the first concurrent write in production. Define the constraint explicitly up front.",
      },
    ],
  },
  {
    icon: Gauge,
    title: "Sizing & autoscaling",
    practices: [
      {
        title: "Set min CU to cache your working set",
        body: "Each CU ≈ 2 GB RAM. Size min CU so hot data stays cached; performance is degraded until the compute scales up and warms the cache.",
      },
      {
        title: "Respect the autoscale constraints",
        body: "Autoscaling range is 0.5–32 CU and max − min must not exceed 8 CU. Above 32 CU you need a fixed-size compute (36–112 CU).",
      },
      {
        title: "Use scale-to-zero deliberately",
        body: "Enable scale-to-zero for dev/idle databases to cut cost; disable it for latency-sensitive production to avoid cold-start wake-ups (reactivation resets cache and sessions).",
      },
    ],
  },
  {
    icon: RefreshCw,
    title: "Reverse ETL / synced tables",
    practices: [
      {
        title: "Pick the sync mode by freshness need",
        body: "Snapshot for small/static tables or full refreshes; Triggered for scheduled (hourly/daily) updates; Continuous for real-time (seconds) — at the highest cost.",
      },
      {
        title: "Enable Change Data Feed before Triggered/Continuous",
        body: "Both incremental modes require CDF on the source Delta table. Turn it on before creating the synced table — history is only captured after CDF is enabled.",
        code: "ALTER TABLE catalog.schema.table SET TBLPROPERTIES (delta.enableChangeDataFeed = true);",
      },
      {
        title: "Define a real primary key",
        body: "Synced tables upsert on the primary key. Choose a key that is unique and stable in the source.",
      },
    ],
  },
  {
    icon: Search,
    title: "Query & schema design",
    practices: [
      {
        title: "Always parameterize queries",
        body: "Use placeholders (%s) instead of string-formatting values — it's safer and lets Postgres reuse plans.",
      },
      {
        title: "Select only the columns you need",
        body: "Avoid SELECT *; narrower result sets reduce I/O and can enable index-only scans.",
      },
    ],
  },
  {
    icon: Activity,
    title: "Monitoring",
    practices: [
      {
        title: "Keep cache hit ratio above 99%",
        body: "A low buffer cache hit ratio means too much disk I/O — usually a missing index or undersized min CU.",
      },
      {
        title: "Watch sequential vs index scans",
        body: "Tables doing more seq scans than index scans are candidates for new indexes (pg_stat_user_tables).",
      },
      {
        title: "Find slow queries with pg_stat_statements",
        body: "Use the extension to surface the highest-cost statements, then EXPLAIN (ANALYZE, BUFFERS) them. The Optimize tab automates much of this.",
      },
    ],
  },
];

function BestPracticesPage() {
  return (
    <div>
      <PageHeader
        title="Best Practices"
        description="Curated guidance for running OLTP / reverse-ETL workloads on Lakebase. The Optimize tab automates many of these checks against your live database."
      />
      <div className="space-y-6 p-8">
        {SECTIONS.map((section) => (
          <Card key={section.title}>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <section.icon className="h-4 w-4 text-primary" />
                {section.title}
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              {section.practices.map((p) => (
                <div key={p.title}>
                  <h3 className="text-sm font-medium">{p.title}</h3>
                  <p className="mt-1 text-sm text-muted-foreground">{p.body}</p>
                  {p.code && (
                    <pre className="mt-2 overflow-x-auto rounded-md border bg-muted/30 p-3 text-xs">
                      {p.code}
                    </pre>
                  )}
                </div>
              ))}
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}
