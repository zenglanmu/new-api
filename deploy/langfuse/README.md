# new-api + Langfuse v4 (self-hosted)

One `docker-compose.yml` that runs the new-api gateway together with a complete
Langfuse v4 stack on a single host, plus a script that packages everything into
an offline bundle.

## Architecture

```
        browsers / API clients
          |                    |
          v                    v
   :3000 new-api        :3001 langfuse-web  (UI + API + OTLP ingress)
          |                    |
          |                    +--> langfuse-redis  (queue + cache, noeviction)
          |                    +--> clickhouse      (events, 25.12+)
          |                    +--> rustfs :9090   (S3 blobs: events/ + media/)
          |                    +--> postgres        (database "langfuse")
          |                    |
          |                    +--> langfuse-worker (consumes the queue,
          |                                          writes to ClickHouse)
          +--> postgres  (database "new-api")
          +--> redis     (cache, eviction allowed)
          +--> OTLP over the docker network to langfuse-web
```

- `postgres` hosts two databases: `new-api` (created by the image's `POSTGRES_DB`)
  and `langfuse` (created by `initdb/01-create-databases.sql`).
- `redis` (new-api cache) and `langfuse-redis` (Langfuse queue/cache, fixed to
  `maxmemory-policy noeviction`) are separate on purpose: an eviction policy that
  is correct for a cache would silently drop Langfuse queue entries.
- `rustfs` is the S3-compatible object store for Langfuse blobs (observation
  events and media). `create-bucket` runs once with the RustFS `rc` client and
  creates `LANGFUSE_BUCKET` before `langfuse-web`/`langfuse-worker` start, so
  Langfuse never boots against a missing bucket. RustFS's console is bound to
  `127.0.0.1:9091`; the S3 API on `9090` is published for presigned media URLs.
- The gateway sends OTLP to `http://langfuse-web:3000/api/public/otel/v1/traces`
  with `x-langfuse-ingestion-version: 4`. Nothing leaves the docker network.

## Prerequisites

- Docker Engine 24+ with the compose v2 plugin (`docker compose version`).
- 4 CPU, 16 GiB RAM. ClickHouse will use most of the memory under ingest load.
- ~100 GiB free disk for ClickHouse plus image storage. Traces are the largest
  consumer; the stack keeps seven days by default (see Data retention below).
- Ports `3000`, `3001`, and `9090` reachable by users. `9090` is RustFS's S3 API:
  Langfuse hands browsers presigned URLs for media. All other ports bind to
  `127.0.0.1` only, including the RustFS console on `9091`.
- Langfuse v4 requires ClickHouse 25.12+ and PostgreSQL 15+; the compose defaults
  satisfy both. The postgres volume is mounted at `/var/lib/postgresql/data`,
  which is correct for the default `postgres:17-alpine`: if you override
  `POSTGRES_IMAGE` with an 18+ image, mount the volume at `/var/lib/postgresql`
  instead, otherwise the container refuses to start.

## First boot

```bash
cd deploy/langfuse
cp .env.example .env
$EDITOR .env          # replace every CHANGEME value
docker compose up -d
```

If you deploy outside the source tree, use the offline bundle instead (below).

Secret values are quoted in `docker-compose.yml` on purpose: an unquoted
all-digit secret (such as a 64-character hex encryption key made only of zeros)
would be parsed as a YAML integer, and Langfuse rejects it with
`ENCRYPTION_KEY must be 256 bits`. Keep the quotes when you edit that file.

Boot order is enforced by healthchecks: postgres, redis, clickhouse, and rustfs,
then the one-shot `create-bucket`, then `langfuse-web`/`langfuse-worker`, then
`new-api`. Langfuse runs its own Postgres and ClickHouse schema migrations on
start; the first boot can take a few minutes.

RustFS ships with the default credentials `rustfsadmin`/`rustfsadmin`; set a real
`RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY` before the first boot — they are also the
S3 credentials Langfuse and the `create-bucket` step use. The bucket named by
`LANGFUSE_BUCKET` is created by `create-bucket` (`rustfs/rc`), which must exit
successfully before Langfuse starts; `RUSTFS_IMAGE` and `RUSTFS_RC_IMAGE` are
overridable (arch-specific builds are published as `rustfs/rustfs-x86-musl:<tag>`).
The console at `http://127.0.0.1:9091` is optional for operation.

Langfuse v4 ingests traces only through OpenTelemetry: the legacy
`POST /api/public/ingestion` endpoint rejects everything except `score-create`.
The gateway therefore posts OTLP/HTTP JSON to
`/api/public/otel/v1/traces` with `x-langfuse-ingestion-version: 4`.

`initdb/01-create-databases.sql` is executed by the postgres image only when the
data directory is empty. If you point this stack at an existing Postgres volume,
create the `langfuse` database yourself.

Verify:

```bash
docker compose ps
curl -fsS http://localhost:3001/api/public/health
curl -fsS http://localhost:3000/api/status
```

## Logging into Langfuse

There are no default credentials.

- With the shipped headless-init values, the account is
  `LANGFUSE_INIT_USER_EMAIL` / `LANGFUSE_INIT_USER_PASSWORD` from `.env`, and the
  project already has the API keys `LANGFUSE_INIT_PROJECT_PUBLIC_KEY` /
  `LANGFUSE_INIT_PROJECT_SECRET_KEY`. The same key pair is injected into
  `new-api`, so tracing works on the first boot without opening the UI.
- `LANGFUSE_INIT_ORG_ID` and `LANGFUSE_INIT_PROJECT_ID` must both be non-empty:
  when `LANGFUSE_INIT_ORG_ID` is empty, Langfuse ignores every other
  `LANGFUSE_INIT_*` value, creates no project, and OTLP export fails with
  `HTTP 401`.
- With `LANGFUSE_INIT_*` deliberately empty, open `http://localhost:3001` and
  sign up; then create a project and an API key pair in the UI and store them in
  `.env` as `LANGFUSE_INIT_PROJECT_PUBLIC_KEY` / `LANGFUSE_INIT_PROJECT_SECRET_KEY`
  before restarting `new-api`. Keys created in the UI also work for `new-api`
  even though `LANGFUSE_INIT_*` is then only read by the gateway.

Headless init only runs on the first boot of `langfuse-web` against an empty
database. Changing `LANGFUSE_INIT_*` afterwards does not create or modify
anything in Langfuse; after init those variables only feed the gateway's
`LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY`, which is why the same names are
reused above. Rotate real keys in the UI and put the new pair there when you
need to rotate on a running stack.

## Viewing gateway traces

1. Open `http://localhost:3001` and select the project.
2. Open **Tracing → Observations** (or **Tracing → Traces**).
3. Filter by `service.name` / environment `new-api`, or by trace name
   (`langfuse.trace.name`) to isolate gateway requests.
4. A request appears after the exporter flushes; the default batch interval plus
   the worker's ClickHouse write interval means a delay of a few seconds.

`LANGFUSE_SAMPLE_RATE` limits the share of requests that are traced and
`LANGFUSE_CAPTURE_CONTENT` controls whether prompt/response bodies are stored.
When enabled, the generation observation's input is the client request body and
its output is the relayed response body. SSE responses are folded into the final
text, reasoning, and tool calls instead of storing every chunk. Both values are
bounded by `LANGFUSE_CONTENT_MAX_BYTES`.

## Data retention

`LANGFUSE_RETENTION_DAYS` (default `7`, minimum `3`) caps Langfuse data in
ClickHouse. The one-shot `clickhouse-retention` service waits for `langfuse-web`
to finish its schema migrations and then applies a ClickHouse TTL to `traces`,
`observations`, `scores`, `events_full`, and `events_core`. It re-applies the
TTL on every boot, so changing the value and running `docker compose up -d` is
enough. ClickHouse removes expired rows in the background; deletion is not
instant.

Langfuse's own project-level retention (`LANGFUSE_INIT_PROJECT_RETENTION`) only
takes effect with an Enterprise data-retention entitlement. Keep it in sync
with `LANGFUSE_RETENTION_DAYS` if the stack runs an Enterprise license.

ClickHouse TTL removes rows, not objects already written to RustFS. Event and
media blobs survive row expiry, so add an S3 lifecycle rule on the bucket if
you need a hard cap on object storage as well.

## Building and shipping the offline bundle

On a machine with network access and this repository checked out:

```bash
deploy/langfuse/package.sh                    # build image + full bundle
deploy/langfuse/package.sh --help             # options
deploy/langfuse/package.sh --no-save          # compose files only, no images
deploy/langfuse/package.sh --tag new-api:1.2.3 --output /srv/bundles
```

The script validates the compose file, resolves the version (the `VERSION`
environment variable, else a non-empty `VERSION` file, else
`git describe --tags --always` with a leading `v` stripped), builds
`new-api:langfuse-<version>` from the repository's `Dockerfile`, collects every
stack image (pulling missing ones with `docker pull`), and writes
`deploy/langfuse/dist/new-api-langfuse-<version>.tar.gz` containing
`docker-compose.yml`, `.env.example`, `initdb/`, `README.md`, `load.sh`, and
`images.tar`.

`VERSION` is intentionally an empty file in git (CI writes it before building).
For the duration of `docker build`, package.sh writes the resolved version into
it so the Dockerfile can embed it in the Go binary and the frontend, then
restores the original file on exit — including when the build fails.

On the target host:

```bash
tar -xzf new-api-langfuse-<VERSION>.tar.gz -C /opt/new-api-langfuse
cd /opt/new-api-langfuse
./load.sh                 # docker load + create .env (NEW_API_IMAGE pinned)
$EDITOR .env              # replace every CHANGEME value
docker compose up -d
```

`images.tar` is architecture-specific: build the bundle on the same CPU
architecture as the target host. Building the image itself needs the base images
from the repository `Dockerfile` (`oven/bun`, `golang`, `debian`) available
locally or via a registry mirror.

### Behind a registry mirror

If Docker Hub is unreachable, pull through a mirror, retag to the canonical name,
and build as usual (the `Dockerfile` pins base images by digest, so the pull must
resolve that exact digest):

```bash
for img in golang:1.26.10-alpine oven/bun:1.4.4 debian:bookworm-slim \
           postgres:17-alpine clickhouse/clickhouse-server:25.12 \
           langfuse/langfuse:4 langfuse/langfuse-worker:4 \
           redis:7-alpine rustfs/rustfs:latest rustfs/rc:latest; do
  docker pull "docker.1ms.run/$img" && docker tag "docker.1ms.run/$img" "$img"
done
```

Alternatively point every `*_IMAGE` variable in `.env` at the mirror
(`RUSTFS_IMAGE=docker.1ms.run/rustfs/rustfs:latest`, and so on) and run compose
with `--pull never` after the images are present.

## Upgrades

```bash
docker compose pull
docker compose up -d
```

Langfuse applies its own Postgres and ClickHouse migrations at startup; no manual
migration step is required. Pin `LANGFUSE_IMAGE` / `LANGFUSE_WORKER_IMAGE` to the
same exact version and take a Postgres, ClickHouse, and RustFS backup before
upgrading.
ClickHouse cannot be downgraded, so keep the previous image tags available until
the new release has been verified.

## `LANGFUSE_INSECURE=true`

The shipped default (`LANGFUSE_INSECURE=true`) makes the gateway send traces over
plaintext HTTP to `langfuse-web:3000`. That is only acceptable because the
traffic stays inside the docker bridge network and that network is not exposed
beyond the host's published ports.

When Langfuse is reached through a reverse proxy or any host outside this docker
network, set `LANGFUSE_INSECURE=false` and terminate TLS in front of
`langfuse-web`. Anyone who can read that plaintext stream can read the trace
payloads, including prompt content when `LANGFUSE_CAPTURE_CONTENT=true`.
