# Running kda-indexer behind HAProxy

This page shows how to put kda-indexer's compat API behind an existing HAProxy that already
sends the chainweb-data routes (`/txs/*`, `/stats`, `/coins`) to chainweb-data and `/v1/txs`
to the old wallet-history service, and how to move those routes to kda-indexer **one step at a
time**, each step with a rollback that takes under a minute.

Names in angle brackets are placeholders:

| Placeholder | Meaning |
|---|---|
| `<lb>` | the HAProxy host (the load balancer) |
| `<lb-private-ip>` | the balancer's address on the private LAN, as the indexer hosts see it |
| `<indexer-host-1>`, `<indexer-host-2>` | hosts running kda-indexer |
| `<indexer-1-private-ip>`, `<indexer-2-private-ip>` | their private LAN addresses |
| `<old-host-N>`, `<old-N-private-ip>` | hosts running chainweb-data (`:31352`) and the `/v1/txs` service (`:9876`) |

Nothing here changes chainweb-data. It keeps running and stays the fallback until every route
has moved and you decide to retire it.

## 1. The indexer host

kda-indexer binds loopback by default. Behind a balancer, add the host's private LAN address
to the compat, graphql and admin listeners (each `api.*` key takes one address or an array;
see [install.md](install.md)), and trust `X-Forwarded-For` from the balancer only:

```toml
[api]
native = "127.0.0.1:31361"                                   # internal; not routed by the balancer
compat = ["127.0.0.1:31362", "<indexer-1-private-ip>:31362"]
graphql = ["127.0.0.1:31363", "<indexer-1-private-ip>:31363"]
admin = ["127.0.0.1:31369", "<indexer-1-private-ip>:31369"]  # the balancer's health check
trusted_proxies = ["<lb-private-ip>/32"]
```

- Keep loopback in each list: local tools (the comparison harness, `curl` on the host) keep
  working without going through the LAN address.
- Do not bind `0.0.0.0` on a host with a public interface. Binding the private address only
  keeps the ports off the public network even if a firewall rule is missing.
- `config check` refuses a public address on `native` or `admin`.
- Restart the unit after changing listeners (`systemctl restart kda-indexer-<network>`).

Check on the host:

```sh
ss -ltn | grep -E ':3136[1-9]'                       # every listed address appears
curl -s http://<indexer-1-private-ip>:31369/readyz   # {"ready":true} once caught up
curl -s http://<indexer-1-private-ip>:31362/stats
```

### Health: `/healthz` and `/readyz`

Both are on the admin listener.

- `/healthz` answers `ok` while the process serves.
- `/readyz` answers 200 `{"ready":true}` only when the store opens, the node's tips are
  known, no backfill is running, no chain is halted and every chain is within
  `api.ready_max_lag` blocks (default 20) of the node. Otherwise it answers 503 with a
  `reason`.

The balancer checks `/readyz`, so a lagging, halted, backfilling or restarting indexer leaves
the pool by itself, and the routes fall back to chainweb-data (section 3). One halted chain is
enough to send every moved route back.

### Client IPs: `X-Forwarded-For`

The per-IP rate limit (`api.rate_per_ip`, `api.rate_burst`) keys on the client address. Behind
a balancer every TCP peer is the balancer, so:

- the balancer appends the client address with `option forwardfor`. Do **not** use
  `option forwardfor if-none`: it keeps a header the client sent, so a client could pick its
  own rate-limit bucket;
- kda-indexer reads `X-Forwarded-For` only when the TCP peer is in `api.trusted_proxies`. It
  walks the list from the right and takes the first address that is not a trusted proxy. A
  header from any other peer is ignored, and an unparsable entry falls back to the peer;
- list only the balancer's private address (`/32`), never a whole subnet that clients can
  reach. List the address the balancer connects **from** (check with `ip route get
  <indexer-1-private-ip>`; a `source` directive or a second interface changes it);
- if a CDN or another proxy sits in front of the balancer, the balancer's own peer is that
  proxy, not the client. Then the addresses to trust, and the header the balancer should
  take the client from, are different. Settle this before the first step: a wrong list puts
  every client in one rate-limit bucket (429 for everyone).

## 2. The backend

Add one backend for the compat API. It is shared by every cutover step.

```haproxy
backend kdx_compat
    mode http
    balance leastconn
    option forwardfor
    option httpchk GET /readyz
    http-check expect status 200
    timeout server 15s
    default-server check port 31369 inter 5s fall 3 rise 6 observe layer4 error-limit 3 on-error mark-down
    server kdx1 <indexer-1-private-ip>:31362
```

- `balance leastconn` and no stickiness: compat `next` tokens identify a position in the
  chain, not a server, so a client can page across instances. Check this on your own pair of
  instances before adding the second server (section 6).
- The health check goes to the admin port (`check port 31369`), the traffic to the compat
  port. It only proves the process and the chain position are fine; a wedged compat listener
  with a live admin port stays "up" until connection errors mark it down.
- `rise 6` (30 s of passing checks) keeps a lag that hovers around `api.ready_max_lag` from
  flapping the routes in and out every few seconds; each flip breaks paging tokens.
- `timeout server 15s`: the indexer's own deadline is 5 s (504 past it), so 15 s only catches
  a hung connection.
- `observe layer4 … on-error mark-down` takes a server out after connection errors without
  waiting for three failed checks.

## 3. One step = one ACL and one `use_backend` line

Each step adds a named ACL for its paths and one `use_backend` line to the frontend that
serves these routes today. The first step also adds the shared method ACL:

```haproxy
    # kda-indexer cutover. Keep every kdx line above the existing use_backend lines.
    acl kdx_read method GET HEAD OPTIONS
    acl kdx_step1 path /txs/tx /txs/txs
    use_backend kdx_compat if kdx_read kdx_step1 { nbsrv(kdx_compat) gt 0 }
```

- **Order.** HAProxy takes the first `use_backend` rule that matches, so the kdx lines go
  **before** the existing `use_backend` lines that send `/txs`, `/stats`, `/coins` or `/v1` to
  the old backends, and before `default_backend`.
- **Exact paths.** `path` matches the whole path (without the query string), so `/txs/tx`
  does not match `/txs/txs` or `/txs/tx/`. Use `path_beg` only where the whole subtree is served by the indexer
  (`/v1/txs/…`). A route with exactly one path parameter uses `path_reg` with its shape
  (`^/txs/account/[^/]+$`), so `/txs/account/<a>/` stays on the old backend, as the indexer
  would answer it with a 404.
- **Methods.** Only GET, HEAD and OPTIONS (CORS preflight) move. Anything else keeps going to
  the old backend.
- **Fallback.** `{ nbsrv(kdx_compat) gt 0 }` is true only while at least one indexer passes
  its health check. With none healthy the rule does not match, and the request falls through
  to the existing rule for that path: the chainweb-data backend for `/txs*`, `/stats` and
  `/coins`, the old `/v1` backend for `/v1/txs*`. No manual action is needed for that; the step stays in
  place and resumes when an indexer is healthy again.

The steps, in order. Each one runs a 24 h canary before the next:

| Step | Routes | ACL line(s) |
|---|---|---|
| 1 | `/txs/tx`, `/txs/txs` | `acl kdx_step1 path /txs/tx /txs/txs` |
| 2 | `/txs/account/{account}` | `acl kdx_step2 path_reg ^/txs/account/[^/]+$` |
| 3 | `/v1/txs/…` | `acl kdx_step3 path /v1/txs` and `acl kdx_step3 path_beg /v1/txs/` |
| 4 | `/txs/recent`, `/stats`, `/coins` | `acl kdx_step4 path /txs/recent /stats /coins` |
| 5 | `/txs/events`, `/txs/search` | `acl kdx_step5 path /txs/events /txs/search` |

Each step's `use_backend` line has the same form:

```haproxy
    use_backend kdx_compat if kdx_read kdx_stepN { nbsrv(kdx_compat) gt 0 }
```

Two ACL lines with the same name are ORed (step 3). Step 3 is also a good moment to remove
any server line in the old `/v1` backend that has nothing listening behind it.

## 4. Apply, check, roll back

**Apply** (on `<lb>`):

```sh
cp -a /etc/haproxy/haproxy.cfg /etc/haproxy/haproxy.cfg.before-kdx-stepN
cp /etc/haproxy/haproxy.cfg /tmp/haproxy.cand
# edit /tmp/haproxy.cand: add the step's lines
haproxy -c -f /tmp/haproxy.cand
install -m 644 /tmp/haproxy.cand /etc/haproxy/haproxy.cfg
systemctl reload haproxy
echo "show servers state kdx_compat" | socat stdio /run/haproxy/admin.sock
```

The stats socket path is whatever your `global` section's `stats socket` line says.

A reload is hitless (old workers finish their requests) only when HAProxy runs in
master-worker mode (`-Ws`, the default of the packaged systemd unit since 1.8; check with
`systemctl cat haproxy | grep -- -Ws`). A reload also resets server state: a server that was
down starts as up until its first checks. If that matters, keep it with `server-state-file`
and `load-server-state-from-file global`, saving the state
(`echo "show servers state" | socat stdio /run/haproxy/admin.sock > /var/lib/haproxy/state`)
before each reload.

**Check** at 15 minutes, 1 hour and 24 hours after the reload:

| What | How | Roll back when |
|---|---|---|
| 5xx share on the moved paths | `show stat` on the balancer (`hrsp_5xx` vs the total for `kdx_compat`), or on the indexer `sum(rate(kdx_http_requests_total{api="compat",status=~"5.."}[5m])) / sum(rate(kdx_http_requests_total{api="compat"}[5m]))` | above 0.5% over 5 min, or above the old backend's own baseline |
| Latency per route | `histogram_quantile(0.99, sum by (le, route) (rate(kdx_http_request_seconds_bucket{api="compat"}[1h])))` | p99 above twice the old backend's p99 for that route |
| Answers | the comparison harness with a target at the balancer's public URL, paired with chainweb-data direct, at about 1 r/s | any difference the allowlist does not explain |
| Wallet history | a fixed list of about 20 known accounts, requested through the balancer and compared with chainweb-data direct (`kda-compare once <path>`) | any failure |
| Indexer health | `kdx_chain_lag_blocks`, `kdx_chain_halted`, the unit's restarts | a halted chain or the lag alert |

**Roll back** a step: delete that step's `use_backend` line (its ACL lines may stay), then

```sh
haproxy -c -f /etc/haproxy/haproxy.cfg && systemctl reload haproxy
```

The routes go back to chainweb-data at once. No data is migrated in either direction.
Rolling back step 1 can also remove the `kdx_compat` backend and the `kdx_read` ACL, but only
when no later step is applied (their `use_backend` lines use both); leaving them is harmless.
To undo everything, delete the kdx lines rather than restoring an old copy of the file, so
that unrelated edits made in between are not lost.

**Known transients.** A client that is paging with a `next` token when its route moves (or
falls back) gets an error for that token from the other implementation ("Invalid next token" from
kda-indexer), because the two token formats differ. It restarts from the first page. `offset` paging is not affected.

## 5. Planned restarts of the indexer

A restart closes the listeners for a few seconds. With `observe layer4` the first connection
error marks the server down and the routes fall back, but the request that hit the error fails.
For a planned restart (upgrade, search index rebuild), drain the server first:

```sh
echo "set server kdx_compat/kdx1 state drain" | socat stdio /run/haproxy/admin.sock
# restart the indexer, wait for /readyz = 200
echo "set server kdx_compat/kdx1 state ready" | socat stdio /run/haproxy/admin.sock
```

## 6. A second instance

Install the second host from a snapshot ("A second host from a snapshot" in
[install.md](install.md)), give it the same `[api]` shape with its own private address, and
verify it before it takes traffic:

- a replica pair in the harness (both targets `kda_indexer_compat`) against the first
  instance for a couple of hours with no differences;
- a `next` token taken from one instance pages correctly on the other (this is what makes
  `balance leastconn` without stickiness safe).

Then add it to the backend at a step boundary, not in the middle of a canary:

```haproxy
    server kdx2 <indexer-2-private-ip>:31362
```

Rollback: delete the line and reload, then stop and disable the unit on that host.

## 7. GraphQL

The GraphQL listener (`api.graphql`, `/graphql` for POST, GET and the websocket upgrade) is a
separate port. If clients reach it through the balancer, give it its own backend with the
same health check and a `timeout tunnel` long enough for subscriptions (for example `1h`).
Clients that call it directly are re-pointed in their own configuration; rolling back is
pointing them back.

## 8. Afterwards

Once every route has run on the indexer for a few weeks, the old services can be retired.
Until then, paths that no step moves (and non-GET methods) still reach chainweb-data;
decide what they should get (a 404 from the indexer, or a redirect) before chainweb-data
goes away.

## Appendix: a minimal model for `haproxy -c`

A self-contained file to try the lines above with `haproxy -c -f model.cfg` before touching a
real balancer. The two old backends stand in for whatever your config has.

```haproxy
global
    log stdout format raw local0
    stats socket /tmp/haproxy-model.sock mode 600 level admin

defaults
    mode http
    log global
    option httplog
    timeout connect 5s
    timeout client 60s
    timeout server 60s

frontend public
    bind 127.0.0.1:8080
    # kda-indexer cutover: steps 1 to 5, above the existing rules
    acl kdx_read method GET HEAD OPTIONS
    acl kdx_step1 path /txs/tx /txs/txs
    acl kdx_step2 path_reg ^/txs/account/[^/]+$
    acl kdx_step3 path /v1/txs
    acl kdx_step3 path_beg /v1/txs/
    acl kdx_step4 path /txs/recent /stats /coins
    acl kdx_step5 path /txs/events /txs/search
    use_backend kdx_compat if kdx_read kdx_step1 { nbsrv(kdx_compat) gt 0 }
    use_backend kdx_compat if kdx_read kdx_step2 { nbsrv(kdx_compat) gt 0 }
    use_backend kdx_compat if kdx_read kdx_step3 { nbsrv(kdx_compat) gt 0 }
    use_backend kdx_compat if kdx_read kdx_step4 { nbsrv(kdx_compat) gt 0 }
    use_backend kdx_compat if kdx_read kdx_step5 { nbsrv(kdx_compat) gt 0 }
    # existing rules
    acl old_cwd path_beg /txs /coins /stats
    acl old_v1 path_beg /v1
    use_backend chainweb_data if old_cwd
    use_backend wallet_history if old_v1
    default_backend chainweb_data

backend chainweb_data
    balance source
    server old1 <old-1-private-ip>:31352 check

backend wallet_history
    balance source
    server old1 <old-1-private-ip>:9876

backend kdx_compat
    balance leastconn
    option forwardfor
    option httpchk GET /readyz
    http-check expect status 200
    timeout server 15s
    default-server check port 31369 inter 5s fall 3 rise 6 observe layer4 error-limit 3 on-error mark-down
    server kdx1 <indexer-1-private-ip>:31362
```

Replace the placeholders (for a local try, `127.0.0.1`) before running `haproxy -c`.
