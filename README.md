# Proxy Account Router

[中文文档](README.zh-CN.md)

Proxy Account Router uses nodes from airport/Clash subscriptions to
automatically assign independent proxy nodes to CPA and Sub2API account pools.
Each account gets a stable local SOCKS5 port, while the router handles upstream
node selection, health checks, region locks, and failover behind that port.

Example:

```text
socks5://127.0.0.1:19101
```

The account keeps using port `19101`; the router changes the upstream node
behind it when health checks or admin actions require a switch.

## What It Does

- Exposes one stable SOCKS5 port per account.
- Loads upstream nodes from Clash profile files and can run managed `sing-box`
  local listeners for those nodes.
- Persists account routing state in `data/state.json`.
- Keeps region affinity: an account's first selected region is locked unless
  you change it.
- Supports strict region locks, preferred regions, and fallback regions.
- Runs health checks and switches unhealthy accounts to healthy candidate nodes.
- Supports fixed external proxies for accounts that must never auto-switch.
- Discovers accounts from CLIProxyAPI auth files and Sub2API PostgreSQL when
  enabled.
- Exposes status, summary, health, reload, health-check, and account admin HTTP
  APIs.
- Includes a small PowerShell admin helper for common operations.

## How It Works

```text
client/app -> socks5://proxy-account-router:19101
           -> account state and health policy
           -> managed sing-box node listener, for example 127.0.0.1:19216
           -> upstream proxy node
```

Important port distinction:

- `19100-19199` are stable account entrypoint ports.
- `19200-19399` are managed node listener ports. Failover changes the node, not
  the account port.

## Requirements

- Docker and Docker Compose
- Go 1.23 or newer, only for local development outside Docker
- Clash profile YAML files, or another source compatible with the current
  `node_source: "clash"` mode
- Optional: CLIProxyAPI auth directory
- Optional: Sub2API PostgreSQL database access

The Docker image builds the Go binary and bundles `sing-box`.

## Quick Start

1. Copy the example config:

```bash
cp config.example.yaml config.yaml
```

2. Edit `config.yaml`:

- Set `clash_profile_paths` to your Clash profile files inside the container.
- Adjust mounted paths in `docker-compose.yml`.
- Add accounts manually, or enable `auto_sync` for CLIProxyAPI/Sub2API.
- Keep secrets out of `config.example.yaml`.

3. Start the router:

```bash
docker compose up -d --build
```

4. Check status:

```bash
curl http://127.0.0.1:19080/healthz
curl http://127.0.0.1:19080/summary
```

5. Use an account SOCKS5 port:

```text
socks5://127.0.0.1:19101
```

For containers on the same Docker network:

```text
socks5://proxy-account-router:19101
```

## Docker Compose Notes

The included `docker-compose.yml` is intentionally simple. Before publishing or
deploying, review these mounts:

```yaml
volumes:
  - ./config.yaml:/app/config.yaml:ro
  - ./data:/app/data
  - /path/to/clash/profiles:/clash/profiles:ro
  - ../sub2socks5-deploy/data:/sub2socks5/data:ro
  - ../cliproxyapi-deploy/auths:/cliproxyapi/auths
  - ../sub2api-deploy/.env:/sub2api/.env:ro
```

If you do not use CLIProxyAPI or Sub2API, disable those blocks in
`config.yaml` and remove their mounts.

## Configuration

Main fields:

- `listen_host`: SOCKS5 listener bind address.
- `clash_profile_paths`: Clash YAML files used as upstream node sources.
- `managed_sing_box`: enables local `sing-box` listeners for upstream nodes.
- `state_path`: persisted account routing state.
- `reload_interval`: how often config/node sources are reloaded.
- `health_interval`: how often node health checks run.
- `health_url`: default URL used for health checks.
- `allow_region_fallback`: lets non-strict accounts use fallback regions.
- `region_fallbacks`: global fallback region order.
- `region_aliases`: maps short tags like `US` or `JP` to stable region codes.
- `auto_sync`: optional discovery from CLIProxyAPI/Sub2API.
- `accounts`: manually configured account entries.

Example account:

```yaml
accounts:
  - name: "example-account-1"
    project: "manual"
    listen_port: 19101
    region: "USA"
    strict_region: true

  - name: "example-account-2"
    project: "manual"
    listen_port: 19102
    preferred_regions: ["USA", "JPN", "SGP"]

  - name: "example-fixed-proxy"
    project: "manual"
    listen_port: 19103
    fixed_proxy:
      type: "socks5"
      url: "socks5://user:pass@203.0.113.10:1080"
```

## Admin Commands

PowerShell helper:

```powershell
.\scripts\router-admin.ps1 -Action summary
.\scripts\router-admin.ps1 -Action status
.\scripts\router-admin.ps1 -Action account -Port 19101
.\scripts\router-admin.ps1 -Action switch -Port 19101
.\scripts\router-admin.ps1 -Action health-check
.\scripts\router-admin.ps1 -Action reload
```

Lock an account to a region:

```powershell
.\scripts\router-admin.ps1 -Action lock-region -Port 19101 -Region JPN -Strict
```

Allow fallback regions:

```powershell
.\scripts\router-admin.ps1 -Action lock-region -Port 19101 -Region SGP -FallbackRegions JPN,USA
```

Remove an explicit region lock:

```powershell
.\scripts\router-admin.ps1 -Action unlock-region -Port 19101
```

Pin an account to a fixed proxy:

```powershell
.\scripts\router-admin.ps1 -Action set-fixed-proxy -Port 19101 -ProxyUrl socks5://user:pass@203.0.113.10:1080
```

Return it to router-managed nodes:

```powershell
.\scripts\router-admin.ps1 -Action clear-fixed-proxy -Port 19101
```

## HTTP API

Read-only:

```bash
curl http://127.0.0.1:19080/healthz
curl http://127.0.0.1:19080/status
curl http://127.0.0.1:19080/summary
```

Admin:

```bash
curl -X POST http://127.0.0.1:19080/admin/reload
curl -X POST http://127.0.0.1:19080/admin/health-check
```

Account admin endpoints are under:

```text
/admin/accounts/{url-escaped-account-name}
```

Supported account actions:

- `GET /admin/accounts/{name}`
- `POST /admin/accounts/{name}/switch`
- `POST /admin/accounts/{name}/lock-region`
- `POST /admin/accounts/{name}/unlock-region`
- `POST /admin/accounts/{name}/set-fixed-proxy`
- `POST /admin/accounts/{name}/clear-fixed-proxy`

## Development

Run tests:

```bash
cd src
go test ./...
```

Build locally:

```bash
cd src
go build -o ../proxy-account-router .
```

Build the container:

```bash
docker compose build
```

## Security Notes

- Do not commit `config.yaml`, `data/`, account emails, proxy credentials, or
  database credentials.
- Bind the status/admin port to localhost unless you put authentication in
  front of it.
- `fixed_proxy` credentials are stored in router state, so protect `data/`.
- The admin API is intended for trusted local/internal use.

## License

MIT
