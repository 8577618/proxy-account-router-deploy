# Proxy Account Router

[English](README.md)

Proxy Account Router 使用机场/Clash 订阅节点，给 CPA 和 Sub2API 号池自动分配
独立代理节点。每个账号都会得到一个稳定的本地 SOCKS5 端口，路由器在这个端口背后
处理上游节点选择、健康检查、地区锁定和故障切换。

示例：

```text
socks5://127.0.0.1:19101
```

账号始终使用 `19101` 这个端口；当健康检查或管理员操作需要切换时，路由器只会
更换它背后的上游节点。

## 能做什么

- 为每个账号暴露一个稳定的 SOCKS5 端口。
- 从 Clash 配置文件加载上游节点，并可为这些节点运行托管的 `sing-box` 本地监听。
- 将账号路由状态持久化到 `data/state.json`。
- 保持地区亲和：账号首次选中的地区会被锁定，除非你主动修改。
- 支持严格地区锁、优先地区和 fallback 地区。
- 执行健康检查，并把不健康账号切换到健康候选节点。
- 支持固定外部代理，适合必须禁止自动切换的账号。
- 可选从 CLIProxyAPI auth 文件和 Sub2API PostgreSQL 自动发现账号。
- 提供 status、summary、health、reload、health-check 和账号管理 HTTP API。
- 附带一个 PowerShell 管理脚本，覆盖常用操作。

## 工作方式

```text
client/app -> socks5://proxy-account-router:19101
           -> account state and health policy
           -> managed sing-box node listener, for example 127.0.0.1:19216
           -> upstream proxy node
```

端口区别很重要：

- `19100-19199` 是稳定的账号入口端口。
- `19200-19399` 是托管节点监听端口。故障切换会改变节点，不会改变账号端口。

## 环境要求

- Docker 和 Docker Compose
- Go 1.23 或更新版本，仅本地非 Docker 开发需要
- Clash profile YAML 文件，或兼容当前 `node_source: "clash"` 模式的节点来源
- 可选：CLIProxyAPI auth 目录
- 可选：Sub2API PostgreSQL 数据库访问

Docker 镜像会构建 Go 二进制，并内置 `sing-box`。

## 快速开始

1. 复制示例配置：

```bash
cp config.example.yaml config.yaml
```

2. 编辑 `config.yaml`：

- 将 `clash_profile_paths` 设置为容器内的 Clash 配置文件路径。
- 调整 `docker-compose.yml` 里的挂载路径。
- 手动添加账号，或启用 CLIProxyAPI/Sub2API 的 `auto_sync`。
- 不要把密钥、订阅、真实账号写入 `config.example.yaml`。

3. 启动：

```bash
docker compose up -d --build
```

4. 检查状态：

```bash
curl http://127.0.0.1:19080/healthz
curl http://127.0.0.1:19080/summary
```

5. 使用账号 SOCKS5 端口：

```text
socks5://127.0.0.1:19101
```

同一个 Docker 网络里的容器可以使用：

```text
socks5://proxy-account-router:19101
```

## Docker Compose 说明

内置的 `docker-compose.yml` 尽量保持简单。发布或部署前，请检查这些挂载：

```yaml
volumes:
  - ./config.yaml:/app/config.yaml:ro
  - ./data:/app/data
  - /path/to/clash/profiles:/clash/profiles:ro
  - ../sub2socks5-deploy/data:/sub2socks5/data:ro
  - ../cliproxyapi-deploy/auths:/cliproxyapi/auths
  - ../sub2api-deploy/.env:/sub2api/.env:ro
```

如果不使用 CLIProxyAPI 或 Sub2API，请在 `config.yaml` 中关闭对应配置，并删除
对应挂载。

## 配置

主要字段：

- `listen_host`：SOCKS5 监听绑定地址。
- `clash_profile_paths`：作为上游节点来源的 Clash YAML 文件。
- `managed_sing_box`：为上游节点启用本地 `sing-box` 监听。
- `state_path`：账号路由状态持久化路径。
- `reload_interval`：配置和节点来源重新加载间隔。
- `health_interval`：节点健康检查间隔。
- `health_url`：默认健康检查 URL。
- `allow_region_fallback`：允许非严格账号使用 fallback 地区。
- `region_fallbacks`：全局 fallback 地区顺序。
- `region_aliases`：把 `US`、`JP` 等短标签映射为稳定地区代码。
- `auto_sync`：可选的 CLIProxyAPI/Sub2API 自动发现。
- `accounts`：手动配置的账号列表。

账号示例：

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

## 管理命令

PowerShell helper：

```powershell
.\scripts\router-admin.ps1 -Action summary
.\scripts\router-admin.ps1 -Action status
.\scripts\router-admin.ps1 -Action account -Port 19101
.\scripts\router-admin.ps1 -Action switch -Port 19101
.\scripts\router-admin.ps1 -Action health-check
.\scripts\router-admin.ps1 -Action reload
```

锁定账号到某个地区：

```powershell
.\scripts\router-admin.ps1 -Action lock-region -Port 19101 -Region JPN -Strict
```

允许 fallback 地区：

```powershell
.\scripts\router-admin.ps1 -Action lock-region -Port 19101 -Region SGP -FallbackRegions JPN,USA
```

移除显式地区锁：

```powershell
.\scripts\router-admin.ps1 -Action unlock-region -Port 19101
```

将账号固定到外部代理：

```powershell
.\scripts\router-admin.ps1 -Action set-fixed-proxy -Port 19101 -ProxyUrl socks5://user:pass@203.0.113.10:1080
```

恢复为路由器管理节点：

```powershell
.\scripts\router-admin.ps1 -Action clear-fixed-proxy -Port 19101
```

## HTTP API

只读接口：

```bash
curl http://127.0.0.1:19080/healthz
curl http://127.0.0.1:19080/status
curl http://127.0.0.1:19080/summary
```

管理接口：

```bash
curl -X POST http://127.0.0.1:19080/admin/reload
curl -X POST http://127.0.0.1:19080/admin/health-check
```

账号管理接口位于：

```text
/admin/accounts/{url-escaped-account-name}
```

支持的账号操作：

- `GET /admin/accounts/{name}`
- `POST /admin/accounts/{name}/switch`
- `POST /admin/accounts/{name}/lock-region`
- `POST /admin/accounts/{name}/unlock-region`
- `POST /admin/accounts/{name}/set-fixed-proxy`
- `POST /admin/accounts/{name}/clear-fixed-proxy`

## 开发

运行测试：

```bash
cd src
go test ./...
```

本地构建：

```bash
cd src
go build -o ../proxy-account-router .
```

构建容器：

```bash
docker compose build
```

## 安全注意事项

- 不要提交 `config.yaml`、`data/`、账号邮箱、代理凭据或数据库凭据。
- 除非前面有认证层，否则请把 status/admin 端口绑定到 localhost。
- `fixed_proxy` 凭据会存储在路由器状态里，请保护好 `data/`。
- 管理 API 只适合可信的本地或内网环境使用。

## 许可证

MIT
