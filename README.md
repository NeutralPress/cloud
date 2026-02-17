# NeutralPress Cloud

NeutralPress Cloud 是 NeutralPress 实例的中央调度服务（Cloudflare Worker）。

## 功能

- `POST /v1/instances/sync`：实例注册/更新（prebuild 同步）
- `POST /v1/instances/deregister`：实例注销
- `GET /.well-known/jwks.json`：云公钥发布（JWKS）
- `GET /v1/health`：基础健康状态
- 每分钟 Cron 扫描到期实例，写入 Queue
- Queue Consumer 调用实例 `/api/internal/cron/cloud-trigger`
- 接收触发响应中的遥测并写入 D1
- 数据保留：原始 90 天，小时聚合 365 天

## 快速开始

1. 安装依赖

```bash
pnpm install
```

2. 创建 D1 并填入 `wrangler.toml`

```bash
pnpm wrangler d1 create neutralpress-cloud
```

3. 执行初始化 SQL

```bash
pnpm db:migrate:local
# 或
pnpm db:migrate:remote
```

4. 配置秘密（必须）

```bash
pnpm wrangler secret put CLOUD_PRIVATE_KEYS_JSON
pnpm wrangler secret put CLOUD_JWKS_JSON
pnpm wrangler secret put CLOUD_ACTIVE_KID
```

5. 本地开发

```bash
pnpm dev
```

## 密钥格式建议

- `CLOUD_PRIVATE_KEYS_JSON`

```json
{
  "np-cloud-ed25519-2026-01": {
    "kty": "OKP",
    "crv": "Ed25519",
    "x": "<public-base64url>",
    "d": "<private-base64url>",
    "kid": "np-cloud-ed25519-2026-01"
  }
}
```

- `CLOUD_JWKS_JSON`

```json
{
  "keys": [
    {
      "kty": "OKP",
      "crv": "Ed25519",
      "x": "<public-base64url>",
      "kid": "np-cloud-ed25519-2026-01",
      "alg": "EdDSA",
      "use": "sig"
    }
  ]
}
```

## pending URL 判定

以下 URL 会被判定为 `pending_url`（不参与调度）：

- 空值
- `https://example.com`（默认值）
- `localhost / 127.0.0.1 / ::1 / *.localhost / *.local`

## 说明

- 投递成功判定：`HTTP 2xx 且 accepted=true`
- 云端请求实例超时：默认 15 秒（可用 `REQUEST_TIMEOUT_MS` 覆盖）
- 实例签名格式支持：
  - PEM 公钥
  - Base64/Base64URL 公钥
  - DNS TXT 风格：`v=pub1; k=ed25519; p=...`
