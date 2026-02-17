#!/usr/bin/env node

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { exportJWK, generateKeyPair } from "jose";

function printHelp() {
  console.log(`用法:
  pnpm gen:env -- [--kid <kid>] [--out-dir <dir>] [--write-dev-vars]

参数:
  --kid <kid>           指定密钥 ID（默认: np-cloud-ed25519-YYYY-MM）
  --out-dir <dir>       输出目录（默认: .generated）
  --write-dev-vars      同时写入项目根目录 .dev.vars
  -h, --help            查看帮助
`);
}

function parseArgs(argv) {
  const options = {
    kid: "",
    outDir: ".generated",
    writeDevVars: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--") {
      continue;
    }
    if (arg === "-h" || arg === "--help") {
      options.help = true;
      continue;
    }
    if (arg === "--kid") {
      options.kid = argv[i + 1] || "";
      i += 1;
      continue;
    }
    if (arg === "--out-dir") {
      options.outDir = argv[i + 1] || "";
      i += 1;
      continue;
    }
    if (arg === "--write-dev-vars") {
      options.writeDevVars = true;
      continue;
    }
    throw new Error(`未知参数: ${arg}`);
  }

  return options;
}

function currentKid() {
  const now = new Date();
  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  return `np-cloud-ed25519-${year}-${month}`;
}

function makeDevVarsText(input) {
  return [
    `# 由 scripts/generate-cloud-env.mjs 自动生成于 ${input.generatedAt}`,
    "# 用于 wrangler dev 的本地 secret",
    `CLOUD_PRIVATE_KEYS_JSON=${input.privateKeysJson}`,
    `CLOUD_JWKS_JSON=${input.publicJwksJson}`,
    `CLOUD_ACTIVE_KID=${input.kid}`,
    "",
  ].join("\n");
}

function assertKeyMaterial(kid, publicJwk, privateJwk) {
  if (publicJwk.kty !== "OKP" || publicJwk.crv !== "Ed25519" || !publicJwk.x) {
    throw new Error("公钥 JWK 生成失败：缺少 Ed25519 所需字段");
  }
  if (
    privateJwk.kty !== "OKP" ||
    privateJwk.crv !== "Ed25519" ||
    !privateJwk.x ||
    !privateJwk.d
  ) {
    throw new Error("私钥 JWK 生成失败：缺少 Ed25519 所需字段");
  }
  if (!kid) {
    throw new Error("kid 不能为空");
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const kid = options.kid || currentKid();
  const outDir = path.resolve(process.cwd(), options.outDir || ".generated");
  const generatedAt = new Date().toISOString();

  const { publicKey, privateKey } = await generateKeyPair("EdDSA", {
    crv: "Ed25519",
  });
  const exportedPublic = await exportJWK(publicKey);
  const exportedPrivate = await exportJWK(privateKey);

  assertKeyMaterial(kid, exportedPublic, exportedPrivate);

  const privateKeyJwk = {
    kty: "OKP",
    crv: "Ed25519",
    x: exportedPrivate.x,
    d: exportedPrivate.d,
    kid,
  };
  const publicKeyJwk = {
    kty: "OKP",
    crv: "Ed25519",
    x: exportedPublic.x,
    kid,
    alg: "EdDSA",
    use: "sig",
  };

  const privateKeysJson = JSON.stringify({
    [kid]: privateKeyJwk,
  });
  const publicJwksJson = JSON.stringify({
    keys: [publicKeyJwk],
  });
  const bulkSecretsJson = {
    CLOUD_PRIVATE_KEYS_JSON: privateKeysJson,
    CLOUD_JWKS_JSON: publicJwksJson,
    CLOUD_ACTIVE_KID: kid,
  };
  const dnsTxtRecord = `v=pub1; k=ed25519; p=${publicKeyJwk.x}`;

  await mkdir(outDir, { recursive: true });
  await Promise.all([
    writeFile(
      path.join(outDir, "cloud-secrets.env"),
      makeDevVarsText({
        generatedAt,
        privateKeysJson,
        publicJwksJson,
        kid,
      }),
      "utf8",
    ),
    writeFile(
      path.join(outDir, "cloud-secrets.bulk.json"),
      `${JSON.stringify(bulkSecretsJson, null, 2)}\n`,
      "utf8",
    ),
    writeFile(
      path.join(outDir, "cloud-public-jwks.json"),
      `${JSON.stringify({ keys: [publicKeyJwk] }, null, 2)}\n`,
      "utf8",
    ),
    writeFile(path.join(outDir, "cloud-public-dns.txt"), `${dnsTxtRecord}\n`, "utf8"),
  ]);

  if (options.writeDevVars) {
    await writeFile(
      path.resolve(process.cwd(), ".dev.vars"),
      makeDevVarsText({
        generatedAt,
        privateKeysJson,
        publicJwksJson,
        kid,
      }),
      "utf8",
    );
  }

  console.log(`已生成环境变量文件:
- ${path.join(outDir, "cloud-secrets.env")}
- ${path.join(outDir, "cloud-secrets.bulk.json")}
- ${path.join(outDir, "cloud-public-jwks.json")}
- ${path.join(outDir, "cloud-public-dns.txt")}
`);
  if (options.writeDevVars) {
    console.log(`已写入本地文件: ${path.resolve(process.cwd(), ".dev.vars")}`);
  }
  console.log(`建议下一步:
1) 本地开发可使用:
   Copy-Item "${path.join(outDir, "cloud-secrets.env")}" ".dev.vars"
2) 远端部署可直接导入:
   pnpm wrangler secret bulk "${path.join(outDir, "cloud-secrets.bulk.json")}"
`);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`生成失败: ${message}`);
  process.exitCode = 1;
});
