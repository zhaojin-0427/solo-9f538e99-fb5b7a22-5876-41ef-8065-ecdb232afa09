# LIS 检验数据入库服务（FHIR R4 transaction）

把 LIS 的检验包（FHIR R4 `transaction` Bundle）并入研究库，供后续分析。服务只接受
**Patient、Specimen、Observation** 三类资源，全部通过一个原子事务写入 SQLite，每个资源
都可以用包内响应返回的**永久地址**直接读取。

## 快速开始

### Docker Compose

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000/docs 演示
```

数据保存在命名卷 `fhir-data`（容器内 `/data/fhir.sqlite3`），重启、重建容器后仍保留。
`FHIR_BASE_URL` 决定写入资源和响应中的永久地址前缀，默认 `http://localhost:8000`；
若服务部署在别的域名/端口，请在 `docker-compose.yml` 中同步修改。

### 本地运行

```bash
python3 -m pip install -r requirements.txt
FHIR_DB_PATH=data/fhir.sqlite3 FHIR_BASE_URL=http://localhost:8000 \
  python3 -m uvicorn main:app --host 0.0.0.0 --port 8000
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/` | 提交 `application/fhir+json` 的 transaction Bundle |
| `GET` | `/{Type}/{id}` | 按永久地址读取 Patient / Specimen / Observation |
| `GET` | `/health` | 健康检查，返回库内资源数 |
| `GET` | `/docs` | Swagger UI（含可直接执行的示例 Bundle） |

### 请求示例

```bash
curl -s -X POST http://localhost:8000/ \
  -H 'content-type: application/fhir+json' \
  --data-binary @examples/demo-bundle.json | python3 -m json.tool
```

成功时返回 `transaction-response` Bundle，`entry[i].response.location` 即该资源的永久地址，
顺序与请求 `entry` 一致。每个响应项也回带 `fullUrl`，便于调用方对齐。

### 规则与实现取舍

* **入口范围**：`Bundle.type` 必须是 `transaction`，最多 **30** 条 entry；每条只接受
  `POST`，资源类型只接受 Patient / Specimen / Observation。其他类型、PUT/DELETE、
  非 `application/fhir+json`（或 `application/json`）一律拒绝。
* **患者身份**：每条 Patient 至少要有一个完整的 `identifier.system` + `identifier.value`。
  只要**任意一个**完整标识在库中已存在（或已被同包前面的 entry 占用），就复用该患者：
  响应为 `200 OK`，`location` 指向已有患者，库内资源**不做任何改写**（不更新人口学信息、
  不追加标识）。同一身份的多个 `fullUrl` 合并到同一个患者。若一个 Patient 的不同标识分别
  命中**不同**的已有患者，视为歧义冲突，整包拒绝。
* **引用**：`reference` 只能是
  * 本包某条 entry 的 `fullUrl`（允许前向引用，处理顺序无关）；或
  * 库内已有资源的永久地址 `<FHIR_BASE_URL>/<Type>/<id>`。

  悬空引用、指向库外地址、类型不符、`fullUrl` 与库内永久地址撞车导致同一字符串有两种解释
  （歧义引用）都会拒绝整包。落库时所有引用改写为永久地址。
* **拓扑约束**：`Specimen.subject` 必须指向 Patient；`Observation.subject` 与
  `Observation.specimen` 所指标本的 subject 必须是**同一患者**（标本既可在包内，也可来自
  库内并沿其已存 subject 追溯）。
* **原子性**：整包在一个 SQLite `BEGIN IMMEDIATE` 事务里完成校验与写入；任何校验失败都会
  `ROLLBACK`，不会留下本包的任何新资源，也不会记录 `Bundle.id`，修正后可用同一 id 重试。
* **幂等**：带 `Bundle.id` 的包，成功处理后记录规范化请求体的 SHA-256 指纹与响应映射。
  同 id 同文重试直接返回**原始响应**（地址不变，不重复建资源）；同 id 异文返回 `409`。
  记录在 SQLite 中，重启后仍然有效。无 `Bundle.id` 的包按普通请求处理，依赖身份匹配去重。
* **并发**：写事务由 SQLite 串行化（WAL + `busy_timeout`，进程内再加一把按数据库的锁）。
  并发提交同一患者不会重复建患者，引用只会绑定到该次事务实际解析到的患者。
* **永久地址**：形如 `http://localhost:8000/Patient/<uuid>`。资源可显式带 `id`
  （合法 FHIR id），否则服务生成 UUID；显式 id 与库内已有资源冲突时返回 `409`，绝不覆盖。

### 状态码

| 状态 | 含义 |
| --- | --- |
| `200` | 整包成功（含幂等重放） |
| `400` | 请求体不是合法 JSON，或不是 JSON 对象 |
| `409` | `Bundle.id` 异文冲突；或请求的 `resource.id` 已存在 |
| `413` / `415` | 请求体过大 / Content-Type 不支持 |
| `422` | 包内容被拒（超 30 项、非 POST、类型不支持、悬空/歧义引用、患者/标本不匹配等） |

错误响应为 `application/fhir+json` 的 `OperationOutcome`，`issue[].expression` 尽量定位到
具体 entry 字段。`422` 已使用 HTTP 语义的标准码，未采用 FHIR 规范中非标准的 `4xx`+`OperationOutcome`
混用写法；客户端应以 HTTP 状态码 + `issue[].code` 判断。

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `FHIR_BASE_URL` | `http://localhost:8000` | 永久地址前缀 |
| `FHIR_DB_PATH` | `data/fhir.sqlite3` | SQLite 文件路径 |
| `FHIR_MAX_BUNDLE_ENTRIES` | `30` | 单包 entry 上限 |
| `FHIR_MAX_BODY_BYTES` | `2000000` | 请求体上限 |
| `FHIR_SQLITE_BUSY_TIMEOUT_MS` | `15000` | SQLite 写锁等待 |

## 测试

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q
python3 -m ruff check .
```

测试覆盖：三类资源正常入库、前向引用、库内地址引用、引用改写、患者按标识复用且不改写、
同身份 fullUrl 合并、身份歧义、悬空/类型错误/歧义引用、标本与结果患者一致性、30 项上限、
非法 Bundle 形状、幂等重放与异文冲突、重启后幂等、多进程与多线程并发唯一患者、失败回滚。
