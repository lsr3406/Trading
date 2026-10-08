# 下一阶段数据接入路线

调研起始日期：2026-10-07；前两项实施更新：2026-10-09。`config/data_sources.yaml` 是工作台可读的优先级与状态清单。**“接口可达”只代表本机完成样本探测，尚不是可靠连续采集。**

## 1. 多资产现货历史：已完成研究样本采集

[Binance 官方公开归档](https://github.com/binance/binance-public-data) 提供按月归档的现货 K 线与校验文件。`config/multi_asset.yaml` 固定 BTC、ETH、SOL、BNB 的 USDT 现货、4 小时频率和 2025-01-01 至 2026-10-01 半开 UTC 窗口。该资产池于 2026-10-08 回溯选定，**只供工程管道和探索性研究，不是历史时点预先确定的可交易成分池**。配置中的 `eligible_from/until` 是本次研究观察窗口，不是交易所上市/退市事实。

`trading multi-collect` 沿用已校验的月度 ZIP 和 SHA-256，生成不可变版本的标准 Parquet、逐资产质量 JSON、含 `observed/missing/outside_eligibility` 状态的时间日历和源档哈希清单。本机真实验收得到 15,312 根 K 线，各资产 3,828 根；逐资产缺失、重复、无效、越界和填充数量均为零。离线测试覆盖时间对齐、因子输入、缺口拒绝与首个上市月不从月初开始的归档。无需用户登录。

```bash
uv sync --locked
uv run trading multi-collect --universe config/multi_asset.yaml
```

下一步若要做无幸存者偏差的横截面策略，须先构建历史时点的交易所现货成分与上市、停牌、退市证据；资产池应在新的未来样本开始前冻结。当前四资产样本不能用于声称历史真实可投资宇宙或独立样本外收益。

## 2. 订单簿：公开频道 60 秒录制与重放已验收

[Coinbase Advanced Trade 公开 WebSocket](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints) 的 `level2` 频道无需 JWT，[消息规范](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/websocket/level2) 说明先发快照，再发绝对数量的增量更新。`trading record-book` 订阅 `level2` 与 `heartbeats`，先落盘原始文本，再重建精确 Decimal 盘口；每次连接是独立快照世代。归一化前 20 档写入分段 Parquet，原始 JSONL 可以离线重放并比较完整盘口 SHA-256。质量报告记录连接中断、序列缺口、检查点和最终回放结果。配置在 `config/coinbase_l2.yaml`。

Advanced Trade 的 `sequence_num` 在本机实测跨盘口消息和订阅回执连续递增，录制器据此检查整个连接的消息序列；heartbeat 的计数器另行检查。盘口要求数量非负、买价低于卖价，断线、超时、序列缺口或盘口异常后重新取快照，失败区间写入 `ok=false` 报告。检查点的 `exchange_timestamp` 是消息封套时间；逐价位的 `event_time` 原样保存在 JSONL，不能混同。旧 Coinbase Exchange Level 2 在本机明确返回“now require authentication”；其旧格式仅保留离线回放与兼容测试，不再作为默认公开采集入口。历史上**未录制**的 Level 2 数据不能由当前快照回填。

```bash
uv run trading record-book --feed config/coinbase_l2.yaml --duration 60
uv run trading book-snapshot --product BTC-USD
```

本机 BTC-USD 60 秒在线录制收到 991 条消息，其中 1 个快照、930 个增量和 58 个 heartbeat；无连接中断或回放错误，完整盘口 SHA-256 与在线状态一致，原始 JSONL 约 11.3 MiB。离线本地 WebSocket 测试覆盖协议、断线与回放。公开 HTTPS [当前盘口接口](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-book) 另提供单次快照，**它不是连续盘口历史**。60 秒验证不能替代长期稳定性测试；需要长期运行主机、磁盘容量计划和中断后的重新取快照机制。

本机 python.org Python 3.13 的默认 OpenSSL `cert.pem` 缺失，造成最初的 `SSLCertVerificationError`。[Python macOS 安装文档](https://docs.python.org/3.13/using/mac.html) 指出安装后需运行证书安装步骤。工程现在对 WSS 显式加载依赖锁定的 `certifi` 根证书，仍要求证书链和主机名验证；如确需额外组织 CA，可用 `SSL_CERT_FILE` 指向可信 PEM 文件叠加。`proxy_mode` 可设为 `direct` 或 `auto`；`auto` 使用系统代理，SOCKS 支持由 `python-socks[asyncio]` 提供。不会关闭 TLS 验证。

## 3. 链上聚合与原始事件：区分观测时间

[DefiLlama 官方 API](https://api-docs.defillama.com/) 的 Ethereum 历史 TVL 接口已在本机返回记录，适合先探索链上活动和流动性指标。但历史日期不等于当时发布/可知时间，且后续可能修订。接入时保存每次原始响应、`observed_at` 和内容哈希；在建立可靠的可知时间之前，只用于描述性分析或保守滞后研究，不直接输入历史回测。

[Etherscan V2](https://docs.etherscan.io/) 更适合在明确链、合约和事件问题后取原始事件。届时需用户申请只读 API key；先做小范围区块回补、链重组处理、区块时间与最终确认策略、去重与速率限制，再考虑因子化。密钥只存环境变量，不进入 YAML 或 Git。

## 4. 宏观外部数据：保留历史版本

[FRED observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) 支持 `realtime_start`、`realtime_end` 和 `vintage_dates`，可用于重建研究时点能看到的利率、流动性数据。[FRED API key 文档](https://fred.stlouisfed.org/docs/api/api_key.html) 要求注册密钥。先确定研究假设和频率，再申请密钥；绝不能直接以今日最新修订值回填过去的决策日。外部数据还应记录发布时间、时区、节假日、修订和缺值策略。

## 共用数据契约与实施顺序

每条原始记录至少带 `source`、`source_timestamp`、`observed_at_utc`、`ingested_at_utc`、资产标识、原始版本哈希与质量状态。规范化表保持原始值与清洗值分离；在研究视图中显式构造 `known_at_utc`，只连接决策时刻已可知的数据。TimescaleDB 用于可重建查询缓存，Parquet/原始响应是可追溯档案。

优先顺序：多资产历史 → 多资产质量与组合/横截面验证 → 订单簿连续录制 → DefiLlama 版本化采集 → Etherscan/FRED（研究问题与密钥明确后）。每一步都先提交离线模拟和异常数据的验收测试，再跑真实公共数据样本；所有候选按资产、参数、数据源合并计入多重比较家族，保留新的未来时间段做最终确认。

## 需要用户线下配合的待办

当前前两阶段无需配合登录。若要进行长期订单簿采集，需要确认本机持续运行或使用稳定主机，并监控磁盘、断线和质量报告。若后续选择 Etherscan 原始事件或 FRED 宏观序列，需要用户分别创建只读 API key，并提供想研究的链/合约事件或宏观主题；密钥由用户填入本地 `.env`，不要发到聊天或提交代码库。
