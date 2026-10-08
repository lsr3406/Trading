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

## 2. 订单簿：录制与重放已实现，在线连续采集待网络连通

[Coinbase Exchange WebSocket 文档](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview) 列出公开 feed；[level2 频道](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) 给出快照与增量更新。`trading record-book` 订阅公开 `level2` 与 `heartbeat`，先落盘收到的原始文本，再重建精确 Decimal 盘口；每次连接是独立快照世代，断线、超时或盘口异常均记录中断并重新订阅。归一化前 20 档写入分段 Parquet，原始 JSONL 可以离线重放并比较完整盘口 SHA-256。质量报告标明所有中断、检查点和最终重放结果。配置在 `config/coinbase_l2.yaml`。

**Level 2 更新消息未提供可逐条比较的序列号**；heartbeat 的序列号对应更广的产品消息流，不能当作 Level 2 增量的连续序列。这里检查 heartbeat 回退、接收超时、盘口非负且买价低于卖价，并在传输中断后强制重新取快照；报告不会宣称能证明每一条 Level 2 更新都完整。历史上**未录制**的 Level 2 数据不能由当前快照回填。连续录制需要稳定网络和长期运行主机。

```bash
uv run trading record-book --feed config/coinbase_l2.yaml --duration 60
uv run trading book-snapshot --product BTC-USD
```

本机在线试录的 WebSocket 握手受当前网络/代理限制，未取得真实增量消息，失败区间已输出 `ok=false` 质量报告。离线本地 WebSocket 测试验证录制、断线、重建和回放。公开 HTTPS [Level 2 当前盘口接口](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-book) 可达，已保存一份 BTC-USD 原始全量响应、序列号、前 20 档 Parquet 和质量报告；**它只是单次观测，不是连续盘口历史**。本机不需要交易所登录或 API 密钥；在线录制须在允许 `wss://ws-feed.exchange.coinbase.com:443` 的网络重新验收。

`proxy_mode` 可设为 `direct` 或 `auto`（按系统代理自动选择）；当前样例配置为直连，避免本机现有代理的证书握手失败。切换网络时可调整此字段，但不会跳过 TLS 证书校验。

## 3. 链上聚合与原始事件：区分观测时间

[DefiLlama 官方 API](https://api-docs.defillama.com/) 的 Ethereum 历史 TVL 接口已在本机返回记录，适合先探索链上活动和流动性指标。但历史日期不等于当时发布/可知时间，且后续可能修订。接入时保存每次原始响应、`observed_at` 和内容哈希；在建立可靠的可知时间之前，只用于描述性分析或保守滞后研究，不直接输入历史回测。

[Etherscan V2](https://docs.etherscan.io/) 更适合在明确链、合约和事件问题后取原始事件。届时需用户申请只读 API key；先做小范围区块回补、链重组处理、区块时间与最终确认策略、去重与速率限制，再考虑因子化。密钥只存环境变量，不进入 YAML 或 Git。

## 4. 宏观外部数据：保留历史版本

[FRED observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) 支持 `realtime_start`、`realtime_end` 和 `vintage_dates`，可用于重建研究时点能看到的利率、流动性数据。[FRED API key 文档](https://fred.stlouisfed.org/docs/api/api_key.html) 要求注册密钥。先确定研究假设和频率，再申请密钥；绝不能直接以今日最新修订值回填过去的决策日。外部数据还应记录发布时间、时区、节假日、修订和缺值策略。

## 共用数据契约与实施顺序

每条原始记录至少带 `source`、`source_timestamp`、`observed_at_utc`、`ingested_at_utc`、资产标识、原始版本哈希与质量状态。规范化表保持原始值与清洗值分离；在研究视图中显式构造 `known_at_utc`，只连接决策时刻已可知的数据。TimescaleDB 用于可重建查询缓存，Parquet/原始响应是可追溯档案。

优先顺序：多资产历史 → 多资产质量与组合/横截面验证 → 订单簿连续录制 → DefiLlama 版本化采集 → Etherscan/FRED（研究问题与密钥明确后）。每一步都先提交离线模拟和异常数据的验收测试，再跑真实公共数据样本；所有候选按资产、参数、数据源合并计入多重比较家族，保留新的未来时间段做最终确认。

## 需要用户线下配合的待办

当前前两阶段无需配合登录。若要把 Coinbase 连续录制验收为在线可用，需要在允许目标 WebSocket 443 连接且能完成 TLS 握手的网络运行上方 `record-book` 命令；长期录制还需要确认本机持续运行或使用稳定主机。若后续选择 Etherscan 原始事件或 FRED 宏观序列，需要用户分别创建只读 API key，并提供想研究的链/合约事件或宏观主题；密钥由用户填入本地 `.env`，不要发到聊天或提交代码库。
