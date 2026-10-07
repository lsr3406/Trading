# 下一阶段数据接入路线

调研日期：2026-10-07。`config/data_sources.yaml` 是工作台可读的优先级与状态清单。**“接口可达”只代表本机完成样本探测，尚不是可靠连续采集。**

## 1. 多资产现货历史：先做

[Binance 官方公开归档](https://github.com/binance/binance-public-data) 提供按月归档的现货 K 线与校验文件；本机已经取得 `ETHUSDT-4h-2025-01.zip.CHECKSUM`。沿用现有单资产归档校验代码，将 BTC、ETH、SOL、BNB 定义成**事前固定**的研究资产池，并为每个资产记录上市、停牌、退市和实际可交易的起止时刻。即便能下载完整历史，也不能用“今天仍存活的币”回推当年的可选集合。先做逐资产完整性，再按 UTC 四小时网格对齐；缺失、未上市和异常停牌必须分开标注，不能一律前值填充。

交付验收：多资产原始 ZIP 与 SHA-256、标准 Parquet、逐资产质量报告、缺失/上市日历、冻结的成分池配置、跨资产因子时间对齐测试。无需用户登录。

## 2. 订单簿：从录制时刻建立证据

[Coinbase Exchange WebSocket 文档](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview) 列出公开 feed；[level2 频道](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) 给出快照和增量更新。本机已从 Coinbase 公开 HTTP 接口收到 BTC-USD level=1 买卖盘、序列号和时间。下一步建立单独的 WebSocket 录制服务：订阅、保存首帧快照与原始消息、按序列号检测缺口、断线重连后重新取快照，并同时保留交易所时间与本机接收时间。

验收不能只看“消息不断”：每次重建后检查买价小于卖价、盘口非负、序列连续、重放与实时候选状态一致。历史上**未录制**的 level2 数据不能由当前快照回填。持续采集需要本机长时间开机或另设可靠主机；这属于后续部署决策，当前无需交易所账号。

## 3. 链上聚合与原始事件：区分观测时间

[DefiLlama 官方 API](https://api-docs.defillama.com/) 的 Ethereum 历史 TVL 接口已在本机返回记录，适合先探索链上活动和流动性指标。但历史日期不等于当时发布/可知时间，且后续可能修订。接入时保存每次原始响应、`observed_at` 和内容哈希；在建立可靠的可知时间之前，只用于描述性分析或保守滞后研究，不直接输入历史回测。

[Etherscan V2](https://docs.etherscan.io/) 更适合在明确链、合约和事件问题后取原始事件。届时需用户申请只读 API key；先做小范围区块回补、链重组处理、区块时间与最终确认策略、去重与速率限制，再考虑因子化。密钥只存环境变量，不进入 YAML 或 Git。

## 4. 宏观外部数据：保留历史版本

[FRED observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) 支持 `realtime_start`、`realtime_end` 和 `vintage_dates`，可用于重建研究时点能看到的利率、流动性数据。[FRED API key 文档](https://fred.stlouisfed.org/docs/api/api_key.html) 要求注册密钥。先确定研究假设和频率，再申请密钥；绝不能直接以今日最新修订值回填过去的决策日。外部数据还应记录发布时间、时区、节假日、修订和缺值策略。

## 共用数据契约与实施顺序

每条原始记录至少带 `source`、`source_timestamp`、`observed_at_utc`、`ingested_at_utc`、资产标识、原始版本哈希与质量状态。规范化表保持原始值与清洗值分离；在研究视图中显式构造 `known_at_utc`，只连接决策时刻已可知的数据。TimescaleDB 用于可重建查询缓存，Parquet/原始响应是可追溯档案。

优先顺序：多资产历史 → 多资产质量与组合/横截面验证 → 订单簿连续录制 → DefiLlama 版本化采集 → Etherscan/FRED（研究问题与密钥明确后）。每一步都先提交离线模拟和异常数据的验收测试，再跑真实公共数据样本；所有候选按资产、参数、数据源合并计入多重比较家族，保留新的未来时间段做最终确认。

## 需要用户线下配合的待办

当前前两阶段无需配合登录。若后续选择 Etherscan 原始事件或 FRED 宏观序列，需要用户分别创建只读 API key，并提供想研究的链/合约事件或宏观主题；密钥由用户填入本地 `.env`，不要发到聊天或提交代码库。订单簿长期录制前还需要确认本机是否能够持续运行，或允许使用稳定的长期运行主机。
