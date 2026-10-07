# Trading

本地量化研究工程。数据与因子研究在 `src/trading/data/`、`src/trading/alpha/` 中运行；模拟执行在 `src/trading/execution/` 中运行。架构测试检查研究与执行的隔离。当前没有实盘连接器，`config/execution.live.yaml` 在加载时直接拒绝。

## 目录

```text
config/                     基础配置、模拟盘与禁用的实盘配置
data/raw|interim|processed/ 本地数据；内容不提交 Git
src/trading/data/           CCXT 公共数据、Parquet/TimescaleDB 缓存、Polars 处理与质量报告
src/trading/alpha/          因子、IC/IR、滚动样本外评估、vectorbt 参数扫描
src/trading/strategy/       组合策略接口
src/trading/backtest/       研究回测边界
src/trading/execution/      Nautilus 模拟引擎、风控路由、监控；没有实盘适配器
src/trading/workbench/      本机 Web 展示层和受控离线研究任务
src/trading/risk/           保守硬性风险限制
src/trading/research/       数据、配置、代码来源清单
research/                  笔记、实验记录和报告；运行输出不提交 Git
tests/                     离线单元与集成测试
docs/                      架构和因子报告模板
```

## 安装与验收

需要 Python 3.13 和 [uv](https://docs.astral.sh/uv/)。在工程根目录运行：

```bash
cd /Users/lixingyao/Workspace/Trading
uv --version
uv sync --locked --all-extras
uv run --all-extras trading doctor
uv run --all-extras pytest -q
uv run --all-extras ruff check .
uv run --all-extras mypy
```

## 本地 Web 工作台

启动 Python 工程即可打开本机研究工作台：

```bash
cd /Users/lixingyao/Workspace/Trading
uv sync --locked --extra research
uv run python -m trading
```

浏览器访问 `http://127.0.0.1:8765`。也可用 `uv run trading workbench --port 8765` 指定端口。页面集中查看数据版本与质量、因子和策略试验、单资产研究结果、模拟执行准备状态，以及[下一阶段数据路线](docs/data_sources_roadmap.md)；可启动固定的采集和研究命令。工作台只监听本机地址，不提供实盘或模拟下单入口。更多设计、运行约定与限制见[工作台说明](docs/workbench.md)。

`uv` 根据 `uv.lock` 建立隔离的 `.venv`。全部测试使用离线模拟数据，不访问交易所，也不需要 TimescaleDB。`research` extra 含 vectorbt；`execution` extra 含固定的 NautilusTrader 2.0.0rc6。日常只做数据处理时可用 `uv sync --locked`。绘图依赖 Plotly 固定在 5.x，以兼容当前 vectorbt。

本机已通过官方安装脚本将 uv 0.12.23 放在 `~/.local/bin/uv`，并将此目录加入 `~/.zshrc` 的 `PATH`。新终端可直接运行 `uv`；当前终端如未刷新，请运行 `source ~/.zshrc`。

## 首个可复现的单资产研究

`config/single_asset.yaml` 固定了 BTC/USDT 现货、4 小时周期、2025-01 至 2026-09 共 21 个完整 UTC 月份。数据来自 [Binance 官方公开月度归档](https://github.com/binance/binance-public-data)，每月 ZIP 都校验随附 SHA-256。无需交易所账户或 API 密钥，重复运行会复用本地已校验原始归档。CCXT 的实时公共接口仍保留供后续增量采集；首次历史研究不依赖其网络可用性。

```bash
cd /Users/lixingyao/Workspace/Trading
uv sync --locked --extra research
uv run --extra research trading single-study --study config/single_asset.yaml
```

命令依次下载和校验原始档案、生成版本化 Parquet、做完整性检查、计算预先固定的 60 根动量规则，并使用 vectorbt 跑滚动样本外及最终 20% 保留集。结果在 `research/runs/single-asset-*/report.md` 与 `report.json`；数据质量和原始归档 SHA 清单在 `research/reports/`。`data/` 和运行结果默认不进 Git；需要归档时连同源代码版本、`uv.lock`、配置和原始 ZIP 一起保存。

归档时间戳表示 K 线的 **UTC 开始时刻**。因子在 4 小时后收盘才可得，交易在下一根 K 线收盘按收盘价模拟成交；每侧另计 10 bps 手续费和 10 bps 滑点，期初 10,000 USDT，单笔买入 100 USDT，无杠杆。此成本是研究假设，不是账户实际费率；完整 K 线延迟比配置中的 1 秒延迟更保守。策略与同额买入持有均在每个评估区间末尾清仓。报告附区块 bootstrap 区间、时间序列 Spearman IC、成本和数据局限。此命令只运行离线研究，不提交订单，也不能证明实盘可行性。

## 因子与策略目录研究

在相同版本的 BTC/USDT 数据上，可以批量计算 112 个因子并评估 `config/factor_strategy.yaml` 中预声明的 23 个多头/现金策略：

```bash
uv run --extra research trading catalog-study \
  --study config/single_asset.yaml --catalog config/factor_strategy.yaml
```

目录使用 Polars 计算已完成 K 线的滞后因子，用 vectorbt 批量计入手续费和滑点。最后 20% 历史数据不参与候选评估；开发期策略和因子分别记录分块 IC、样本外净收益及 Bonferroni 校正结果。此前单规则研究已看过同一时期的结果，因此它不再是全新保留集。新目录只能提供探索性证据；独立确认必须等待未来数据。详见 [因子与策略目录设计](docs/factor_strategy_catalog.md)。

## Data Factory

`CcxtPublicSource` 使用 CCXT 公共接口，按交易所能力检查并分页获取 OHLCV、订单簿快照和历史资金费率。`MultiExchangeCollector` 保留交易所标识；`DataFactory` 先存原始响应快照和请求元数据，再运行质量检查、清洗和缓存。`ParquetCache` 是本地规范化缓存，`TimescaleCache` 是可选查询缓存，需提供 `TIMESCALE_DSN` 和已安装的 TimescaleDB。数据库未启动时不会尝试连接。

Polars 处理函数提供 UTC 网格对齐、显式缺失值策略、逐根 K 线复权因子与可扩展的滞后特征管道。复权因子必须带 `known_at`，晚于对应 K 线的因子会被拒绝。质量报告包含缺口、重复、无效值和越界 K 线计数；`ok=false` 要求研究者处理或记录问题。订单簿接口只取得当前快照，不提供历史深度回放。

接入真实公共数据时，在 Python 中实例化 `CcxtPublicSource(exchange_id)`，再交给 `DataFactory`；交易所、交易对、UTC 窗口和抓取上限由调用方配置。先用测试中的假交易所验证流程，再决定真实数据请求。原始快照与处理后缓存均需在研究清单里记录哈希。

对已有规范化 Parquet 文件生成质量报告：

```bash
uv run trading quality --kind ohlcv --data data/processed/example.parquet \
  --start 2025-01-01T00:00:00+00:00 --end 2025-02-01T00:00:00+00:00 \
  --interval 1h --output research/reports/quality.json
```

发现问题时退出码为 2，JSON 仍会保存，供研究者审查。

## Alpha Factory

`Factor` 接口和 `FactorBatch` 批量计算滞后动量/波动率；计算要求完整的市场时间网格。`ic_series` 计算每个时点的横截面 Spearman IC，`run_factor_study` 先预留最终 holdout，再做带 embargo 的滚动训练/测试。训练期以区块 bootstrap 得到筛选 p 值，并对完整候选集合执行 Bonferroni 或 BH-FDR 校正。最终 holdout 只评估开发阶段冻结的因子。`docs/factor_report_template.md` 给出研究报告字段。

`scan_thresholds` 把单市场因子列交给 vectorbt 批量扫描，显式传入手续费和滑点。它只适合训练窗口初筛；非零资金费率或延迟会被拒绝，因为该路径未建模这些成本。每个阈值都算一次试验，不能在测试集或最终 holdout 上挑参数。

## 模拟执行与风控

`build_paper_engine` 只创建 NautilusTrader `BacktestEngine`，按模拟配置设置随机种子、限价成交概率、滑点概率、maker/taker 费用及延迟。默认模拟配置中的成本与风险参数为 `null`，必须先用经记录的假设填齐才能创建引擎。`NautilusPaperRouter` 在提交原生限价单之前调用 `RealtimeRiskMiddleware`；它检查行情新鲜度、价差、价格偏离和单品种仓位，再调用组合层的最大回撤、单日亏损、订单金额、杠杆与快照新鲜度检查。订单状态由 NautilusTrader 管理，`ExecutionMonitor` 提供结构化日志和本地 Prometheus 指标。

策略接入时必须通过该路由提交订单，并从引擎状态构建每次请求的组合及行情快照。目前没有现成策略、账户对账或持续运行的模拟盘服务；这里的模拟引擎适用于可重复的离线实验。实盘模式无可用授权路径。进入实盘前还需至少 3–6 个月有代表性的模拟验证、独立账户、对账、紧急停机和单独的设计审查。

## 开发工作流

1. 在 `config/base.yaml` 或被 Git 忽略的 `.env` 中设置研究数据版本、成本和风险参数。环境变量格式为 `TRADING__SECTION__FIELD`；进程环境优先于 `.env`，再优先于 YAML。密钥仅放本地环境或系统凭据存储，绝不提交。
2. 抓取时归档原始响应、质量报告与不可变数据版本。TimescaleDB 只作可重建缓存。
3. 在训练集定义完整候选因子/阈值，固定随机种子、样本外窗口和多重检验方法。
4. 在独立模拟引擎评估费用、滑点、延迟和极端情境；记录未建模的资金费率等成本。
5. 记录数据、配置、代码来源：

   ```bash
   uv run trading manifest --data data/processed/example.parquet \
     --output research/runs/example/manifest.json
   ```

   此命令拒绝尚未校准的占位配置。归档数据、代码版本、配置、报告和清单。
6. 运行上方测试、代码检查和类型检查，再提交 `pyproject.toml` 与 `uv.lock`。

详见 [架构说明](docs/architecture.md)。
