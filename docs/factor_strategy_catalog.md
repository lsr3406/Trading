# OHLCV 因子与策略目录

## 架构

`alpha/catalog.py` 在现有 `Factor` / `FactorBatch` 接口上注册 21 个滚动因子族 × 5 个窗口，以及 7 个 K 线形态因子，总计 112 列。全部用 Polars 按交易所、交易对和周期分组计算。`research/strategies.py` 声明 23 个参数固定的多头/现金策略；`research/catalog_study.py` 将同一完整数据版本送入 vectorbt 批量回测，并分别对因子和策略候选族做多重检验。研究层只产生信号和报告，不可调用实盘执行。

目录受 [Qlib Alpha158 的公式化特征族](https://github.com/microsoft/qlib/blob/main/qlib/contrib/data/loader.py)和 [awesome-quant 的技术指标分类](https://github.com/wilsonfreitas/awesome-quant)启发。这里**不是** Qlib Alpha158 的逐列复制：Qlib 的默认数据、VWAP 和股票横截面工作流与本工程当前的 BTC/USDT 四小时现货数据不同。多候选成本回测使用 [vectorbt 的 `Portfolio.from_signals`](https://vectorbt.dev/api/portfolio/base/)。

## 已接入因子

滚动窗口固定为 6、12、24、60、120 根；每个窗口都计算以下 21 族：

| 类别 | 因子族 | 含义 |
|---|---|---|
| 趋势 | `momentum`, `log_momentum`, `ma_gap`, `ema_gap`, `efficiency_ratio` | 过去收益、均线偏离与路径效率 |
| 反转与区间 | `donchian_position`, `bollinger_z`, `cutler_rsi`, `breakout_distance`, `pullback_distance`, `typical_price_gap` | 价格相对区间、波动带和过去极值的位置 |
| 波动与风险 | `return_volatility`, `downside_volatility`, `price_range`, `parkinson_volatility` | 历史收益、下行及高低价波动 |
| 成交量与流动性代理 | `volume_ratio`, `volume_volatility`, `dollar_volume`, `volume_return_correlation`, `amihud_proxy`, `flow_proxy` | 成交量变化、量价关系及仅由 OHLCV 推算的流动性代理 |

另外计算 7 个单根 K 线形态：`bar_body`、`bar_range`、`upper_wick`、`lower_wick`、`close_location`、`true_range`、`open_gap`。`cutler_rsi` 使用简单滚动和，不等同于 Wilder 平滑的 RSI。`flow_proxy` 通过收盘位置加权成交量计算，**不是** 买卖盘失衡。`amihud_proxy` 使用绝对收益与估计成交额，**不是** 实际冲击成本。高低价波动的 Parkinson 估计在跳空或微观结构噪声下可能偏离真实风险。

每一列在输入 K 线的 UTC 开始时间 `t` 上，只使用 `t` 之前已完成的 K 线。当前 K 线的任何 OHLCV 变化都不会改变该行因子。暖机期为 `null`，无效分母产生的非有限值也保留为 `null`，不会用未来数据填充。缺失或合成 K 线直接拒绝进入目录研究。

## 已接入策略

`config/factor_strategy.yaml` 预先列出全部 23 个候选，覆盖时间序列动量、短期反转、简单/指数均线趋势与交叉、布林带反转与突破、Cutler RSI 反转、唐奇安突破与区间反转、量能确认、低波动过滤、K 线实体延续和宽幅收盘突破。每组窗口和阈值均算作独立假设。策略只读上述滞后因子，输出入场与离场布尔信号；模拟订单在该行 K 线收盘成交，因此信号距所用观测至少一根完整 K 线。每折末尾按同样成本清仓。

统一假设为期初 10,000 USDT、单次买入名义金额 100 USDT、每侧手续费 10 bps、滑点 10 bps、无杠杆、不做空。此滑点是压力假设，不是由订单簿实测。vectorbt 批量评估仅用于研究；模拟盘与实盘继续使用独立的执行与风控边界。

## 验证协议

保留最后 20% 历史数据不参与目录研究；之前的单规则动量研究已经看过同一时期，因此这段数据**不是新的独立保留集**。开发期使用 1,080 根训练、1 根隔离、最多 540 根测试的滚动窗口。所有策略在相同窗口与成本下评估，以同额买入持有为基准。策略用连续 42 根 K 线的循环区块 bootstrap 检验样本外平均超额逐根收益；因子用不跨折的 42 根分块 Spearman IC、ICIR 和三块循环 bootstrap。每个候选族分别做 Bonferroni 校正。只有调整后 `p <= 0.05` 且平均超额收益为正，才可选出开发期策略；没有合格候选时结果必须为 `null`。

4,999 次重采样使 112 个因子的 Bonferroni 最小可分辨调整后 p 值约为 0.0224。高度相关的因子会带来大量重复信息；单纯增加因子数不会增加独立证据。所有结果仅为探索性证据，需等待后续未见过的完整月份做前瞻确认。

## 运行与产物

```bash
cd /Users/lixingyao/Workspace/Trading
uv sync --locked --extra research
uv run --extra research trading catalog-study \
  --study config/single_asset.yaml --catalog config/factor_strategy.yaml
```

版本化因子矩阵保存在 `data/interim/factor-catalog-*.parquet`；报告保存在 `research/runs/catalog-*/report.md` 和 `report.json`。报告记录原始数据、因子矩阵、配置和代码/锁文件哈希，列出所有因子与策略及其原始/校正 p 值。原始数据和运行产物默认不提交 Git。

## 下一阶段的数据需求

| 方向 | 需要先增加的数据 | 可研究的因子/策略 |
|---|---|---|
| 多资产 | 同步、可交易的现货池与历史成分 | 横截面动量、低波动、风险平价、分层组合、配对交易 |
| 订单簿 | 连续 L2 快照或逐笔成交，精确时间戳 | 价差、深度、盘口失衡、成交冲击、做市与短周期执行 |
| 永续合约 | 资金费率、持仓量、标记价和基差 | 期限结构、资金费率套利、拥挤度；需额外清算风控 |
| 链上与外部 | 有可验证发布时间的链上、宏观或情绪数据 | 流入流出、活跃度、事件研究；必须控制发布时间泄漏 |

这些方向不能从当前单一 BTC/USDT OHLCV 文件可靠推断，因此尚未伪造其因子值或回测收益。
