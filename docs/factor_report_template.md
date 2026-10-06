# Factor study: [name]

## Reproducibility

- Dataset version and SHA-256:
- Source revision and configuration SHA-256:
- Universe, venue, symbols, survivorship and timestamp policy:
- OHLCV availability, missing-data and adjustment policy:
- Fee, slippage, funding and latency calibration:
- All attempted factors, parameters and thresholds (including failed trials):

## Research protocol

- Frozen train/test split, walk-forward windows and embargo:
- Forward-return horizon and minimum cross-sectional asset count:
- Block-bootstrap length, resamples and random seed:
- Bonferroni or BH-FDR family and significance threshold:
- Final untouched holdout date range and one-time evaluation rule:

## Results

| Fold | Training period | OOS period | Selected factor | Adjusted p | OOS IC | OOS IC/IR |
| --- | --- | --- | --- | ---: | ---: | ---: |
| [fill] | | | | | | |

## Execution realism

- Vectorbt screen (training only) with explicit fees and slippage:
- Event-driven paper test with modeled fill, fee, latency and funding:
- Stress tests, capacity, drawdown and abnormal market conditions:
- Paper-run history and operational incidents:

## Decision and limitations

- Decision:
- Remaining uncertainty and rejected hypotheses:
- Next experiment (pre-register before using untouched holdout):
