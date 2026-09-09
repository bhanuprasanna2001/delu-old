# DELU experiments

This document records offline model research. These are retrospective results,
not forecasts that existed before the market outcome was published.

[Project README](README.md) · [Model and data](MODEL_DATA.md)

## Evaluation rules

- Use only features available before the forecast cutoff.
- Split by delivery day in chronological order; never shuffle future days into
  training.
- Compare against unchanged DE-LU EXAA on the same intervals.
- Keep a final holdout separate from fitting and parameter selection.
- Treat predictive accuracy and economic value as different claims. Economic value
  requires a trading rule and costs.

## Weather features

The weather ablation asked whether 125 weather features improve the existing model.
Both candidates were trained through 31 August 2026 with the same holdout procedure.

| Holdout metric | Without weather | With weather | Change |
| :--- | ---: | ---: | ---: |
| Mean absolute error (EUR/MWh) | 8.5751 | 8.5962 | 0.25% worse |
| Interval score (EUR/MWh) | 66.8717 | 64.8303 | 3.05% better |
| Coverage | 90.96% | 91.67% | 0.71 pp higher |
| Mean interval width (EUR/MWh) | 54.03 | 50.50 | 6.53% narrower |

Weather did not improve point accuracy. It did improve interval score and coverage
while narrowing the intervals. Weather remains in the current feature set, but this
evidence supports its interval estimates rather than its point forecast. The
repository records this result but does not yet contain a dedicated weather-ablation
runner; rerun it before making a new production decision.

## Point losses for `SDAC - EXAA`

This comparison changes only the point loss. It uses 31,488 Gold rows across 328
complete days through 31 August 2026, the same 149 features and boosting settings,
three expanding 28-day validation folds, and a final holdout from 4 to 31 August.
Correction shrinkage is fixed at 1 so it cannot hide what each loss learns.

| Point loss | CV MAE | Holdout MAE | MAE gain vs EXAA (95% CI) | RMSE | Direction | Material MAE | Material recall | Beats EXAA |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| EXAA unchanged | 11.081 | 8.808 | +0.000 [+0.000, +0.000] | 13.512 | 0.0% | 19.396 | 0.0% | 0.0% |
| MAE | 10.680 | 8.668 | +0.140 [-0.189, +0.480] | 13.647 | 58.4% | 18.507 | 1.1% | 52.6% |
| MSE | 12.890 | 9.580 | -0.772 [-1.544, -0.080] | 14.535 | 55.5% | 19.415 | 7.7% | 47.1% |
| Huber (delta=10) | 10.906 | 8.917 | -0.110 [-0.446, +0.237] | 13.668 | 57.9% | 18.757 | 4.1% | 51.9% |
| Weighted MAE (abs shift >= 10) | 11.731 | 9.965 | -1.157 [-1.875, -0.431] | 14.782 | 56.1% | 19.047 | 10.8% | 43.0% |

Positive gain is better. The confidence interval resamples whole delivery days.
Material metrics use actual shifts of at least EUR 10/MWh; recall requires a
predicted shift of that size in the correct direction. Weighted MAE gives those
training rows five times the weight. Direction excludes exact zero shifts. Beats
EXAA is the share of intervals with lower absolute error than unchanged EXAA.

### Result

MAE is the best tested loss, but its gain is small and its confidence interval
includes zero. Its average absolute correction is 2.968 EUR/MWh versus an observed
average shift of 8.808 EUR/MWh. MSE and weighted MAE make the final forecast worse.
Changing the loss alone does not solve the EXAA-mimicry problem.

### Reproduce

Use the locked project environment. Refresh the read-only Gold cutoff from an
explicit Databricks CLI profile, then run the comparison:

```bash
uv run python -m delu.ml.loss_experiment \
  --refresh-data \
  --profile "<your Databricks CLI profile>"
```

The script saves the validated snapshot to `data/gold/model_input.parquet` and raw
results to `data/research/loss_comparison.csv`. Both paths are ignored by Git. Rerun
from the frozen snapshot with:

```bash
uv run python -m delu.ml.loss_experiment
```

Implementation: [`src/delu/ml/loss_experiment.py`](src/delu/ml/loss_experiment.py)
