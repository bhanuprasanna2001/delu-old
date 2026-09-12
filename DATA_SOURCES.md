# Data sources and attribution

The website provides a dedicated [Data sources & attribution page](https://delu.bhanuprasanna.com/sources).
This reference describes the providers, usage terms, and transformations behind
the charts, model inputs, and downloadable Gold dataset. For field names and
temporal joins, see [MODEL_DATA.md](MODEL_DATA.md).

## Electricity market data

**Source: [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/).**
The platform collects information from transmission system operators, power
exchanges, and other data providers. See ENTSO-E's
[platform description](https://www.entsoe.eu/data/transparency-platform/).

DELU retrieves the following through the ENTSO-E REST API:

| Data | Coverage | Use in DELU |
| :--- | :--- | :--- |
| SDAC auction prices | Germany-Luxembourg and Austria | Germany-Luxembourg prices are the evaluation target and supply 1-, 2-, and 7-day lag features; Austrian SDAC is retained in source history |
| EXAA auction prices | Germany-Luxembourg and Austria | Delivery-day model inputs; Germany-Luxembourg EXAA is the model anchor and evaluation baseline |
| Total load | Germany-Luxembourg | Forecast curves from the previous delivery day and actual curves from two days earlier |
| Solar, onshore wind, offshore wind | Germany-Luxembourg | Forecast and actual curves aligned with the load inputs |

EXAA prices are fetched through ENTSO-E, not a separate EXAA API. Prices use
EUR/MWh; load and generation use MW. DELU derives residual load by subtracting
the three renewable generation series from total load.

ENTSO-E's [Legal Terms and Conditions](https://transparencyplatform.zendesk.com/hc/en-us/articles/40921911218961-Legal-Terms-and-Conditions)
include its current free-reuse list. Listed datasets carry CC BY 4.0; use of
other data follows the platform terms and applicable data-owner conditions.
The Gold download combines several sources and is not assigned a blanket
licence that replaces those conditions.

## Weather data

**Weather data by [Open-Meteo](https://open-meteo.com/), using ECMWF IFS.**
The [Single Runs API](https://open-meteo.com/en/docs/single-runs-api) supplies the
specific `D-1` 00:00 UTC run for delivery day `D`.

- The grid contains 20 land locations and five offshore locations across Germany,
  Luxembourg, the North Sea, and the Baltic Sea.
- Fields are temperature at 2 m, wind speed and direction at 100 m, shortwave
  radiation, and cloud cover.
- `ecmwf_ifs025` from the same run fills a missing primary temperature only.
- DELU aligns shortwave radiation to the start of the preceding hour it represents,
  keeps the hours represented on the delivery day, expands them into quarter-hour
  features, and uses every location separately in the model.
- Website weather charts and table columns show unweighted arithmetic means
  across all 25 locations. Wind direction remains an individual model input;
  it is not averaged into a website weather series.

These selections, expansions, and averages are DELU transformations.
Open-Meteo API data is provided under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), subject to its
[licence](https://open-meteo.com/en/license) and
[API terms](https://open-meteo.com/en/terms).
ECMWF describes the underlying forecast data and attribution requirements on its
[open-data page](https://www.ecmwf.int/en/forecasts/datasets/open-data).

## Holiday data

Contains information from [OpenHolidays API](https://www.openholidaysapi.org/en/),
available under the [Open Database Licence (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/).
Its [FAQ](https://www.openholidaysapi.org/en/faq/) describes the data origins and
licensing.

DELU uses nationwide German public holidays and Luxembourg public holidays.
Validated daily flags, including non-holidays, are cached in Silver and joined by
Gold. Weekday, weekend, month, season, and quarter-hour fields are calculated
locally using the Europe/Berlin calendar.

## DELU transformations and outputs

Source responses are validated and preserved in Bronze. A03 curves are expanded
only inside each published period. Gold first validates the exact physical grid,
then creates a fixed 96-quarter local-day grid. On daylight-saving transitions,
repeated scalar wall-clock quarters are averaged, wind direction uses a circular
mean, and missing spring clock positions are filled from adjacent values. This
normalization is distinct from the physical market's 92- or 100-interval
transition days. Evaluation returns physical-grid metrics and keeps the normalized
96-slot metrics as separate diagnostics.

On the autumn overlap, ENTSO-E's solar and onshore-wind forecast responses have a
verified 96-quarter Period starting one hour after the 100-quarter delivery day.
Silver keeps only those published intervals; Gold fills the four leading model
slots only for those two series.

The downloadable Gold CSV contains this prepared data. Retain the corresponding
source attribution and terms when reusing its columns.

DELU produces the price forecasts, calibrated prediction intervals, and
evaluation metrics. Data-provider attribution does not imply endorsement of
DELU or its results.

Missing data remains pending and is retried on subsequent pipeline runs.
Weather is requested as soon as its selected model run can be retrieved; there
is no Berlin morning release gate. Original published forecasts retain their
values and actual creation timestamps. Forecasts created after the `D-1` 12:00
Europe/Berlin cutoff are labeled retrospective and excluded from operational
rolling monitoring. Once actual SDAC prices are complete, evaluation compares
them with the stored forecast.
