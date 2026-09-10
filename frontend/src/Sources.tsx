import { ArrowLeft } from 'lucide-react'
import { GitHubLink, Logo } from './ui'

export default function Sources() {
  return <main className="detail-page sources-page">
    <title>Data sources & attribution | DELU</title>
    <header className="detail-header">
      <div className="flex items-center gap-5"><a href="/" className="icon-button back-button" aria-label="Back to forecasts"><ArrowLeft size={17} /></a><a href="/" className="shrink-0"><Logo className="brand-logo-small" /></a></div>
      <GitHubLink />
    </header>

    <div className="sources-intro">
      <div className="eyebrow">The data behind the day ahead</div>
      <h1>Data sources & attribution</h1>
      <p>DELU brings together electricity market data, weather forecasts, and holiday calendars. These providers supply the observations and inputs; DELU prepares the data and produces its own price forecasts and evaluations.</p>
    </div>

    <section className="source-section" aria-labelledby="market-source">
      <div><div className="eyebrow">01 · Electricity market</div><h2 id="market-source">ENTSO-E</h2></div>
      <div className="source-copy">
        <p><strong>Source: ENTSO-E Transparency Platform.</strong> Market information is supplied to the platform by transmission system operators, power exchanges, and other data providers.</p>
        <p>DELU retrieves SDAC and EXAA auction prices for Germany-Luxembourg and Austria, plus Germany-Luxembourg load, solar, onshore wind, and offshore wind curves. The EXAA prices are retrieved through ENTSO-E. The Germany-Luxembourg SDAC price is the outcome used to evaluate DELU forecasts.</p>
        <p>For each delivery day, the model uses that day’s EXAA prices, earlier SDAC prices, measured load and generation from two days earlier, and a pinned weather forecast. DELU also collects delivery-day load and generation forecasts, but displays them only when they were actually captured before the 12:00 SDAC cutoff. Those nullable curves are forward research context, not version 2 model features. Prices are expressed in EUR/MWh; load and generation in MW.</p>
        <p>Data use is governed by the ENTSO-E Transparency Platform terms and the applicable data-owner conditions. ENTSO-E’s free-reuse list identifies the datasets covered by CC BY 4.0.</p>
        <div className="source-links"><a className="source-link" href="https://transparency.entsoe.eu/" target="_blank" rel="noreferrer">Transparency Platform</a><a className="source-link" href="https://www.entsoe.eu/data/transparency-platform/" target="_blank" rel="noreferrer">About the data</a><a className="source-link" href="https://eepublicdownloads.entsoe.eu/clean-documents/Transparency/MoP_Ref2_DDD_v3r4.pdf" target="_blank" rel="noreferrer">Data timing definitions</a><a className="source-link" href="https://transparencyplatform.zendesk.com/hc/en-us/articles/40921911218961-Legal-Terms-and-Conditions" target="_blank" rel="noreferrer">Terms & free-reuse list</a></div>
      </div>
    </section>

    <section className="source-section" aria-labelledby="weather-source">
      <div><div className="eyebrow">02 · Weather forecasts</div><h2 id="weather-source">Open-Meteo & ECMWF</h2></div>
      <div className="source-copy">
        <p><strong>Weather data by <a className="source-link" href="https://open-meteo.com/" target="_blank" rel="noreferrer">Open-Meteo</a>, using ECMWF IFS.</strong> DELU uses the Single Runs API to retrieve a specific weather forecast run for 25 locations across Germany, Luxembourg, the North Sea, and the Baltic Sea.</p>
        <p>For a delivery day, the weather comes from the previous day’s 00:00 UTC run and describes the delivery day itself. The inputs include temperature at 2 m, wind speed and direction at 100 m, shortwave radiation, and cloud cover. A missing primary temperature can be filled from the corresponding ECMWF IFS 0.25° run.</p>
        <p>DELU expands hourly values into quarter-hour features. The weather charts and table show arithmetic means across the 25 locations; the model uses each location separately. These are DELU transformations of the supplied forecasts.</p>
        <p>Open-Meteo API data and ECMWF open forecast data are provided under <a className="source-link" href="https://creativecommons.org/licenses/by/4.0/" target="_blank" rel="noreferrer">CC BY 4.0</a>, with their respective terms of use.</p>
        <div className="source-links"><a className="source-link" href="https://open-meteo.com/en/docs/single-runs-api" target="_blank" rel="noreferrer">Single Runs API</a><a className="source-link" href="https://open-meteo.com/en/license" target="_blank" rel="noreferrer">Open-Meteo licence</a><a className="source-link" href="https://open-meteo.com/en/terms" target="_blank" rel="noreferrer">API terms</a><a className="source-link" href="https://www.ecmwf.int/en/forecasts/datasets/open-data" target="_blank" rel="noreferrer">ECMWF open data</a></div>
      </div>
    </section>

    <section className="source-section" aria-labelledby="holiday-source">
      <div><div className="eyebrow">03 · Calendar</div><h2 id="holiday-source">OpenHolidays</h2></div>
      <div className="source-copy">
        <p><strong>Contains information from OpenHolidays API, available under the <a className="source-link" href="https://opendatacommons.org/licenses/odbl/1-0/" target="_blank" rel="noreferrer">Open Database Licence (ODbL)</a>.</strong> DELU uses nationwide German public holidays and Luxembourg public holidays, converting holiday date ranges into daily flags.</p>
        <p>Weekday, weekend, month, season, and quarter-hour fields are calculated by DELU using the Europe/Berlin calendar.</p>
        <div className="source-links"><a className="source-link" href="https://www.openholidaysapi.org/en/" target="_blank" rel="noreferrer">OpenHolidays API</a><a className="source-link" href="https://www.openholidaysapi.org/en/faq/" target="_blank" rel="noreferrer">Data origins & licence</a></div>
      </div>
    </section>

    <section className="source-section" aria-labelledby="delu-processing">
      <div><div className="eyebrow">04 · Processing & results</div><h2 id="delu-processing">Prepared by DELU</h2></div>
      <div className="source-copy">
        <p>DELU aligns source dates, derives residual load, and normalizes each local day to 96 quarter-hour positions. At daylight-saving transitions, repeated quarters are averaged and missing clock positions are filled from adjacent values. The downloadable Gold dataset contains these prepared values.</p>
        <p>The model learns the spread from the 10:15 EXAA auction to the 12:00 SDAC auction and produces a price forecast with a 90% target prediction interval. Forecasts, intervals, and performance metrics are generated by DELU. Attribution does not imply that any data provider endorses the model or its results.</p>
        <p>Missing source data stays pending and is retried automatically. Weather is fetched when its run becomes available. Published forecasts keep their original values and creation timestamps; evaluation follows once the actual SDAC prices are available. Late and reconstructed forecasts remain auditable but do not enter rolling production-performance claims.</p>
        <div className="source-links"><a className="source-link" href="https://github.com/bhanuprasanna2001/delu/blob/main/MODEL_DATA.md" target="_blank" rel="noreferrer">Model & data methodology</a><a className="source-link" href="https://github.com/bhanuprasanna2001/delu/blob/main/DATA_SOURCES.md" target="_blank" rel="noreferrer">Attribution reference</a></div>
      </div>
    </section>

    <footer className="site-footer"><Logo className="brand-logo-tiny" /><a className="source-link" href="/">Back to forecasts</a></footer>
  </main>
}
