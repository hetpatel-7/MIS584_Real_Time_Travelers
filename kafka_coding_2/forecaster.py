
import pandas as pd
from statsforecast import StatsForecast
from statsforecast.models import AutoETS
import statsmodels.api as sm

import matplotlib
import matplotlib.dates as matplotdates
import matplotlib.ticker as ticker
from functools import partial, reduce
# From FPP3 book (The Pythonic Way)
import warnings
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message=".*FigureCanvasAgg is non-interactive.*"
)
import os
os.environ["NIXTLA_ID_AS_COL"] = "true"
import numpy as np
np.set_printoptions(suppress=True)
np.random.seed(1)
import random
random.seed(1)
import pandas as pd
pd.set_option("max_colwidth", 100)
pd.set_option("display.precision", 3)
from utilsforecast.plotting import plot_series as plot_series_utils
import seaborn as sns
sns.set_style("whitegrid")
import matplotlib.pyplot as plt
plt.style.use("ggplot")
plt.rcParams.update({
    "figure.figsize": (8, 5),
    "figure.dpi": 100,
    "savefig.dpi": 300,
    "figure.constrained_layout.use": True,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "legend.fontsize": 9,
    "legend.title_fontsize": 10,
    "grid.alpha": 1.0,
})
import matplotlib as mpl
from cycler import cycler
mpl.rcParams['axes.prop_cycle'] = cycler(color=["#000000", "#000000"])
from fpppy.utils import plot_series
import matplotlib.dates as mdates


mpl.colormaps.register(
    mpl.colors.ListedColormap(
        ["#000000", "#2f2fff"], name="black_and_blue"),
    force=True,
)
mpl.colormaps.register(
    mpl.colors.ListedColormap(
        ["#000000", "#D55E00"], name="black_and_orange"),
    force=True,
)
mpl.colormaps.register(
    mpl.colors.ListedColormap(
        ["#000000", "#000000"], name="black"),
    force=True,
)
mpl.colormaps.register(
    mpl.colors.ListedColormap(
        ["#000000", "#569CC6", "#D55F03"],
        name='black_and_2color',
    ),
    force=True
)
mpl.colormaps.register(
    mpl.colors.ListedColormap(
        ["#000000", "#D55F03", "#569CC6", "#13A076"],
        name='black_and_3color',
    ),
    force=True
)
mpl.colormaps.register(
    mpl.colors.ListedColormap(
        ["#000000", "#D55F03", "#569CC6", "#13A076", "#CC79A7"],
        name='black_and_4color',
    ),
    force=True
)
mpl.colormaps.register(
    mpl.colors.ListedColormap(
        ["#D55F03", "#569CC6", "#13A076", "#CC79A7"],
        name='r_colors',
    ),
    force=True
)

matplotlib.use('Agg')

import statsmodels.api as sm
from matplotlib.ticker import MaxNLocator
from prophet import Prophet
from statsforecast import StatsForecast
from statsforecast.adapters.prophet import AutoARIMAProphet
from statsforecast.models import MSTL, AutoETS, AutoARIMA, ARIMA
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.tsa.api import VAR
from utilsforecast.evaluation import evaluate
from utilsforecast.feature_engineering import trend, fourier, pipeline
from utilsforecast.losses import rmse, mae, mape, mase, smape
from utilsforecast.preprocessing import fill_gaps
import time
import holidays

class Forecaster():
    def __init__(self, start_date):
        self.prediction_start_date = start_date
        ptc_weather = pd.read_csv("./data/ptc_weather.csv")
        ptc_weather["ds"] = pd.to_datetime(ptc_weather["ds"])
        self.prepare_2022_data(ptc_weather)
        print("FC: PREPARED 2022 DATA")
        self.prepare_2023_data(ptc_weather)
        print("FC: PREPARED 2023 DATA")
        pass

    def prepare_2022_data(self, ptc_weather):
        ee_holidays = holidays.Estonia(years=[2022])
        ptc_weather_2022 = ptc_weather[ptc_weather["year"] == 2022].copy()
        # Here we set the holidays
        ptc_weather_2022["is_holiday"] = ptc_weather_2022["ds"].dt.date.map(
            lambda x : 1 if x in ee_holidays else 0
        )
        # Okay, now let's drop whatever we don't need
        self.ptc_weather_2022 = ptc_weather_2022.drop(
            columns=[
                'year', 'month', 'day', 'hour', 'in_sum', 'out_sum', #'day_of_week',
            ]
        )
        # self.window_size = len(self.ptc_weather_2022) // 2  ← wrong: used full 2022 data and halved it
        # Notebook approach: filter to last 5 months only, use full 5 months as window (~3694 rows)
        self.ptc_weather_2022 = self.ptc_weather_2022[self.ptc_weather_2022["ds"] > "2022-07-31"]
        self.window_size = len(self.ptc_weather_2022)
        self.hour_window = 8

    def prepare_2023_data(self, ptc_weather):
        """ Loads in all data that's needed for forecasts
        """
        ee_holidays = holidays.Estonia(years=[2023])
        ptc_weather_2023_dev = ptc_weather[ptc_weather["year"] == 2023].copy()
        self.ptc_weather_2023_bkp = ptc_weather_2023_dev[["ds", "unique_id", "y"]].copy()
        ptc_weather_2023_dev["is_holiday"] = ptc_weather_2023_dev["ds"].dt.date.map(
            lambda x : 1 if x in ee_holidays else 0
        )
        self.ptc_weather_2023_dev = ptc_weather_2023_dev.drop(
            columns=[
                "year", "month", "day", "hour",
                "in_sum", "out_sum", "y" #, "day_of_week"
            ]
        )
        self.ptc_weather_2023_ground_truth = pd.DataFrame({
            "ds":[],
            "unique_id":[],
            "day_of_week":[],
            "temp_mean":[],
            "humidity_mean":[],
            "precipitation_mean":[],
            "rain_mean":[],
            "snowfall_mean":[],
            "snow_depth_mean":[],
            "wind_speed_mean":[],
            "cloud_cover_mean":[],
            "cloud_cover_low_mean":[],
            "cloud_cover_mid_mean":[],
            "cloud_cover_high_mean":[],
            "is_holiday":[]
        })

    def generate_plots(self, pass_gt, moving_forecasts):

        fig, ax = plt.subplots(figsize=(16, 5))
        plot_series(
            df=pass_gt, #I want the year 2022's data to be in black
            forecasts_df=moving_forecasts, #The forecasts we want to show
            models=["MSTL"],
            level=[80, 95],
            max_insample_length=12,
            xlabel="DateTime [1h]", ylabel="People Count",
            title="Hourly foot traffic in Tallinn (MSTL(d, w) + AutoARIMA + Exogenous Integration)",
            palette="black_and_blue", rm_legend=False, ax=ax)
        ax.xaxis.set_major_locator(mdates.HourLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

        if len(self.ptc_weather_2023_ground_truth) < 1:
            print("No data found. Plotting on backup!")
            com_data = self.ptc_weather_2023_bkp.head(self.hour_window)
        else:
            print(f"Sensor data ready to print {len(self.ptc_weather_2023_ground_truth)}")
            com_data = self.ptc_weather_2023_ground_truth.head(self.hour_window)

        ax.plot(
            com_data["ds"],
            com_data["y"],
            color="red", alpha=0.7
        )
        print("Plots generated!")
        return fig

    def aggregate_data(self, sensor_readings_df):
        print(f"AGGREGATING DATA {len(self.ptc_weather_2023_ground_truth)}")
        srdf = sensor_readings_df.copy()
        srdf["ts"] = pd.to_datetime(srdf["ts"], utc=True)
        #srdf["ymdh"] = srdf["ts"].map(
        #    lambda some_date: some_date.replace(minute=0)
        #)
        srdf["year"] = pd.DatetimeIndex(srdf["ts"]).year
        srdf["month"] = pd.DatetimeIndex(srdf["ts"]).month
        srdf["day"] = pd.DatetimeIndex(srdf["ts"]).day
        srdf["hour"] = pd.DatetimeIndex(srdf["ts"]).hour
        srdf = srdf.groupby(["year", "month", "day", "hour"], as_index=False).agg({
            "total_count": ["sum"],
            "humidity": ["mean"],
            "temp": ["mean"]
        })
        srdf.columns = list(map("".join, srdf.columns.values)) # From behzad.nouri (2014) https://stackoverflow.com/a/26325610
        srdf["ymd_idx"] = pd.to_datetime(
            srdf[["year", "month", "day", "hour"]]
        )
        srdf.rename(columns={
            "total_countsum": "y",
            "humiditymean":"humidity_mean",
            "tempmean":"temp_mean",
            "ymd_idx":"ds"
            },
            inplace=True
        )
        srdf.drop(
            columns=["temp_mean", "humidity_mean", "year", "month", "day", "hour"], # Bc we'll use our own.
            inplace=True
        )
        srdf = srdf.merge(
            self.ptc_weather_2023_dev[
                ["ds", "unique_id", "day_of_week",
                 "temp_mean", "humidity_mean",
                 "precipitation_mean", "rain_mean",
                 "snowfall_mean", "snow_depth_mean",
                 "wind_speed_mean", "cloud_cover_mean",
                 "cloud_cover_low_mean", "cloud_cover_mid_mean",
                 "cloud_cover_high_mean", "is_holiday"
                ]
            ],
            left_on="ds",
            right_on="ds",
            how="left" #Prioritize keeping sensor readings!
        )
        self.ptc_weather_2023_ground_truth = pd.concat([
            self.ptc_weather_2023_ground_truth,
            srdf
        ])
        print(f"DATA HAS BEEN AGGREGATED: {len(self.ptc_weather_2023_ground_truth)}")

    
    def forecast(self):
        # So next time we'll be 4 hours in the future.
        self.prediction_start_date = self.prediction_start_date + pd.Timedelta(hours=4)
        print(f"Prediction up until {self.prediction_start_date}")

        window_2022 = self.window_size - len(self.ptc_weather_2023_ground_truth)

        pass_gt = pd.concat([ #We combine the training data + our new example
            self.ptc_weather_2022.tail(window_2022),
            self.ptc_weather_2023_ground_truth
        ])

        print(f"Getting {window_2022} entries from 2022 | {len(self.ptc_weather_2023_ground_truth)} from 2023")

        # Let's readjust the MAD outlier cleaning
        # Step 1: Calculate the Median for the time series
        median = pass_gt["y"].median()
        # Step 2: Calculate the median absolute deviation
        mad = (pass_gt["y"] - median).abs().median()
        # Step 3: The threshold is 3 times that
        threshold = 3 * mad
        # Then we simply find those values and set it to the median
        pass_gt_3mad = pass_gt.copy()
        pass_gt_3mad.loc[(pass_gt_3mad["y"] - median).abs() > threshold, "y"] = median

        print("3MAD Calculated")

        #Move the future data up four hours.
        future_weather = self.ptc_weather_2023_dev.iloc[
            len(self.ptc_weather_2023_ground_truth) : len(self.ptc_weather_2023_ground_truth)+ self.hour_window
        ]
        assert len(future_weather) == self.hour_window, f"Expected {self.hour_window} rows, got {len(future_weather)}"

        print("Future Weather assertion passed. Training model...")

        # Setup the model to be trained.
        sf = StatsForecast(
            models=[MSTL(
                season_length=[24, 24*7],  # adjust to your original config
                trend_forecaster=AutoARIMA()
            )],
            freq='1h',
            n_jobs=1,  # start with 1 to avoid multiprocessing issues
        )
        print("Model has been trained. Forecasting...")
        
        moving_forecasts = sf.forecast(
            h=self.hour_window,
            df=pass_gt_3mad,
            X_df = future_weather,
            level=[80, 95]
        )
        # Update start date?
        print("DONE. Generating Plots...")
        return self.generate_plots(pass_gt, moving_forecasts)
    

