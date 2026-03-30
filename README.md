# MIS584_Real_Time_Travelers

Tourist Flow Forecasting for Old Town Tallinn
=============================================

01\. Introduction
-----------------

Old Town Tallinn is a **UNESCO World Heritage Site** recognized as a well-preserved medieval trading city from the 1400s. This project aims to address the challenges of preserving this historic center while managing the modern influx of visitors.


### The Need for Forecasting

*   **Preservation**: Protecting a historic UNESCO site from physical wear.
*   **Urban Planning**: Providing data-driven insights for city management.
*   **Resource Allocation**: Improving the management of bottlenecks and overcrowding.

**Key Stakeholders**: Tallinn City Government, local businesses, tourism boards, and both tourists and locals.

* * *

02\. Dataset
------------

The project utilizes **The Old Town Motion Sensor Dataset**, which tracks entries and exits through 18 motion sensors installed at 16 gates of the Old Town.

*   **Frequency**: 10-minute intervals.
*   **Source**: 
    [Tallinn Open Data Portal (avaandmed.tallinn.ee)](https://avaandmed.tallinn.ee/)
    .
    +1
*   **Metadata**: Includes sensor locations such as _Viru Street 27_, _Great Beach Gate_, and _Castle Hill_.

* * *

03\. Data Validation & Feature Engineering
------------------------------------------

Before modeling, we performed rigorous statistical testing to ensure data reliability:

*   **Hypothesis 1 (Sensor Accuracy)**: Used the **Wilcoxon Signed-Rank Test** to determine that discrepancies between IN and OUT counts are non-random.
*   **Hypothesis 2 (Time-Series Memory)**: An **Autocorrelation Function (ACF)** test proved a strong **7-day cyclical memory** in the traffic data.
*   **Feature Engineering**: Based on validation, we created lag features (e.g., `count_1_day_ago`) and analyzed seasonality and spatial disparities.

* * *

04\. Forecasting Models
-----------------------

We are exploring hybrid models to capture both long-term seasonality and short-term autocorrelations:

*   **Primary Models**: Fourier + ARIMA, Prophet + ARIMA, and MSTL + ARIMA.
*   **Alternative for Sparse Data**: Log scaling, or Fourier + Negative Binomial models for frequent zero-value scenarios.

* * *

05\. Project Goals
------------------

Our final model aims to assist city administrators with:

1.  **Hourly foot traffic forecasting**.
2.  **Proactive resource allocation**.
3.  **Adaptive tourism management** to improve the experience for all stakeholders.

* * *

References
----------

*   Hyndman, R.J., & Athanasopoulos, G. (2021). _Forecasting: Principles and Practice_.
*   Imanov, O.Y.L (2025). _The Old Town Motion Sensor Dataset_. Kaggle.
*   UNESCO (n.d.). _Historic Centre (Old Town) of Tallinn_.

* * *

Contributors
------------

*   **Het Patel**
*   **Tanishk Singh**
*   **Jorge Carlos Encinas Alegre**

_MIS 584 Project Proposal_
