-- 1. Create staging table (all NVARCHAR)
CREATE TABLE staging_final_delivery (
  Order_ID nvarchar(100),
  distance_km nvarchar(100),
  Preparation_Time_min nvarchar(100),
  courier_experience_yrs nvarchar(100),
  delivery_time_minutes nvarchar(100),
  order_hour nvarchar(100),
  order_day nvarchar(100),
  is_weekend nvarchar(100),
  speed_km_per_hr nvarchar(100),

  Weather_Foggy nvarchar(200),
  Weather_Rainy nvarchar(200),
  Weather_Snowy nvarchar(200),
  Weather_Windy nvarchar(200),
  Weather_nan nvarchar(200),

  traffic_Low nvarchar(200),
  traffic_Medium nvarchar(200),
  traffic_nan nvarchar(200),

  Time_of_Day_Evening nvarchar(200),
  Time_of_Day_Morning nvarchar(200),
  Time_of_Day_Night nvarchar(200),
  Time_of_Day_nan nvarchar(200),

  Vehicle_Type_Car nvarchar(200),
  Vehicle_Type_Scooter nvarchar(200),

  predicted_delivery_time nvarchar(200),
  prediction_error nvarchar(200),
  abs_error nvarchar(200),
  delay_risk nvarchar(200)
);
BULK INSERT staging_final_delivery
FROM 'C:\Users\nandana\Documents\final_delivery_final.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '\n',    -- or '\r\n' if needed
  CODEPAGE = '65001',
  TABLOCK
);
SELECT TOP 20 * FROM staging_final_delivery;
INSERT INTO final_delivery_final(
  Order_ID, distance_km, Preparation_Time_min, courier_experience_yrs,
  delivery_time_minutes, order_hour, order_day, is_weekend, speed_km_per_hr,
  Weather_Foggy, Weather_Rainy, Weather_Snowy, Weather_Windy, Weather_nan,
  traffic_Low, traffic_Medium, traffic_nan,
  Time_of_Day_Evening, Time_of_Day_Morning, Time_of_Day_Night, Time_of_Day_nan,
  Vehicle_Type_Car, Vehicle_Type_Scooter,
  predicted_delivery_time, prediction_error, abs_error, delay_risk
)
SELECT
  TRY_CAST(NULLIF(Order_ID,'') AS bigint),
  TRY_CAST(NULLIF(distance_km,'') AS float),
  TRY_CAST(NULLIF(Preparation_Time_min,'') AS float),
  TRY_CAST(NULLIF(courier_experience_yrs,'') AS float),
  TRY_CAST(NULLIF(delivery_time_minutes,'') AS float),
  TRY_CAST(NULLIF(order_hour,'') AS int),
  TRY_CAST(NULLIF(order_day,'') AS int),
  CASE WHEN LOWER(ISNULL(NULLIF(is_weekend,''),'0')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,
  TRY_CAST(NULLIF(speed_km_per_hr,'') AS float),

  -- weather booleans: convert common truthy values to 1, else 0
  CASE WHEN LOWER(ISNULL(Weather_Foggy,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,
  CASE WHEN LOWER(ISNULL(Weather_Rainy,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,
  CASE WHEN LOWER(ISNULL(Weather_Snowy,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,
  CASE WHEN LOWER(ISNULL(Weather_Windy,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,
  CASE WHEN LOWER(ISNULL(Weather_nan,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,

  CASE WHEN LOWER(ISNULL(traffic_Low,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,
  CASE WHEN LOWER(ISNULL(traffic_Medium,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,
  CASE WHEN LOWER(ISNULL(traffic_nan,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,

  CASE WHEN LOWER(ISNULL(Time_of_Day_Evening,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,
  CASE WHEN LOWER(ISNULL(Time_of_Day_Morning,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,
  CASE WHEN LOWER(ISNULL(Time_of_Day_Night,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,
  CASE WHEN LOWER(ISNULL(Time_of_Day_nan,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,

  CASE WHEN LOWER(ISNULL(Vehicle_Type_Car,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,
  CASE WHEN LOWER(ISNULL(Vehicle_Type_Scooter,'')) IN ('1','true','t','yes','y') THEN 1 ELSE 0 END,

  TRY_CAST(NULLIF(predicted_delivery_time,'') AS float),
  TRY_CAST(NULLIF(prediction_error,'') AS float),
  TRY_CAST(NULLIF(abs_error,'') AS float),
  NULLIF(LTRIM(RTRIM(delay_risk)), '')
FROM staging_final_delivery;

SELECT COUNT(*) FROM final_delivery_final;
SELECT TOP 20 * FROM final_delivery_final;

DROP TABLE staging_final_delivery;

SELECT COUNT(*) FROM final_delivery_final;

CREATE INDEX IX_final_delivery_final_order_hour ON final_delivery_final(order_hour);
CREATE INDEX IX_final_delivery_final_delay_risk ON final_delivery_final(delay_risk);
CREATE INDEX IX_final_delivery_final_pred_time ON final_delivery_final(predicted_delivery_time);

Go
CREATE OR ALTER VIEW vw_food_delivery_business AS
SELECT
    Order_ID,

    -- Core delivery metrics
    distance_km,
    Preparation_Time_min,
    courier_experience_yrs,
    delivery_time_minutes,

    -- Time features excluded (all values missing in dataset)
    -- order_hour
    -- order_day

    is_weekend,
    speed_km_per_hr,

    -- Reconstructed traffic level
    CASE
        WHEN traffic_Low = 1 THEN 'Low'
        WHEN traffic_Medium = 1 THEN 'Medium'
        ELSE 'Unknown'
    END AS traffic_level,

    -- Reconstructed weather condition
    CASE
        WHEN Weather_Rainy = 1 THEN 'Rainy'
        WHEN Weather_Foggy = 1 THEN 'Foggy'
        WHEN Weather_Snowy = 1 THEN 'Snowy'
        WHEN Weather_Windy = 1 THEN 'Windy'
        ELSE 'Clear'
    END AS weather_condition,

    -- Model outputs
    predicted_delivery_time,
    prediction_error,
    abs_error,
    delay_risk

FROM final_delivery_final;

-- NOTE:
-- order_hour values are missing in the dataset (encoded as -1 originally),
-- so hour-level delivery analysis was excluded to avoid misleading insights.



--1. Average actual vs predicted delivery time
SELECT 
    AVG(delivery_time_minutes) AS avg_actual,
    AVG(predicted_delivery_time) AS avg_predicted
FROM vw_food_delivery_business;
--This query provides a high-level validation of model performance by comparing average actual delivery time against predicted ETA, helping assess whether
-- the model is systematically over- or under-estimating delivery duration.

--Insight:
-- Indicates the model is well-calibrated at an aggregate level with no significant overall bias.
-- The minimal difference (~0.1 minutes) shows no systematic over- or under-prediction.
-- Aggregate averages can mask individual delivery errors, so detailed analysis is complemented using worst-case errors and SLA breach queries.


--2. Risk distribution
SELECT 
    delay_risk, 
    COUNT(*) AS count_orders
FROM vw_food_delivery_business
GROUP BY delay_risk;
--This analysis shows how delivery orders are distributed across risk categories, enabling operations teams to understand overall exposure and prioritize
-- monitoring of high-risk deliveries.

-- insights 
-- Risk categories are almost evenly distributed, indicating balanced exposure across deliveries.
-- A significant share of high-risk orders suggests the need for continuous monitoring rather than exception-only handling.
-- Risk classification is meaningful and not skewed toward a single category.


--3. Top 20 worst predictions (highest error)
SELECT TOP 20
    Order_ID,
    delivery_time_minutes,
    predicted_delivery_time,
    abs_error
FROM vw_food_delivery_business
ORDER BY abs_error DESC;
-- This query identifies the most severe prediction failures, allowing teams to investigate outliers, data quality issues, or operational scenarios where 
-- the model performs poorly.

-- insights
-- The worst prediction errors range from ~5.4 to ~32.4 minutes, indicating that a small number of deliveries deviate significantly from expected ETA.
-- These extreme errors confirm that average model accuracy masks high-impact outliers, which are critical from a customer experience perspective.
-- Several cases show actual delivery times far exceeding predictions, suggesting operational disruptions rather than random noise.
-- These outliers likely correspond to unusual conditions (traffic uncertainty, weather impact, long distances, or execution issues).
-- Identifying and monitoring such cases is essential for SLA protection, escalation handling, and improving fallback ETA buffers.


--4. SLA breach rate by traffic condition
SELECT
  traffic_level,
  COUNT(*) AS total_orders,
  SUM(
      CASE 
        WHEN delivery_time_minutes > predicted_delivery_time 
        THEN 1 
        ELSE 0 
      END
  ) AS sla_breaches,
  ROUND(
      100.0 * SUM(
          CASE 
            WHEN delivery_time_minutes > predicted_delivery_time 
            THEN 1 
            ELSE 0 
          END
      ) / COUNT(*),
      2
  ) AS sla_breach_rate_pct
FROM vw_food_delivery_business
GROUP BY traffic_level
ORDER BY sla_breach_rate_pct DESC;
--This analysis measures how different traffic conditions impact SLA breaches, supporting data-driven decisions for routing strategies, staffing 
-- adjustments, and traffic-aware delivery planning.

-- insights
-- SLA breaches increase significantly under unknown or unclassified traffic conditions, indicating data gaps or routing uncertainty.
-- Even under low traffic, breach rates remain high, suggesting factors beyond congestion affect delivery performance.
-- Traffic-aware planning alone is insufficient; multi-factor optimization is required.

--5.SLA Breach Rate by Distance Bucket
SELECT
    CASE
        WHEN distance_km < 5 THEN '0–5 km'
        WHEN distance_km BETWEEN 5 AND 10 THEN '5–10 km'
        ELSE '10+ km'
    END AS distance_bucket,
    COUNT(*) AS total_orders,
    SUM(
        CASE
            WHEN delivery_time_minutes > predicted_delivery_time
            THEN 1 ELSE 0
        END
    ) AS sla_breaches,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN delivery_time_minutes > predicted_delivery_time
                THEN 1 ELSE 0
            END
        ) / COUNT(*),
        2
    ) AS sla_breach_rate_pct
FROM vw_food_delivery_business
GROUP BY
    CASE
        WHEN distance_km < 5 THEN '0–5 km'
        WHEN distance_km BETWEEN 5 AND 10 THEN '5–10 km'
        ELSE '10+ km'
    END
ORDER BY sla_breach_rate_pct DESC;
--This query measures SLA breach rates across delivery distance ranges to identify which delivery radii contribute most to service failures, supporting 
-- pricing, zoning, and delivery-radius decisions.

-- insights
-- SLA breach rates increase with delivery distance, confirming longer routes pose higher operational risk.
-- However, even short-distance deliveries show substantial breach rates, indicating last-mile inefficiencies.
-- Distance-based pricing or delivery-radius limits may help reduce SLA failures.


--6.Inefficient Deliveries
SELECT
    COUNT(*) AS inefficient_deliveries
FROM vw_food_delivery_business
WHERE speed_km_per_hr < 15
  AND delivery_time_minutes > predicted_delivery_time;
  --This query identifies deliveries that were both slow and late compared to predictions, highlighting operational inefficiencies and potential cost 
  -- leakage areas that require immediate intervention.

-- insights
-- A significant number of deliveries were both slow and late, indicating potential cost leakage.
-- These cases represent opportunities for process optimization, courier training, or routing improvements.
-- Inefficiency is not isolated and requires systemic intervention.

--7.Weather vs SLA
SELECT
    weather_condition,
    COUNT(*) AS total_orders,
    SUM(
        CASE
            WHEN delivery_time_minutes > predicted_delivery_time
            THEN 1 ELSE 0
        END
    ) AS sla_breaches,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN delivery_time_minutes > predicted_delivery_time
                THEN 1 ELSE 0
            END
        ) / COUNT(*),
        2
    ) AS sla_breach_rate_pct
FROM vw_food_delivery_business
GROUP BY weather_condition
ORDER BY sla_breach_rate_pct DESC;
--This analysis evaluates how different weather conditions impact SLA performance, helping operations teams anticipate delays and adjust staffing or 
-- routing strategies during adverse weather.

--insights
-- Adverse weather conditions significantly increase SLA breach rates, with snow and fog being the most disruptive.
-- Even during clear weather, over one-third of deliveries breach SLA, indicating non-weather-related inefficiencies.
-- Weather-aware staffing and dynamic ETA buffers could improve reliability.


--8. SLA Compliance %
SELECT
    ROUND(
        100.0 * SUM(
            CASE
                WHEN delivery_time_minutes <= predicted_delivery_time
                THEN 1 ELSE 0
            END
        ) / COUNT(*),
        2
    ) AS sla_compliance_pct
FROM vw_food_delivery_business;
--This metric provides an executive-level view of overall SLA adherence, enabling leadership to quickly assess whether delivery operations are meeting 
-- customer time expectations.

-- insights
-- Only about half of all deliveries meet SLA expectations, highlighting substantial room for improvement.
--  metric emphasizes the gap between prediction accuracy and operational execution.
-- Improving SLA compliance should be a top operational priority.

--9.Courier Experience Levels Linked to Inefficiency
SELECT
    CASE
        WHEN courier_experience_yrs < 1 THEN '0–1 year'
        WHEN courier_experience_yrs BETWEEN 1 AND 3 THEN '1–3 years'
        ELSE '3+ years'
    END AS experience_bucket,
    COUNT(*) AS total_orders,
    SUM(
        CASE
            WHEN delivery_time_minutes > predicted_delivery_time
            THEN 1 ELSE 0
        END
    ) AS sla_breaches,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN delivery_time_minutes > predicted_delivery_time
                THEN 1 ELSE 0
            END
        ) / COUNT(*),
        2
    ) AS sla_breach_rate_pct
FROM vw_food_delivery_business
WHERE courier_experience_yrs IS NOT NULL
GROUP BY
    CASE
        WHEN courier_experience_yrs < 1 THEN '0–1 year'
        WHEN courier_experience_yrs BETWEEN 1 AND 3 THEN '1–3 years'
        ELSE '3+ years'
    END
ORDER BY sla_breach_rate_pct DESC;
--This analysis reveals whether certain courier experience levels are associated with higher SLA failures, helping management target training, onboarding,
-- or performance improvement programs.


-- insights
-- Less experienced couriers show higher SLA breach rates, indicating a learning curve effect.
-- Performance improves with experience, but breaches remain significant even among senior couriers.
-- Targeted training and mentoring for newer couriers could yield measurable SLA improvements.