# Food Delivery ETA Optimization & SLA Analysis  
**End-to-End Analytics Project | Python + SQL + Dashboard**

---

## Project Overview
This project demonstrates an end-to-end analytics workflow combining Python, SQL, and dashboarding to analyze delivery performance, ETA reliability, and SLA breaches in a food delivery context.  

The analysis focuses on understanding how distance, traffic, weather, courier experience, and execution speed impact delivery outcomes and customer service levels.  

The project is designed as a portfolio-ready, real-world analytics case, emphasizing operational decision-making rather than model accuracy alone.

---

## Business Problem
Food delivery platforms face persistent challenges:
- Late deliveries despite ETA predictions  
- Low SLA compliance affecting customer satisfaction  
- Limited visibility into operational delay drivers  
- Difficulty identifying high-risk and inefficient deliveries  

**Objective:**  
Use data to predict delivery time, identify SLA risk patterns, and translate analytical outputs into actionable operational insights.

---

## Dataset
**Source:** Synthetic food delivery operations dataset  

The dataset includes:
- Order-level delivery information  
- Distance and preparation time  
- Courier experience  
- Traffic and weather indicators  
- Model-generated prediction outputs  

Some time-based fields were incomplete and were intentionally excluded from business analysis to maintain data integrity.

---

## Project Workflow
The project follows a production-style analytics flow:
1. **Python** – Delivery time prediction & risk generation  
2. **SQL** – Business logic, SLA analysis & decision metrics  
3. **Dashboard** – Operational monitoring & insight communication  

Each layer has a clearly defined responsibility.

---

## Python: Delivery Time Prediction

### Purpose
Python was used to:
- Clean and preprocess delivery data  
- Engineer operational features  
- Build a delivery time prediction model  
- Generate risk and error metrics for downstream analysis  

### Key Work
- Modeled delivery time using distance, preparation time, courier experience, traffic, and weather  
- Generated:
  - Predicted delivery time  
  - Prediction error & absolute error  
  - Delay risk classification  

### Outcome
- Strong aggregate calibration between actual and predicted delivery times  
- Extreme prediction failures identified for further operational analysis  

Python provides predictive signals, not final business decisions.

---

## SQL: Business Analysis & SLA Monitoring

### Data Engineering
- Used staging tables for safe data ingestion  
- Created a clean final table with proper data types  
- Built a business view to:
  - Reconstruct traffic and weather categories  
  - Exclude invalid time fields  
  - Centralize transformation logic  

### SQL Business Questions Answered
1. Average Actual vs Predicted Delivery Time  
2. Delivery Risk Distribution  
3. Top 20 Worst Prediction Errors  
4. SLA Breach Rate by Traffic Condition  
5. SLA Breach Rate by Distance Bucket  
6. Weather Impact on SLA Breaches  
7. Courier Experience vs SLA Breach Rate  
8. Inefficient Deliveries (Slow & Late Orders)  
9. Overall SLA Compliance Percentage  

### Key SQL Findings
- Overall SLA compliance is ~54%  
- SLA breaches increase with:
  - Longer delivery distances  
  - Adverse weather conditions  
  - Traffic uncertainty  
- Execution inefficiencies dominate over prediction bias  

---

## Dashboard: Operational Insights

### KPIs Displayed
- Total Deliveries: 994  
- Average Delivery Time: 56.26 mins  
- Average Predicted Time: 56.16 mins  
- SLA Compliance Rate: 53.92%  

### Dashboard Insights
- ETA predictions are accurate on average  
- Nearly half of deliveries miss SLA targets  
- Distance, speed, weather, and traffic are key delay drivers  
- Courier experience improves outcomes but does not eliminate delays  
- Traffic simulation highlights non-linear delay risk  

The dashboard shows *what* is happening, SQL explains *why*, and Python explains *how predictions were generated*.

---
## Power BI Dashboard  
🔗 Download PBIX: https://drive.google.com/file/d/11jcJohesD5ZWe5l45Dr0zfYHFBrmc1y6/view?usp=sharing



---

## Key Business Takeaways
- Accurate ETA predictions alone do not guarantee SLA success  
- Operational execution and external factors drive most delays  
- Distance-based controls and weather-aware buffers can improve reliability  
- Courier training helps, but systemic improvements are required  

---

## Skills Demonstrated
- **Python:** Data preprocessing, feature engineering, predictive modeling  
- **SQL:** Business views, SLA analysis, risk segmentation, decision queries  
- **Analytics:** Translating predictions into operational insights  
- **Dashboarding:** Executive-level KPI and risk visualization  
- **Problem Solving:** Separating model accuracy from execution performance  

---

## Conclusion
This project demonstrates the ability to move from prediction to business impact by integrating Python, SQL, and dashboards. The emphasis on SLA failures, operational inefficiencies, and execution-driven insights makes this project suitable for data analyst and business analyst roles.

---

## Author
**Nandana V Shamjith**  
Data Analytics & Business Intelligence Portfolio Project


