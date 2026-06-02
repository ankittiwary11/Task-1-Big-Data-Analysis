# 🏥 Indian Healthcare & Hospital Records — Big Data Analysis

<p align="center">
  <img src="https://img.shields.io/badge/Tool-Dask-blue?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Language-Python%203-yellow?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Records-60%2C000-green?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Features-47%20Columns-purple?style=for-the-badge" />
  <img src="https://img.shields.io/badge/CODTECH-Internship-red?style=for-the-badge" />
</p>

---

## 🏢 Internship Details

| Field | Details |
|---|---|
| **Company** | CODTECH IT Solutions Pvt. Ltd. |
| **Intern Name** | Your Name |
| **Intern ID** | Your Intern ID |
| **Domain** | Data Analytics |
| **Task** | Task 1 — Big Data Analysis |
| **Duration** | 4 Weeks |
| **Mentor** | Mentor Name |

---

## 📌 Project Title
**Indian Healthcare & Hospital Records Big Data Analysis using Dask**

---

## 🎯 Objective

Perform scalable Big Data analysis on **60,000 patient hospital records** across **47 features** using **Dask** — a Python parallel computing framework — to uncover insights about patient demographics, disease patterns, treatment costs, insurance coverage, and hospital operational efficiency across 15 Indian states.

---

## 💡 Why This Is a Big Data Problem

Hospitals across India generate **millions of patient records** every year. A single large hospital can produce 500,000+ records annually. Analysing this at scale requires tools beyond Pandas:

- **Pandas** loads everything into RAM → crashes on large datasets
- **Dask** processes data in parallel partitions → handles datasets larger than RAM
- **Dask** scales from a laptop to a distributed cluster without changing code

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| **Python** | 3.x | Core programming language |
| **Dask** | ≥ 2023.x | Parallel Big Data loading & aggregations |
| **Pandas** | ≥ 1.5 | Post-aggregation manipulation |
| **NumPy** | ≥ 1.23 | Numerical operations |
| **Matplotlib** | ≥ 3.6 | Multi-panel visualisations |
| **Seaborn** | ≥ 0.12 | Statistical plot styling |
| **Jupyter Notebook** | — | Interactive analysis environment |

---

## 📂 Project Structure

```
Task-1-Big-Data-Analysis/
│
├── README.md                   ← Project documentation (this file)
├── healthcare_big_data.csv     ← Dataset (60,000 rows × 47 columns)
└── healthcare_analysis.ipynb   ← Jupyter Notebook (full Big Data analysis)
```

---

## 📁 Dataset Description

**File:** `healthcare_big_data.csv`
| Property | Value |
|---|---|
| Total Records | 60,000 patient admissions |
| Total Features | 47 columns |
| Time Period | January 2021 – December 2023 |
| Geographic Coverage | 15 Indian states, 75 cities |
| Departments | 15 medical specialities |
| Diseases | 75+ diagnosed conditions |

### Complete Column Reference

| # | Column | Type | Description |
|---|---|---|---|
| 1 | `patient_id` | string | Unique patient identifier |
| 2 | `hospital_id` | string | Hospital identifier |
| 3 | `doctor_id` | string | Treating doctor ID |
| 4 | `bed_id` | string | Bed assigned |
| 5 | `admission_date` | date | Date of admission |
| 6 | `discharge_date` | date | Date of discharge |
| 7 | `month` | int | Admission month |
| 8 | `quarter` | string | Q1 / Q2 / Q3 / Q4 |
| 9 | `year` | int | Admission year |
| 10 | `season` | string | Summer / Monsoon / Festive / Winter |
| 11 | `department` | string | Medical department (15 types) |
| 12 | `disease_diagnosed` | string | Primary diagnosis |
| 13 | `admission_type` | string | Emergency / Elective / Urgent / Routine |
| 14 | `ward_type` | string | General / Semi-Private / Private / ICU / HDU |
| 15 | `hospital_type` | string | Government / Private / Semi-Govt / NGO |
| 16 | `length_of_stay` | int | Days hospitalised |
| 17 | `icu_days` | int | Days in ICU (0 if not admitted to ICU) |
| 18 | `gender` | string | Patient gender |
| 19 | `age` | int | Patient age (1–90) |
| 20 | `age_group` | string | 0-12 / 13-25 / 26-40 / 41-60 / 61+ |
| 21 | `blood_group` | string | ABO blood group |
| 22 | `education` | string | Education level |
| 23 | `occupation` | string | Patient's occupation |
| 24 | `city` | string | Patient's city |
| 25 | `state` | string | Patient's state |
| 26 | `zone` | string | North / South / East / West / Central |
| 27 | `is_rural` | int | Rural (1) or Urban (0) |
| 28 | `has_comorbidity` | int | Comorbidity present flag |
| 29 | `comorbidity_type` | string | Type of comorbidity |
| 30 | `is_smoker` | int | Smoker flag |
| 31 | `daily_rate` | float | Daily ward charge (Rs) |
| 32 | `medicine_cost` | float | Medicine expenses (Rs) |
| 33 | `lab_test_cost` | float | Lab / diagnostic cost (Rs) |
| 34 | `surgery_performed` | int | Surgery flag (1 = yes) |
| 35 | `surgery_type` | string | Open / Laparoscopic / Endoscopic etc. |
| 36 | `surgery_cost` | float | Surgery cost (Rs) |
| 37 | `total_bill` | float | Total hospital bill (Rs) |
| 38 | `insurance_type` | string | CGHS / PMJAY / ESI / Private / None |
| 39 | `payment_mode` | string | Insurance / Cash / UPI / EMI |
| 40 | `insurance_covered` | float | Amount covered by insurance (Rs) |
| 41 | `out_of_pocket` | float | Patient's out-of-pocket expense (Rs) |
| 42 | `bill_settled` | int | Bill fully settled flag |
| 43 | `discharge_type` | string | Recovered / Referred / AMA / Deceased |
| 44 | `readmitted_30days` | int | Readmission within 30 days flag |
| 45 | `num_lab_tests` | int | Number of lab tests conducted |
| 46 | `num_medications` | int | Number of medications prescribed |
| 47 | `satisfaction_score` | float | Patient satisfaction (1.0 – 5.0) |

---

## 📓 Notebook Sections

| Section | Description |
|---|---|
| 1 | Import Libraries |
| 2 | Load Dataset with Dask (parallel partitioned loading) |
| 3 | Dataset Preview & Statistical Summary |
| 4 | Data Cleaning with Dask |
| 5 | 13 Key Healthcare KPIs via Dask `.compute()` |
| 6 | 10 Dask Aggregations (department, monthly, geo, demographics, insurance…) |
| 7 | 9 Multi-panel Visualisation Charts |
| 8 | Insights & Business Recommendations |
| 9 | Dask vs Pandas Scalability Benchmark |
| 10 | Project Summary Table |

---

## 📊 Visualisations Included

| Chart | Description |
|---|---|
| 1 | Department-wise patient volume & avg treatment cost |
| 2 | Monthly admissions + quarterly revenue trend |
| 3 | Geographic distribution — Zone / State / City |
| 4 | Patient demographics — Age group / Gender / Occupation |
| 5 | Ward type & hospital type analysis |
| 6 | Insurance type & payment mode breakdown |
| 7 | Top 15 diseases & comorbidity distribution |
| 8 | Seasonal admissions, discharge outcomes, rural vs urban billing |
| 9 | Bill distribution, Age vs LOS scatter, department satisfaction scores |

---

## 🚀 How to Run

### Step 1 — Clone the repository
```bash
git clone https://github.com/<your-username>/codtech-internship-task1.git
cd codtech-internship-task1
```

### Step 2 — Install dependencies
```bash
pip install dask[dataframe] pandas numpy matplotlib seaborn notebook
```

### Step 3 — Open the notebook
```bash
jupyter notebook healthcare_analysis.ipynb
```

Then: **Kernel → Restart & Run All**

---

## 📈 Sample KPIs Derived

| KPI | Value (approx) |
|---|---|
| Total Patient Records | 60,000 |
| Total Hospital Revenue | Rs 3+ Billion |
| Avg Bill per Patient | Rs 50,000+ |
| Avg Length of Stay | ~15 days |
| Surgery Rate | ~30% |
| 30-Day Readmission Rate | ~12% |
| Insured Patients | ~70% |
| Avg Satisfaction Score | ~3.0 / 5.0 |

*(Exact values printed when you run the notebook)*

---

## 🔍 Key Insights

1. **Oncology & Cardiology** have the highest average treatment costs
2. **Monsoon season** sees a spike in respiratory and waterborne disease admissions
3. **ICU wards** have 4× higher billing than General wards
4. **Rural patients** have significantly lower out-of-pocket expenses due to government scheme coverage
5. **30-day readmission rate** is highest in Nephrology and Psychiatry — needs follow-up programs
6. **PMJAY (Ayushman Bharat)** is the most used insurance scheme
7. **41–60 age group** generates the highest total hospital revenue

---

*Submitted as part of CODTECH IT Solutions — Data Analytics Internship | Task 1: Big Data Analysis*
