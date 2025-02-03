# **GCP Cost Estimation & Architecture for Quarterly Data Processing**

## **1️⃣ Overview**
This document provides a cost estimation and architecture design for running a **quarterly batch job on GCP** and hosting the LLM client. The job will:
- Run on a **Compute Engine instance** with **Apache Airflow**.
- Generate **CSV files (~1MB each)**.
- Store data in **Google Cloud Storage (GCS)**.
- Load data into **BigQuery** (staging and merging to the main table).
- Have server available for users to access the LLM service. 

## **2️⃣ Compute Engine instance**
We will have two instances. One for the batch job and the other for the LLM. Since the batch job **runs only once per quarter**, we select a **Compute Engine VM**.

We will also use a Compute Engine VM for the LLM server.

### **Recommended Compute Engine VM**
| **Instance Type** | **Specs** | **Hourly Cost** | **Quarterly Run (4 Hours)** |
|------------------|-----------|----------------|------------------|
| **e2-medium** (Standard) | 1 vCPUs, 4GB RAM | **$2.32hr** | **$3.39 per run** |

| **Instance Type** | **Specs** | **Hourly Cost** | **Monthly Run (730)** |
|------------------|-----------|----------------|------------------|
| **e2-standard-4** (For LLM) | 4 vCPUs, 16GB RAM | **$0.59/hr** | **$106.37 per month** |

### **Compute Cost Estimate**
- **e2-medium (Recommended)**: **$2.32 × 1.46 hours = $3.39 per quarter**
- **e2-standard-4 (Heavier workloads)**: **$0.59 × 730 hours = $106.37 per month**

## **3️⃣ Cloud Storage Costs (CSV Storage)**
Each run generates **CSV files (1MB each)**.
- **8 CSV files per run → 8MB per quarter**.
- **Storage pricing**: **$0.00 per GB per month**.

### **Storage Cost Estimate**
- **Quarterly Uploads**: 8 × 4 = **32MB per year**.
- **Cost per year**: `(32MB ÷ 1000) × $0.02 × 12 months = $0.00768 per year`.

👉 **Storage cost is negligible (~$0.01/year).**

## **4️⃣ BigQuery Costs (Querying & Processing Data)**
- **Storage Cost**: **$0.02 per GB per month**.
- **Query Cost**: **$5 per TB scanned**.

### **BigQuery Cost Breakdown**
| **BigQuery Cost Component** | **Estimate** | **Quarterly Cost** |
|---------------------------|-------------|------------------|
| **Storage Cost (1GB main table)** | $0.02 per GB per month | **$0.06 per quarter** |
| **Querying & Processing (15MB scanned)** | $6.25 per TiB → **(15MB ÷ 1TB) × $5** | **$0.000075 per run** |

### **BigQuery Cost Estimate**
- **Storage**: **$0.06 per quarter**.
- **Querying**: ** $0.000075 per run **. If we decide to make the LLM avaialble this be the cost per query entered by user. 
- **Total BigQuery cost per quarter**: **~$0.06**. This can go up dramatically depending on how many queries are submitted by users via the LLM each month.

## **5️⃣ GCP Architecture Overview**
### **Components**
1. **Compute Engine (Airflow Instance)**
   - Runs the script **once per quarter**.
   - Generates **CSV files**.
   - Pushes **CSV files to Cloud Storage**.

2. **Cloud Storage (GCS)**
   - Stores **CSV files (~1MB each)**.

3. **BigQuery**
   - **Staging Table**: Data is initially loaded here.
   - **Merge Query**: Data is cleaned and inserted into **main tables**.
   - **LLM Queries**: Will be used to receive and process user queries. 

### **Architecture Diagram**
```plaintext
[Compute Engine (Airflow)]
    |  (Generate CSV)
    v
[Cloud Storage (GCS)]
    |  (Load into Staging)
    v
[BigQuery (Staging Table)]
    |  (Merge to Main Table)
    v
[BigQuery (Final Table)]
```

## **6️⃣ Final Cost Estimate Per Month**
| **Service** | **Monthly Cost** |
|------------|------------------|
| **Compute Engine (e2-medium)** | **$3.39 ** |
| **e2-standard-4** | **$106.37** |
| **Cloud Storage (CSV Storage)** | **~$0.00** |
| **BigQuery (Storage & Querying)** | **$0.06** |
| **Total Per Month** | **$110.01** |

👉 **Total GCP cost per year**: **$1320 per year**.

## **Final Notes** ##
- There may be cost advantages to using Cloud Functions for the batch scripts. However, we will lose Airflow functionality.
- We may be able to use Cloud Run to save money instead of using a Compute Engine. A Cloud Run could accept 1 million requests for $2.78/month. If we use that instead of a compute engine than we can save over $1k yearly. 
```

