# FinGuard Financial — Architecture Diagrams

Mermaid diagrams render natively in GitHub's markdown viewer. Each
diagram reflects what was actually built across Phases 0–10, with
execution status noted where relevant — consistent with this project's
"no fake results" principle applied to diagrams too: a box on a diagram
here corresponds to real code, not aspiration.

## 1. End-to-End System Architecture

```mermaid
flowchart TB
    subgraph ingestion["Ingestion (Phase 2)"]
        GEN[Synthetic Generator] -->|produces| KAFKA[(Kafka<br/>KRaft mode)]
    end

    subgraph streaming["Streaming (Phase 3)"]
        KAFKA -->|consumes| SPARK[Spark Structured<br/>Streaming]
        SPARK --> BRONZE[(Delta Bronze)]
        BRONZE --> SILVER[(Delta Silver)]
    end

    subgraph analytics["Analytics (Phase 4)"]
        PG[(PostgreSQL<br/>reference tables)] -->|dbt run| STAGING[staging models]
        STAGING --> INTERMEDIATE[intermediate models]
        INTERMEDIATE --> GOLD[(Gold: dim_*, fact_*)]
    end

    subgraph ml["Machine Learning (Phase 5)"]
        GOLD -.->|training data source,<br/>Delta unavailable locally| TRAIN[Feature Engineering<br/>+ XGBoost + Isolation Forest]
        PG -->|actual training source| TRAIN
        TRAIN --> MLFLOW[(MLflow Registry<br/>Candidate→Staging→Production)]
    end

    subgraph serving["Serving (Phase 7)"]
        API[FastAPI Scoring Service]
        MLFLOW -->|loads production model| API
        PG -->|real-time features| API
        GOLD -->|risk/alert lookups| API
    end

    subgraph orchestration["Orchestration (Phase 6)"]
        AIRFLOW[Airflow DAGs] -.->|schedules| GEN
        AIRFLOW -.->|schedules| STAGING
        AIRFLOW -.->|schedules| TRAIN
        AIRFLOW -.->|validates| MLFLOW
    end

    subgraph observability["Observability (Phase 8)"]
        API -->|/metrics| PROM[Prometheus]
        PROM --> GRAFANA[Grafana]
    end

    classDef notRun stroke-dasharray: 5 5
    class BRONZE,SILVER,PROM,GRAFANA notRun
```
*Dashed-border nodes (Delta Bronze/Silver, Prometheus, Grafana) are
implemented and unit/HCL-tested but never executed end-to-end in this
sandbox — see each phase's own docs for the specific blocker (Maven
Central for Delta, Docker for Prometheus/Grafana).*

## 2. Local vs. Production Deployment

```mermaid
flowchart LR
    subgraph local["This Sandbox — What Actually Ran"]
        direction TB
        L1[Native PostgreSQL 16] 
        L2[Plain local Spark<br/>no Delta connector]
        L3[MLflow, SQLite backend]
        L4[FastAPI via TestClient<br/>no persistent server]
        L5[Airflow, isolated venv]
        L6[Terraform HCL<br/>validated, not applied]
    end

    subgraph production["Documented Production Target (Azure)"]
        direction TB
        P1[Azure Database<br/>for PostgreSQL]
        P2[Azure Databricks<br/>+ ADLS Gen2 Delta]
        P3[MLflow on a real<br/>tracking server]
        P4[FastAPI behind<br/>Azure API Management]
        P5[Managed Airflow]
        P6[terraform apply<br/>via CI/CD]
    end

    L1 -.maps to.-> P1
    L2 -.maps to.-> P2
    L3 -.maps to.-> P3
    L4 -.maps to.-> P4
    L5 -.maps to.-> P5
    L6 -.maps to.-> P6
```

## 3. Fraud Scoring Request Flow

```mermaid
sequenceDiagram
    participant Client
    participant API as FastAPI
    participant Auth as auth.py
    participant DB as PostgreSQL
    participant Feat as engineer_features()
    participant Model as MLflow (cached)
    participant Hybrid as hybrid.py

    Client->>API: POST /score-transaction
    API->>Auth: require_api_key()
    Auth-->>API: 401 if missing/wrong (else continue)
    API->>DB: look up customer/account/merchant
    DB-->>API: 404 if not found
    API->>DB: load_customer_history() (strictly prior)
    API->>Feat: engineer_features(history + new row)
    Feat-->>API: point-in-time-correct feature row
    API->>Model: predict(features)
    Model-->>API: XGBoost score
    API->>Hybrid: assess_transaction(rules, xgb, iforest=0)
    Hybrid-->>API: risk_score, risk_level, reasons
    API-->>Client: 200 {risk_score, risk_level, decision, reasons}
```

## 4. Gold Layer Star Schema

```mermaid
erDiagram
    dim_customer ||--o{ fact_transactions : "1 to many"
    dim_account ||--o{ fact_transactions : "1 to many"
    dim_merchant ||--o{ fact_transactions : "1 to many"
    dim_customer ||--o{ fact_account_activity : "1 to many"
    dim_customer ||--o| fact_customer_risk : "1 to 1 (current)"
    fact_transactions ||--o| fact_fraud_alerts : "1 to 0..1"

    dim_customer {
        string customer_id PK
        string country
        date signup_date
        string risk_segment
    }
    dim_account {
        string account_id PK
        string customer_id FK
        string account_type
    }
    dim_merchant {
        string merchant_id PK
        string merchant_category
        float risk_score_base
    }
    fact_transactions {
        string transaction_id PK
        string account_id FK
        string customer_id FK
        string merchant_id FK
        float amount
        boolean is_fraud
    }
    fact_fraud_alerts {
        string alert_id PK
        string transaction_id FK
        string alert_source "GROUND_TRUTH_LABEL, provisional"
    }
    fact_customer_risk {
        int customer_risk_key PK
        string customer_id FK
        float risk_score
        boolean is_current "single snapshot, provisional"
    }
```

## 5. What "NOT RUN" Actually Means Across Phases

```mermaid
flowchart LR
    A[Phase 2: Kafka] -->|Docker daemon unavailable| X1[NOT RUN]
    B[Phase 3: Delta Lake] -->|Maven Central blocked| X2[NOT RUN]
    C[Phase 8: Prometheus/Grafana] -->|Docker daemon unavailable| X3[NOT RUN]
    D[Phase 10: terraform apply] -->|Registry blocked| X4[NOT RUN]

    X1 -.-> Y1[Code + unit tests: VERIFIED]
    X2 -.-> Y2[Transformations + watermarking: VERIFIED with real Spark]
    X3 -.-> Y3[/metrics endpoint: VERIFIED, 8/8 tests]
    X4 -.-> Y4[HCL syntax + composition: VERIFIED, 2 independent tools]
```
*Every "NOT RUN" in this project is paired with what WAS independently
verified in its place — never a bare, unqualified gap.*
