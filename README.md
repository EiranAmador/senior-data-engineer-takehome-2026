# Stryker Take-Home Project — Senior Data Engineer

**Version 3.0 | Updated: 2026-01**

***Note, the use of AI is not permmitted***

Welcome to the Stryker Senior Data Engineer take-home exercise. This repository provides a lightweight, opinionated starting point intended to simulate a realistic data engineering workload at Stryker.

This challenge is **not** about completing a large volume of code. Instead, it is designed to surface how you think about data systems, tradeoffs, modeling decisions, scalability, and long-term maintainability.

---

## Problem Overview

A common responsibility for Senior Data Engineers at Stryker is designing and maintaining robust pipelines that ingest third-party data, model it appropriately, store it efficiently, and make it available for downstream consumers such as analytics, reporting, and data science teams.

This exercise represents a simplified snapshot of that responsibility.

To reduce setup overhead, we provide scaffolding for the execution environment (Docker, Airflow, and Postgres). You are encouraged to use, modify, or replace these components if you believe an alternative approach is more appropriate—provided you clearly document your rationale.

---

## What You’ll Build

For this project, you will ingest **current weather data** from the  
[OpenWeatherMap Current Weather API](https://openweathermap.org/current).  
The free API tier is sufficient; **no paid subscription is required**.

At a high level, the system consists of:

### Core Components (Provided)

- **Airflow**  
  Used to orchestrate ingestion and transformation workflows.

- **Postgres**  
  Used as the analytical storage layer for both raw and derived datasets.  
  (You may substitute another database if you prefer.)

- **Docker / Docker Compose**  
  Used to provide a reproducible local execution environment.

---

## Data Flow Expectations

The expected flow is intentionally flexible but should generally include the following stages:

1. **Ingestion (Fetcher DAG)**

   - `fetcher.py` retrieves data from the OpenWeatherMap API.
   - Data should be validated, normalized, and cleaned as appropriate.
   - Raw or lightly processed data should be persisted to Postgres.
   - Design with **schema evolution**, **data quality**, and **idempotency** in mind.

2. **Transformation (Transformer DAG)**

   - `transformer.py` produces one or more derived datasets.
   - Transformations may be implemented in **Python**, **SQL**, or a hybrid approach.
   - Derived tables should support historical analysis and downstream analytics use cases.

3. **Consumption**
   - Assume downstream users will query both raw and transformed datasets.
   - Queries may include time-series analysis, aggregations, or feature extraction.

---

## Design Philosophy

This exercise is intentionally open-ended.

We are far more interested in:

- **How you structure the problem**
- **Why you make certain design choices**
- **How you balance simplicity vs. scalability**
- **How you communicate assumptions and tradeoffs**

Perfection is not expected. Thoughtfulness is.

> **Note:**  
> If you are uncomfortable with Docker, Airflow, or Postgres, you may replace them with alternatives (e.g., local Python execution, SQLite, dbt, etc.). Just document your decision clearly—we will ask about it during follow-up discussions.

---

## Deliverables

Please submit a GitHub pull request containing:

- Source code for:
  - Data ingestion (fetcher)
  - Data transformation logic
- SQL (or equivalent) defining your data model
- Updates to `README.md` that include:
  - Design notes
  - Assumptions
  - Tradeoffs
  - Potential next steps

---

## Evaluation Criteria

We will evaluate this submission as part of a **Senior Data Engineer** interview loop. Review will focus on:

- **Code quality**
  - Readability, structure, naming, and maintainability
- **Data modeling**
  - Schema design, normalization vs. denormalization, relationships
- **Data engineering fundamentals**
  - Idempotency, error handling, observability, performance considerations
- **Technical judgment**
  - Tooling choices and architectural tradeoffs
- **Communication**
  - Clarity of documentation and reasoning

---

## Time Expectations

We recognize that senior candidates have limited availability.

- Expected effort: **~2 hours**
- Spending more or less time is entirely your choice.
- Please note your actual time spent in the section below so we can evaluate your work fairly and in context.

If you choose to go beyond the basics (e.g., testing, schema evolution, incremental loads), that’s welcome—but not required.

---

## Use of Public Resources

We encourage you to attempt this challenge independently.

That said, real-world engineering often involves referencing documentation, blog posts, or existing solutions. If you build upon external work, please:

- Clearly note it in your comments or README
- Provide links to the original sources
- Explain what you adapted or changed

Transparency matters more than originality.

---

## Getting Started

### Environment Setup

1. Fork and clone this repository.
2. Ensure Docker Desktop is installed and running.
3. Initialize and start the environment:

```bash
# Initialize folders and Airflow user
mkdir -p ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Initialize Airflow metadata DB
docker-compose up airflow-init

# Start services
docker-compose up
```

- Airflow UI: [http://localhost:8080](http://localhost:8080)
- Username / Password: `airflow / airflow`

If you encounter issues, refer to the official Airflow Docker docs:
[https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

---

### Implementation Notes

- There are several `TODO` markers in the repository—feel free to go beyond them.
- DAG examples are adapted from the official Airflow tutorial.
- For database interactions, you may reference:

  - Airflow Postgres Operator documentation
  - Native Python DB libraries
  - Any abstraction you deem appropriate

For simplicity, you may store all datasets in the Airflow-managed Postgres instance.

---

### Apple Silicon Note

If you are using Apple hardware (M1/M2), Docker image compatibility may require additional configuration.
Reference:
[https://javascript.plainenglish.io/which-docker-images-can-you-use-on-the-mac-m1-daba6bbc2dc5](https://javascript.plainenglish.io/which-docker-images-can-you-use-on-the-mac-m1-daba6bbc2dc5)

---

## Your Notes (README.md)

### Time Spent

Approximate total time spent on the exercise: 4-5 hours including system setup.

API key took some time to be enabled. This time was not taken into account.

Include any learning or research time if applicable.

Previous learning: 
  
  - LinkedIn Learning:
    - Apache Airflow training

Useful sources:
  
  Setting up Visual Studio Code to use as editor with WSL: https://learn.microsoft.com/en-us/windows/wsl/tutorials/wsl-vscode
  
  PostgreSQL composite primary key for idempotency: (https://www.dbpilot.io/sql-guides/postgresql/composite-primary-keys-in-postgresql)

  Use of TIMESTAMPTZ type on PostgreSQL: https://www.postgresql.org/docs/current/datatype-datetime.html, https://www.slingacademy.com/article/timestamp-and-timestamptz-data-types-in-postgresql-explained-by-examples/

---

### Assumptions

UTC time normalization was used to account for differences as this is used for the primary key along with the coordinates.

---

### Tradeoffs & Design Decisions

Describe key decisions you made and alternatives you considered.

- Multiprocessing:
Despite the CeleryExecutor being configured not a lot of multiprocessing was used on this first iteration of the project. Depending on requirements this feature could prove to be useful, for example, in the case of a large amount of cities being consumed they could be divided into different pools and processed separately on the fetcher. For a small amount like on this example this probably would not provide a big improvement on performance and the logic would be more complicated with not much benefit.
Pros: Simplicity
Cons: Scalability depending on use case

- Normalization:
In the raw table the data was already normalized for ease of consumption at this layer but the raw json is still being stored and available both for help in debugging (comparing json to the stored columns) and as a way of keeping track of orignal data retrieved from API and in the possibility of a format change this would mean the data is not lost but would need to be reprocessed accordingly. 
Pros: Eases the process of querying the raw data layer.
Cons: More storage per row, normalization processing takes some performance.

- Validation:
In the case that an error is encountered in the process, the run has been set to stop, specifically focusing on the validation of certain keys this could mean that if the API response changes and this keys are no longer provided the program would stop processing. Another approach could be to notify of missing keys but continue saving the other information available, specially the raw json response received.

- Available API Parameters: 
Required:
  -lat
  -lon
  -appid
Optional:
  -mode: Kept as default (JSON) due to ease of working with it on python thanks to the libraries.
  -units: kept as default but can easily be maintained changed from configuration.
  -lang: kept as default but can easily be maintained changed from configuration.
(https://openweathermap.org/current?collection=current_forecast&collection=current_forecast&collection=current_forecast#geo)

---

### Next Steps / Improvements

What would you implement next with more time?

- Implement multiprocessing of cities by divinding the cities pool. This would benefit in the case of a large number of cities being analyzed.

- Add some uniqueness constraints in the raw data table.

- Adding more metadadata to the tables like a version control.

- Quality checks or flags to clearly improbable data.

- Unit testing, specially for the normalization logic since this is a key part to the integrity of the data being submitted to the tables.

---

### Instructions to the Evaluator

Provide any notes or guidance that would help someone reviewing or running your solution.

- API Key:
  API key is retrieved from Airflow variables. As an alternative the key can be set in the config file but this way the key is not stored in version control.

- Postgres Connection:
  The connection must me setup in the Admin settings. Port: 5432, Host: postgres

Thank you for taking the time to complete this exercise.
We look forward to discussing your approach and design decisions.

Thank you for the time to setup and review this exercise! Best regards, Eiran
