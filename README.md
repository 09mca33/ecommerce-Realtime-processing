<img width="517" height="283" alt="image" src="https://github.com/user-attachments/assets/8708be44-5787-43b4-9052-334bb70f97db" />


Real-Time Streaming Pipeline: Azure Event Hub → PySpark Structured Streaming → Delta Lake → Power BI

This project demonstrates a complete real-time streaming data pipeline built using Azure cloud services, Apache Spark (Databricks), Delta Lake, and Power BI.
It showcases how data can be ingested from an event streaming platform, processed in real time, stored as Delta tables, and visualized instantly.

🚀 Architecture Overview

Event Generator → Azure Event Hub → Databricks PySpark Streaming → Bronze/Silver Delta Tables → Power BI Dashboard

Event Generator (Python)

Simulates real-time orders (product, quantity, price, timestamp, state)

Publishes streaming messages to Azure Event Hub

Azure Event Hub

Fully managed ingestion service

Acts as a Kafka-compliant message broker

Databricks Structured Streaming (PySpark)

Reads from Event Hub using Kafka API

Cleans and transforms data

Writes data into:

Bronze Layer: raw streaming ingestion

Silver Layer: cleaned, validated, enriched data

Delta Lake

Stores both bronze and silver tables

Provides reliability, schema enforcement, ACID transactions

Power BI

Connects to Delta Lake (ADLS Gen2 or Databricks SQL Endpoint)

Visualizes real-time metrics such as:

Total sales

Orders by state

Product performance

Time-based KPIs



ecommerce-streaming/
│
├── simulator/
│   └── order-generator.py        # Python script to push events to Event Hub
│
├── databricks/
│   ├── bronze-stream.py          # Streaming read from Event Hub → Bronze
│   ├── silver-transform.py       # Streaming transforms → Silver
│   └── utils/                    # Event Hub configs, schema
│
├── powerbi/
│   └── dashboard.pbix            # Power BI report (optional)
│
└── README.md
