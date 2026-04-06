# 🚀 Real-Time Recommendation Engine (Streaming Data Platform)

This project implements a production-grade real-time data pipeline that simulates how modern recommendation systems (like Netflix/Amazon) process streaming user activity to generate personalized insights.

---

## 🧠 Overview

The system ingests live streaming data from external APIs, processes it using Apache Spark Structured Streaming, and computes real-time user and global features that can be used for recommendation systems.

It demonstrates core data engineering concepts including:
- Streaming ingestion
- Stateful processing
- Window-based aggregations
- Real-time feature engineering
- Scalable pipeline design

---

## 🏗️ Architecture

Data flows through a distributed streaming pipeline:

---

## ⚙️ Tech Stack

- **Apache Kafka** – real-time event ingestion  
- **Apache Spark Structured Streaming** – stream processing & feature engineering  
- **Python** – ingestion + pipeline logic  
- **WebSocket APIs (Coinbase)** – live streaming data source  
- *(Planned)* Redis – low-latency feature store  
- *(Planned)* FastAPI – recommendation serving layer  

---

## 🔥 Key Features

### ✅ Real-Time Streaming Pipeline
- Consumes live WebSocket data (crypto trades)
- Publishes events into Kafka topics

### ✅ Schema Handling
- Dynamic schema inference from Kafka sample data
- JSON parsing for streaming records

### ✅ Stateful Stream Processing
- Event-time processing with watermarking
- Handles late-arriving data

### ✅ Window-Based Aggregations
- 5-minute sliding windows
- User-level behavioral features:
  - trade frequency
  - average price
- Global features:
  - symbol popularity