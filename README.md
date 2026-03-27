# Signal Agent API

A production-style backend system for processing, filtering and distributing trading signals using Python and FastAPI.

## 🚀 Overview

This project implements a full signal-processing pipeline including:

- Signal ingestion via API
- Filtering based on score and risk logic
- Dynamic execution engine
- Risk management system
- Customer & account handling
- Delivery tracking & acknowledgment
- KPI and performance analysis

## 🧠 Core Features

### Signal Engine
- Receives and processes trading signals
- Applies scoring and filtering logic
- Distributes signals to connected clients

### Risk Engine
- Daily loss limits
- Trade count limits
- R-multiple tracking

### Auto Gate System
- KPI-based decision engine
- Automatically blocks or reduces risk based on:
  - drawdown
  - win rate
  - loss streak

### Execution Engine
- Dynamic risk adjustment based on signal score
- Multiple modes: safe / normal / aggressive

### Authentication System
- JWT-based login
- Role-based access (master / customer)

### Monitoring & Debugging
- Heartbeat tracking (client connection monitoring)
- Debug endpoints for signals and deliveries
- KPI analytics endpoints

## 🛠 Tech Stack

- Python
- FastAPI
- SQLite
- JWT Authentication
- REST API design

## 📊 Why this project matters

This system demonstrates:

- backend architecture design
- API structuring and endpoint logic
- state handling (signals, deliveries, trades)
- risk-based decision systems
- real-world use case implementation
