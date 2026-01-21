# ATP Tennis Data Engineering: NoSQL to Relational Database

> **Course:** Armazenamento para Big Data 
> **Academic Year:** 2025/2026  
> **Degree:** Ciência de Dados  
> **Institution:** ISCTE

## Project Overview

This project focuses on the end-to-end lifecycle of a large, unstructured dataset containing ATP Tennis professional matches. 

The primary goal was to transform raw, non-relational data (JSON) into a structured, optimized Relational Database (MySQL). 

## Authors

| Name | Student ID |
|------|------------|
| Daniel Fonseca | 125158 |
| Francisco Gonçalves | 130649 |
| João Filipe | 130665 |
| Guilherme Pires | 131658 |

## Tools

* **Source Format:** JSON
* **NoSQL Database:** MongoDB (Data Staging & Collections)
* **Relational Database:** MySQL
* **Data Cleaning:** Regex

## Methodology

The project was executed in three main phases:

### 1. Data Cleaning & Preparation
Processed the raw JSON dataset to handle inconsistencies:
* **Regex Standardization:** Cleaning names and handling special characters, splitting variable fields.
* **Data Consistency:** Standardization and handling null/missing values in match statistics.
* **ID Generation:** Creating unique identifiers for Matches and Tournaments.

### 2. NoSQL Staging (MongoDB)
* Organization of the cleaned data into collections.
* Preparation of data structures for migration to a relational environment.

### 3. Relational Database (MySQL)
Designed a schema to ensure data integrity:
* **Relationships:** Implementation of Primary Keys and Foreign Keys to link players to matches and tournaments.
