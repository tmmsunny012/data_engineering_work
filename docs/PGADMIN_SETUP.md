# pgAdmin Setup Guide

## 🎯 Overview

**pgAdmin** is a web-based PostgreSQL client that lets you browse, query, and manage your database through a graphical interface.

---

## 🚀 Quick Start

### **1. Start Services**

```bash
# Start all containers (including pgAdmin)
docker-compose up -d

# Verify pgAdmin is running
docker-compose ps
```

**Expected output:**
```
NAME                STATUS
business_postgres   Up (healthy)
business_pgadmin    Up
airflow_webserver   Up (healthy)
airflow_scheduler   Up
```

### **2. Access pgAdmin**

Open your browser and go to:
```
http://localhost:5050
```

### **3. Login**

**Default credentials:**
- **Email:** `admin@admin.com`
- **Password:** `admin`

---

## 🔧 First-Time Setup

### **Step 1: Add Database Server**

After logging in for the first time:

1. **Right-click** on "Servers" in the left panel
2. Click **"Register" → "Server..."**

### **Step 2: General Tab**

```
Name: Business Database
```

(You can name it anything you want)

### **Step 3: Connection Tab**

Enter the following details:

```
Host name/address:  postgres
Port:              5432
Maintenance database: business_db
Username:          dataeng
Password:          dataeng123
```

**IMPORTANT:** Use `postgres` as the host (not `localhost`)!
- ✅ `postgres` - Container name (correct)
- ❌ `localhost` - Won't work inside Docker

### **Step 4: Save**

Click **"Save"**

You should now see the connection in the left panel!

---

## 📊 Browsing Your Data

### **Navigate to Tables**

```
Servers
  └─ Business Database
      └─ Databases
          └─ business_db
              └─ Schemas
                  ├─ raw
                  │   └─ Tables
                  │       ├─ listing
                  │       ├─ orders
                  │       ├─ weather
                  │       └─ ...
                  ├─ staging
                  │   └─ Tables
                  │       ├─ stg_listing
                  │       ├─ stg_orders
                  │       └─ ...
                  ├─ intermediate
                  │   └─ Tables
                  └─ mart
                      └─ Tables
                          └─ fct_daily_business_performance
```

### **View Table Data**

1. **Expand** the schema (e.g., `mart`)
2. **Expand** "Tables"
3. **Right-click** on a table (e.g., `fct_daily_business_performance`)
4. Select **"View/Edit Data" → "All Rows"**

---

## 🔍 Running Queries

### **Open Query Tool**

1. **Click** on the database (`business_db`)
2. Click the **"Query Tool"** button (or press F5)
3. **Type** your SQL query
4. Click **"Execute"** button (or press F5)

### **Example Queries**

#### **Query 1: Check Table Counts**

```sql
-- Row counts across all schemas
SELECT
    'raw.listing' as table_name,
    COUNT(*) as row_count
FROM raw.listing
UNION ALL
SELECT 'raw.weather', COUNT(*) FROM raw.weather
UNION ALL
SELECT 'staging.stg_listing', COUNT(*) FROM staging.stg_listing
UNION ALL
SELECT 'mart.fct_daily_business_performance', COUNT(*)
FROM mart.fct_daily_business_performance;
```

#### **Query 2: Sample Data from Final Table**

```sql
-- View sample business performance data
SELECT
    date,
    outlet_name,
    org_name,
    platform_name,
    daily_orders,
    avg_rating,
    avg_rank,
    avg_temperature
FROM mart.fct_daily_business_performance
ORDER BY date DESC, daily_orders DESC
LIMIT 20;
```

#### **Query 3: Top Performing Outlets**

```sql
-- Which outlets have the most orders?
SELECT
    outlet_name,
    org_name,
    SUM(daily_orders) as total_orders,
    AVG(avg_rating) as avg_rating,
    AVG(avg_rank) as avg_rank
FROM mart.fct_daily_business_performance
GROUP BY outlet_name, org_name
ORDER BY total_orders DESC
LIMIT 10;
```

#### **Query 4: Weather Impact Analysis**

```sql
-- Do colder days have more orders?
SELECT
    CASE
        WHEN avg_temperature < 10 THEN 'Cold (<10°C)'
        WHEN avg_temperature < 20 THEN 'Mild (10-20°C)'
        ELSE 'Warm (>20°C)'
    END as temperature_range,
    COUNT(*) as days,
    AVG(daily_orders) as avg_orders
FROM mart.fct_daily_business_performance
WHERE avg_temperature IS NOT NULL
GROUP BY temperature_range
ORDER BY avg_orders DESC;
```

---

## 🎨 pgAdmin Features

### **Visual Query Builder**

1. Right-click on a table
2. Select **"Query Tool"**
3. Click **"Query" → "Graphical Query Builder"**
4. Drag and drop tables to build queries visually

### **Export Data**

1. Run a query
2. Click **"Download as CSV"** button (top right of results)
3. Save to your computer

### **Table Statistics**

1. Right-click on a table
2. Select **"Properties"**
3. View columns, indexes, constraints

### **Schema Diagrams (ER Diagrams)**

1. Right-click on a schema
2. Select **"ERD For Schema"**
3. See visual relationships between tables

---

## 🛠️ Useful Shortcuts

| Action | Shortcut |
|--------|----------|
| Open Query Tool | `Ctrl + Shift + Q` |
| Execute Query | `F5` |
| Explain Query | `F7` |
| Auto-complete | `Ctrl + Space` |
| Format SQL | `Ctrl + Shift + K` |
| Comment Line | `Ctrl + /` |

---

## 🔐 Security Notes

### **Production Deployment**

For production, **change these credentials**:

In `.env` file:
```bash
# Change these!
PGADMIN_DEFAULT_EMAIL=your-email@company.com
PGADMIN_DEFAULT_PASSWORD=strong-password-here
POSTGRES_PASSWORD=strong-db-password
```

Then restart:
```bash
docker-compose down
docker-compose up -d
```

### **Access Control**

pgAdmin is exposed on port 5050 by default. To restrict access:

**Option 1: Only allow localhost**
```yaml
ports:
  - "127.0.0.1:5050:80"  # Only accessible from your machine
```

**Option 2: Use reverse proxy with authentication**
```
Internet → Nginx (with auth) → pgAdmin
```

---

## 🐛 Troubleshooting

### Issue: "Can't connect to pgAdmin"

**Check:**
```bash
# Is container running?
docker-compose ps

# Check logs
docker-compose logs pgadmin
```

**Solution:**
```bash
# Restart pgAdmin
docker-compose restart pgadmin

# Or restart all services
docker-compose restart
```

### Issue: "Server not found: postgres"

**Cause:** Using `localhost` instead of `postgres` as hostname

**Solution:**
- In pgAdmin connection settings, use `postgres` (the container name)
- Docker containers communicate via container names, not localhost

### Issue: "Password authentication failed"

**Cause:** Wrong PostgreSQL credentials

**Solution:**
- Check `.env` file for correct credentials
- Default: `dataeng` / `dataeng123`
- Re-enter in pgAdmin connection settings

### Issue: "Can't see tables"

**Cause:** DBT models haven't run yet

**Solution:**
```bash
# Run DBT to create tables
cd dbt
dbt run
```

Then refresh pgAdmin (right-click on schema → "Refresh")

---

## 📊 Sample Dashboard Queries

### **Daily Performance Overview**

```sql
SELECT
    date,
    COUNT(DISTINCT listing_id) as active_listings,
    SUM(daily_orders) as total_orders,
    AVG(avg_rating) as platform_avg_rating,
    AVG(avg_temperature) as avg_temp
FROM mart.fct_daily_business_performance
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY date
ORDER BY date DESC;
```

### **Rating Distribution**

```sql
SELECT
    rating_category,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM mart.fct_daily_business_performance
WHERE rating_category != 'No Ratings'
GROUP BY rating_category
ORDER BY count DESC;
```

### **Platform Comparison**

```sql
SELECT
    platform_name,
    COUNT(DISTINCT outlet_id) as outlets,
    SUM(daily_orders) as total_orders,
    AVG(avg_rating) as avg_rating,
    AVG(avg_rank) as avg_rank
FROM mart.fct_daily_business_performance
GROUP BY platform_name
ORDER BY total_orders DESC;
```

---

## 🎯 Quick Reference

### **Connection Details**

| Setting | Value |
|---------|-------|
| **pgAdmin URL** | http://localhost:5050 |
| **pgAdmin Email** | admin@admin.com |
| **pgAdmin Password** | admin |
| **PostgreSQL Host** | postgres |
| **PostgreSQL Port** | 5432 |
| **Database Name** | business_db |
| **Database User** | dataeng |
| **Database Password** | dataeng123 |

### **Important Schemas**

| Schema | Purpose |
|--------|---------|
| `raw` | Raw data from CSVs and API |
| `staging` | Cleaned, deduplicated data |
| `intermediate` | Business logic transformations |
| `mart` | Final reporting tables |


**Enjoy exploring your data visually!** 🎉

For more information:
- [pgAdmin Documentation](https://www.pgadmin.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
