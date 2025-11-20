# DBT Macros Documentation

## generate_schema_name

**File:** `generate_schema_name.sql`

**Purpose:** Overrides DBT's default schema naming behavior to prevent unwanted schema prefixes.

---

### Why This Macro Exists

DBT's default behavior concatenates schemas using this formula:
```
schema_name = target_schema + "_" + custom_schema
```

This creates ugly, prefixed schema names that don't align with our database architecture.

---

### The Problem (Without This Macro)

**Configuration:**
- `profiles.yml` has `target.schema = "staging"`
- `dbt_project.yml` has `+schema: intermediate`

**DBT's Default Behavior:**
```sql
-- DBT would create this:
CREATE VIEW staging_intermediate.int_orders_enriched AS (...)

-- But our init.sql only creates these schemas:
-- raw, staging, intermediate, mart
-- (NOT staging_intermediate!)

-- Result: ERROR: schema "staging_intermediate" does not exist
```

**All affected schemas:**
| Model Layer | Config (+schema) | Without Macro | With Macro |
|-------------|------------------|---------------|------------|
| Staging | `+schema: staging` | `staging_staging` | `staging` |
| Intermediate | `+schema: intermediate` | `staging_intermediate` | `intermediate` |
| Mart | `+schema: mart` | `staging_mart` | `mart` |

---

### The Solution (With This Macro)

This macro overrides DBT's default behavior to return ONLY the custom schema name (without prefix).

**Logic:**
1. If no custom schema is specified → use `target.schema` (from profiles.yml)
2. If custom schema IS specified → use ONLY that (ignore target.schema)

**Result:**
```sql
-- Clean schema names that match init.sql:
CREATE VIEW staging.stg_orders AS (...)
CREATE VIEW intermediate.int_orders_enriched AS (...)
CREATE TABLE mart.fct_daily_business_performance AS (...)
```

---

### Architecture Alignment

This macro ensures DBT's schema names match our medallion architecture:

```
init.sql creates:               DBT uses (with macro):
┌─────────────┐                ┌─────────────┐
│ raw         │ ──────────────>│ raw.*       │
│ staging     │ ──────────────>│ staging.*   │
│ intermediate│ ──────────────>│ intermediate.*│
│ mart        │ ──────────────>│ mart.*      │
└─────────────┘                └─────────────┘
     ✅ Match!
```

Without this macro, DBT would try to create:
- `staging_staging.*`
- `staging_intermediate.*`
- `staging_mart.*`

These schemas don't exist → `dbt run` fails.

---

### When This Macro Runs

DBT automatically calls this macro for EVERY model during compilation:

1. **Read model config** from `dbt_project.yml`
2. **Call `generate_schema_name()`** with custom schema value
3. **Use returned schema name** in CREATE statement

---

### Related Files

- **init.sql** - Creates the base schemas (raw, staging, intermediate, mart)
- **dbt_project.yml** - Defines `+schema` configs for each model layer
- **profiles.yml** - Defines `target.schema` for dev/prod environments

---

### References

- [DBT Docs: Custom Schemas](https://docs.getdbt.com/docs/build/custom-schemas)
- [DBT Docs: generate_schema_name](https://docs.getdbt.com/docs/build/custom-schemas#advanced-custom-schema-configuration)
