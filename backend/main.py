import time
import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from deltalake import DeltaTable
import os

app = FastAPI(title="Medallion Architecture Data API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For POC, allow all. Restrict in production.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_LAKE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data_lake"))

def get_delta_dataset(layer: str, table: str = None):
    """
    Returns a PyArrow dataset for a given Delta table.
    """
    if layer == "gold" and table:
        path = os.path.join(DATA_LAKE_PATH, layer, table)
    else:
        path = os.path.join(DATA_LAKE_PATH, layer)
        
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Table path not found: {path}")
        
    try:
        dt = DeltaTable(path)
        return dt.to_pyarrow_dataset()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading Delta table: {str(e)}")

@app.get("/api/data/{layer}/{table}")
def get_table_data(layer: str, table: str, page: int = 1, limit: int = 50):
    start_time = time.time()
    
    # Layer can be batch, staging, silver, gold
    # Table might be 'default' for non-gold layers, or specific table names for gold
    
    # If the user asks for batch, staging, silver, the path doesn't have a sub-folder
    target_table = table if layer == "gold" else None
    
    dataset = get_delta_dataset(layer, target_table)
    
    offset = (page - 1) * limit
    
    # Query with DuckDB
    query = f"SELECT * FROM dataset LIMIT {limit} OFFSET {offset}"
    
    try:
        con = duckdb.connect()
        result_df = con.query(query).df()
        
        # We also want the total count for pagination, but COUNT(*) on large datasets might be slow.
        # For this POC, we'll do a quick COUNT(*)
        count_query = "SELECT COUNT(*) as total FROM dataset"
        total_records = con.query(count_query).fetchone()[0]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
        
    latency_ms = round((time.time() - start_time) * 1000, 2)
    
    # Replace NaN with None so FastAPI can serialize to JSON correctly
    result_df = result_df.replace({float('nan'): None})
    
    return {
        "data": result_df.to_dict(orient="records"),
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total_records
        },
        "latency_ms": latency_ms
    }

class QueryRequest(BaseModel):
    query: str

@app.post("/api/query")
def run_custom_query(request: QueryRequest):
    start_time = time.time()
    
    # We will register all layers as tables in duckdb for easy querying
    con = duckdb.connect()
    
    try:
        if os.path.exists(os.path.join(DATA_LAKE_PATH, "batch")):
            con.register("batch", get_delta_dataset("batch"))
        if os.path.exists(os.path.join(DATA_LAKE_PATH, "staging")):
            con.register("staging", get_delta_dataset("staging"))
        if os.path.exists(os.path.join(DATA_LAKE_PATH, "silver")):
            con.register("silver", get_delta_dataset("silver"))
            
        # Register gold tables
        gold_path = os.path.join(DATA_LAKE_PATH, "gold")
        if os.path.exists(gold_path):
            for table in os.listdir(gold_path):
                table_path = os.path.join(gold_path, table)
                if os.path.isdir(table_path):
                    con.register(f"gold_{table}", get_delta_dataset("gold", table))
    except Exception as e:
        print(f"Warning: Failed to register some datasets. {e}")

    try:
        # Prevent completely unrestrained queries for safety
        clean_query = request.query.strip()
        if clean_query.endswith(';'):
            clean_query = clean_query[:-1]
            
        if "LIMIT" not in clean_query.upper():
            query_to_run = f"{clean_query} LIMIT 1000"
        else:
            query_to_run = request.query
            
        result_df = con.query(query_to_run).df()
        result_df = result_df.replace({float('nan'): None})
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SQL Execution Error: {str(e)}")
        
    latency_ms = round((time.time() - start_time) * 1000, 2)
    
    return {
        "data": result_df.to_dict(orient="records"),
        "latency_ms": latency_ms
    }

@app.get("/api/dashboard")
def get_dashboard_metrics():
    start_time = time.time()
    con = duckdb.connect()
    
    try:
        # Load the PyArrow datasets
        dim_device = get_delta_dataset("gold", "dim_device")
        mart_daily_costs = get_delta_dataset("gold", "mart_daily_costs")
        mart_sla = get_delta_dataset("gold", "mart_sla_breach_summary")
        fact_hourly = get_delta_dataset("gold", "fact_hourly_telemetry")
        
        # Register them
        con.register("dim_device", dim_device)
        con.register("mart_daily_costs", mart_daily_costs)
        con.register("mart_sla", mart_sla)
        con.register("fact_hourly", fact_hourly)
        
        # Query for KPIs
        daily_costs_df = con.query("SELECT Date, SUM(Daily_Cost_USD) as total_cost, SUM(Daily_Power_kW) as total_power FROM mart_daily_costs GROUP BY Date ORDER BY Date").df()
        sla_breaches_df = con.query("SELECT Date, SUM(Total_Breaches) as total_breaches FROM mart_sla GROUP BY Date ORDER BY Date").df()
        
        hourly_temp_df = con.query("""
            SELECT Hour_of_Day, 
                   AVG(Avg_Entering_Temp) as entering, 
                   AVG(Avg_Leaving_Temp) as leaving 
            FROM fact_hourly 
            GROUP BY Hour_of_Day 
            ORDER BY Hour_of_Day
        """).df()
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error computing dashboard metrics: {str(e)}")
        
    latency_ms = round((time.time() - start_time) * 1000, 2)
    
    return {
        "metrics": {
            "daily_costs": daily_costs_df.to_dict(orient="records"),
            "sla_breaches": sla_breaches_df.to_dict(orient="records"),
            "hourly_temperatures": hourly_temp_df.to_dict(orient="records"),
        },
        "latency_ms": latency_ms
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
