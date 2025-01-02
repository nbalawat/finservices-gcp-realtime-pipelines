from fastapi import FastAPI, HTTPException
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import uvicorn
from google.cloud.bigtable.data import ReadRowsQuery
from payment_queries import (
    PaymentsByCustomerQuery,
    PaymentsByDateQuery,
    BigtableConfig
)

app = FastAPI(title="Payment Query API")

# Configuration
BIGTABLE_CONFIG = BigtableConfig(
    project_id="agentic-experiments-446019",
    instance_id="payment-processing-dev",
    table_id="payments_by_customer"  # Default table
)

class QueryRequest(BaseModel):
    customer_id: str
    transaction_types: Optional[List[str]] = None
    start_date: str  # Format: YYYY-MM-DDTHH:mm:ss
    end_date: str    # Format: YYYY-MM-DDTHH:mm:ss
    query_type: str  # "by_customer" or "by_date"

class QueryMetricsResponse(BaseModel):
    query_type: str
    execution_time_ms: float
    row_count: int
    error: Optional[str] = None

class QueryResponse(BaseModel):
    results: List[dict]
    metrics: Optional[QueryMetricsResponse] = None

@app.get("/")
async def root():
    return {"message": "Payment Query API is running"}

@app.post("/query", response_model=QueryResponse)
async def query_payments(request: QueryRequest):
    # Validate dates
    try:
        datetime.strptime(request.start_date, "%Y-%m-%dT%H:%M:%S")
        datetime.strptime(request.end_date, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use YYYY-MM-DDTHH:mm:ss"
        )
    
    # Initialize appropriate query implementation
    if request.query_type == "by_customer":
        config = BigtableConfig(
            project_id=BIGTABLE_CONFIG.project_id,
            instance_id=BIGTABLE_CONFIG.instance_id,
            table_id="payments_by_customer"
        )
        query_impl = PaymentsByCustomerQuery(config)
    elif request.query_type == "by_date":
        config = BigtableConfig(
            project_id=BIGTABLE_CONFIG.project_id,
            instance_id=BIGTABLE_CONFIG.instance_id,
            table_id="payments_by_date"
        )
        query_impl = PaymentsByDateQuery(config)
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid query_type. Use 'by_customer' or 'by_date'"
        )
    
    try:
        results = await query_impl.get_customer_transactions_by_types(
            customer_id=request.customer_id,
            transaction_types=request.transaction_types or [],
            start_date=request.start_date,
            end_date=request.end_date
        )
        
        # Get metrics if available
        metrics = None
        if query_impl.last_query_metrics:
            metrics = QueryMetricsResponse(
                query_type=query_impl.last_query_metrics.query_type,
                execution_time_ms=query_impl.last_query_metrics.execution_time_ms,
                row_count=query_impl.last_query_metrics.row_count,
                error=query_impl.last_query_metrics.error
            )
        
        return QueryResponse(results=results, metrics=metrics)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Query execution failed: {str(e)}"
        )

@app.get("/sample_keys/{table_type}")
async def get_sample_keys(table_type: str, limit: int = 5):
    """Get sample row keys to help with debugging"""
    if table_type == "by_customer":
        table_id = "payments_by_customer"
    elif table_type == "by_date":
        table_id = "payments_by_date"
    else:
        raise HTTPException(status_code=400, detail="Invalid table_type")
    
    config = BigtableConfig(
        project_id=BIGTABLE_CONFIG.project_id,
        instance_id=BIGTABLE_CONFIG.instance_id,
        table_id=table_id
    )
    
    # Use PaymentsByCustomerQuery just to get table access
    query_impl = PaymentsByCustomerQuery(config)
    table = await query_impl._get_table()
    
    try:
        # Read first few rows without any filter
        query = ReadRowsQuery(limit=limit)
        rows = []
        results = await table.read_rows(query)
        for row in results:
            rows.append(row.row_key.decode('utf-8'))
        return {"sample_keys": rows}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read sample keys: {str(e)}"
        )

if __name__ == "__main__":
    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)
