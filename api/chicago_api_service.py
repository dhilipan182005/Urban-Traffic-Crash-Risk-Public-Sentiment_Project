from fastapi import FastAPI
import pandas as pd
import os
from config.config import SILVER_CHICAGO, GOLD_CHICAGO

app = FastAPI(title="Chicago Crash Data API")

@app.get("/api/chicago/crashes")
def get_crashes():
    try:
        local_gold_path = GOLD_CHICAGO.replace("hdfs://namenode:9000/user/hadoop", "/workspace/data")
        
        if os.path.exists(local_gold_path):
            df = pd.read_parquet(local_gold_path)
            return df.to_dict(orient="records")
        else:
            return {"error": f"Data not found at {local_gold_path}"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/chicago/silver")
def get_silver_data():
    try:
        local_silver_path = SILVER_CHICAGO.replace("hdfs://namenode:9000/user/hadoop", "/workspace/data")
        if os.path.exists(local_silver_path):
            df = pd.read_parquet(local_silver_path).head(100)
            return df.to_dict(orient="records")
        else:
            return {"error": "Silver data not found"}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
