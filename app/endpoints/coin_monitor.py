import os
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse, JSONResponse
from datetime import datetime
from scripts.fx.scripts.coins.ochestrator_coins import orchestrator  # Import the orchestrator

router = APIRouter()

# Base directory where processed files are stored
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PROCESSED_DIR = os.path.join(BASE_DIR, 'scripts', 'fx', 'data', 'coins', 'processed')

@router.get("/coin_monitor/report/{date}", response_class=PlainTextResponse)
async def get_report(date: str):
    """
    Endpoint to fetch a report by date in YYYYMMDD format.
    """
    try:
        file_path = os.path.join(PROCESSED_DIR, f"{date}.txt")
        
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="Report not found")

        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read()

        return PlainTextResponse(content)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/coin_monitor/update_report")
async def trigger_report_update(date: str = None):
    """
    Endpoint to trigger the orchestrator for a given or current date in YYYYMMDD format.
    """
    try:
        # Use the provided date, or default to today's date in YYYYMMDD format
        if date:
            try:
                current_date = datetime.strptime(date, "%Y%m%d").date()
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Please use YYYYMMDD.")
        else:
            current_date = datetime.now().date()
        
        result = orchestrator(current_date)
        return {"status": "success", "message": f"Report for {current_date.strftime('%Y-%m-%d')} triggered successfully.", "result": result}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger the report: {str(e)}")

@router.post("/coin_monitor/update_report_for_date")
async def trigger_report_for_date(date: str = Query(...)):
    """
    Endpoint to trigger the orchestrator for a specific date in YYYYMMDD format.
    The date should be passed as a query parameter in the format YYYYMMDD.
    """
    try:
        # Validate and convert the input date string to a datetime.date object using YYYYMMDD format
        try:
            current_date = datetime.strptime(date, "%Y%m%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Please use YYYYMMDD.")

        # Trigger the orchestrator with the given date
        result = orchestrator(current_date)
        return {"status": "success", "message": f"Report for {current_date.strftime('%Y-%m-%d')} triggered successfully.", "result": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger the report: {str(e)}")

@router.get("/coin_monitor/available_dates", response_class=JSONResponse)
async def get_available_dates():
    """
    Endpoint to get a list of available report dates in YYYYMMDD format.
    """
    try:
        # List all .txt files in the processed directory
        files = [f for f in os.listdir(PROCESSED_DIR) if f.endswith(".txt")]
        
        # Extract the date part from each filename (e.g., "20240814" from "20240814.txt")
        available_dates = [f.replace(".txt", "") for f in files]
        
        # Return the list of dates as a JSON response
        return JSONResponse(content={"available_dates": available_dates})
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve available dates: {str(e)}")
