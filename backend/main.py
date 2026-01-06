from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os
import tempfile
from typing import List

# Import extraction logic
from clean_extractor import process_resume_file 

# --- ADD THIS IMPORT ---
from pipeline import save_and_fetch_mysql, save_csv_to_s3
# -----------------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/analyze-resumes")
async def analyze_resumes(files: List[UploadFile] = File(...)):
    results = []

    for file in files:
        tmp_dir = tempfile.gettempdir()
        temp_filename = os.path.join(tmp_dir, f"temp_{file.filename}")
        
        with open(temp_filename, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # 1. Run Extraction (Using your new, better extractor)
        data = process_resume_file(temp_filename)

        # Cleanup temp file
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

        # --- ADD PIPELINE LOGIC HERE ---
        if "error" not in data:
            try:
                # 2. Save to MySQL
                db_row = save_and_fetch_mysql(data)
                
                # 3. Upload CSV to S3 (Triggers Snowflake)
                save_csv_to_s3(db_row)
                
                print(f"Pipeline success for {file.filename}")
            except Exception as e:
                print(f"Pipeline failed for {file.filename}: {e}")
        # -------------------------------

        formatted_result = {
            "filename": file.filename,
            "data": {
                "name": data.get("name"),
                "email": data.get("email"),
                "phone": data.get("mobile"),
                "gender": data.get("gender"),
                "DOB": data.get("dob")
            }
        }
        results.append(formatted_result)

    return results