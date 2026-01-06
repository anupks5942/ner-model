from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os
import tempfile
from typing import List

# Import extraction logic
from clean_extractor import process_resume_file 

app = FastAPI()

# Enable CORS
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
        # Write temp files outside the workspace to avoid triggering any live-reload watchers
        tmp_dir = tempfile.gettempdir()
        temp_filename = os.path.join(tmp_dir, f"temp_{file.filename}")
        
        with open(temp_filename, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Run Extraction
        data = process_resume_file(temp_filename)

        # Cleanup
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

        # Format output as requested
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

    print(results)

    return results
