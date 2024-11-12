from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import boto3
import os
from typing import List
from dotenv import load_dotenv
import asyncio
from datetime import datetime
import logging
from botocore.exceptions import ClientError

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(title="Bulk S3 Uploader")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'us-east-1')
)

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB limit
MAX_CONCURRENT_UPLOADS = 3  # Limit concurrent uploads
upload_semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)

# In-memory storage for upload status
upload_status = {}

async def upload_to_s3(file: UploadFile, upload_id: str):
    try:
        async with upload_semaphore:  # Control concurrent uploads
            if file.size > MAX_FILE_SIZE:
                raise HTTPException(status_code=400, detail=f"File {file.filename} exceeds 50MB limit")
            
            # Update status to uploading
            upload_status[upload_id]['files'][file.filename] = {
                'status': 'uploading',
                'start_time': datetime.now().isoformat()
            }
            
            # Stream file to S3 in chunks to minimize memory usage
            try:
                await asyncio.to_thread(
                    s3.upload_fileobj,
                    file.file,
                    BUCKET_NAME,
                    f"{upload_id}/{file.filename}",
                    ExtraArgs={"ContentType": file.content_type}
                )
                
                # Update success status
                upload_status[upload_id]['files'][file.filename].update({
                    'status': 'completed',
                    'end_time': datetime.now().isoformat()
                })
                
            except ClientError as e:
                logger.error(f"S3 upload error for {file.filename}: {str(e)}")
                upload_status[upload_id]['files'][file.filename].update({
                    'status': 'failed',
                    'error': str(e),
                    'end_time': datetime.now().isoformat()
                })
                
    except Exception as e:
        logger.error(f"Unexpected error for {file.filename}: {str(e)}")
        upload_status[upload_id]['files'][file.filename].update({
            'status': 'failed',
            'error': str(e),
            'end_time': datetime.now().isoformat()
        })
    finally:
        await file.close()

@app.post("/upload/")
async def upload_files(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...)
):
    upload_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Initialize upload status
    upload_status[upload_id] = {
        'total_files': len(files),
        'start_time': datetime.now().isoformat(),
        'files': {}
    }
    
    # Queue uploads in background
    for file in files:
        background_tasks.add_task(upload_to_s3, file, upload_id)
    
    return JSONResponse({
        "message": f"Processing {len(files)} files",
        "upload_id": upload_id,
        "status_endpoint": f"/status/{upload_id}"
    })

@app.get("/status/{upload_id}")
async def get_upload_status(upload_id: str):
    if upload_id not in upload_status:
        raise HTTPException(status_code=404, detail="Upload ID not found")
    
    status = upload_status[upload_id]
    completed = sum(1 for f in status['files'].values() if f['status'] == 'completed')
    failed = sum(1 for f in status['files'].values() if f['status'] == 'failed')
    
    return {
        "upload_id": upload_id,
        "total_files": status['total_files'],
        "completed": completed,
        "failed": failed,
        "in_progress": status['total_files'] - (completed + failed),
        "files": status['files']
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv('PORT', 10000)))