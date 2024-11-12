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
import aiofiles
import tempfile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(title="Bulk S3 Uploader")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'us-east-1')
)

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
MAX_CONCURRENT_UPLOADS = 3
upload_semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
upload_status = {}

def upload_file_to_s3(file_path: str, s3_key: str, content_type: str):
    try:
        s3.upload_file(
            file_path,
            BUCKET_NAME,
            s3_key,
            ExtraArgs={"ContentType": content_type}
        )
        return True
    except Exception as e:
        logger.error(f"S3 upload error: {str(e)}")
        raise e

async def process_upload(file: UploadFile, upload_id: str):
    temp_file = None
    try:
        async with upload_semaphore:
            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                # Read content from UploadFile and write to temp file
                content = await file.read()
                temp_file.write(content)
                temp_file_path = temp_file.name

            upload_status[upload_id]['files'][file.filename] = {
                'status': 'uploading',
                'start_time': datetime.now().isoformat()
            }

            # Upload to S3 using the temporary file
            s3_key = f"{upload_id}/{file.filename}"
            success = await asyncio.to_thread(
                upload_file_to_s3,
                temp_file_path,
                s3_key,
                file.content_type or 'application/octet-stream'
            )

            if success:
                upload_status[upload_id]['files'][file.filename].update({
                    'status': 'completed',
                    'end_time': datetime.now().isoformat()
                })
                logger.info(f"Successfully uploaded {file.filename}")

    except Exception as e:
        logger.error(f"Error uploading {file.filename}: {str(e)}")
        upload_status[upload_id]['files'][file.filename].update({
            'status': 'failed',
            'error': str(e),
            'end_time': datetime.now().isoformat()
        })

    finally:
        # Clean up the temporary file
        try:
            if temp_file:
                os.unlink(temp_file.name)
        except Exception as e:
            logger.error(f"Error cleaning up temporary file: {str(e)}")

@app.post("/upload/")
async def upload_files(
    files: List[UploadFile] = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    upload_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    upload_status[upload_id] = {
        'total_files': len(files),
        'start_time': datetime.now().isoformat(),
        'files': {}
    }

    for file in files:
        if not file.filename:
            continue
        background_tasks.add_task(process_upload, file, upload_id)

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