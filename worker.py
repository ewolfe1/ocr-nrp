#!/usr/bin/env python3
from glmocr import GlmOcr
import requests
from PIL import Image
import time
import io
import base64
import html
import json
import zipfile
import os
import sys
import shutil
from pathlib import Path
from datetime import datetime
import redis
import logging

# Redis queue with improved error handling
def get_redis_connection():
    redis_host = os.environ.get('REDIS_HOST', 'redis-service')
    return redis.Redis(host=redis_host, port=6379, db=0, socket_timeout=10, socket_connect_timeout=10)

def get_next_task():
    """Get next PID from queue using BRPOPLPUSH for safety"""
    try:
        r = get_redis_connection()
        # Move from main queue to processing queue (atomic operation)
        result = r.brpoplpush('archival-ocr', 'archival-ocr:processing', timeout=60)
        if result:
            return json.loads(result.decode('utf-8'))
        else:
            # Check if both queues are empty
            main_queue_length = r.llen('archival-ocr')
            processing_queue_length = r.llen('archival-ocr:processing')
            logger.info(f"Queue status: main={main_queue_length}, processing={processing_queue_length}")

            if main_queue_length == 0 and processing_queue_length == 0:
                logger.info("All queues empty - no more work")
                return "QUEUE_EMPTY"
            elif main_queue_length == 0:
                logger.info("Main queue empty, but items still processing elsewhere")
                return "QUEUE_EMPTY"
            else:
                logger.info("Queue not empty but no task received, retrying...")
                return None

    except redis.ConnectionError as e:
        logger.error(f"Redis connection failed: {str(e)}")
        return "REDIS_ERROR"
    except Exception as e:
        logger.error(f"Redis error: {str(e)}")
        return "REDIS_ERROR"

def complete_task(task):
    """Remove completed task from processing queue"""
    try:
        r = get_redis_connection()
        task_str = json.dumps(task, sort_keys=True)
        removed = r.lrem('archival-ocr:processing', 1, task_str)
    except Exception as e:
        logger.warning(f"Could not complete task {task.get('pid', 'unknown')}: {str(e)}")

def fail_task(task):
    """Move failed task back to main queue for potential retry"""
    try:
        r = get_redis_connection()
        task_str = json.dumps(task, sort_keys=True)
        # Remove from processing queue
        r.lrem('archival-ocr:processing', 1, task_str)
        # Add back to main queue for retry (optional)
        r.lpush('archival-ocr', task_str)
        logger.debug(f"Task {task['pid']} marked as failed")
    except Exception as e:
        logger.warning(f"Could not fail task {task.get('pid', 'unknown')}: {str(e)}")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# get image from Islandora and return encoded string
def get_encoded_image(pid, max_retries=5):

    url = f'https://digital.lib.ku.edu/islandora/object/{pid}/datastream/OBJ/view'
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            #print(response.status_code)
            image = Image.open(io.BytesIO(response.content))
            if image.mode != 'RGB':
                image = image.convert('RGB')

            # if necessary, need to keep under max token limit of 6084
            max_pixels = 23500000
            w, h = image.size
            if w * h > max_pixels:
                scale = (max_pixels / (w * h)) ** 0.5
                image = image.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
                logger.info(f"Image resized from {w}x{h} to {int(w*scale)}x{int(h*scale)}")

            image_size = image.size


            buffer = io.BytesIO()
            image.save(buffer, format='JPEG', quality=95, optimize=True, subsampling=0)
            buffer.seek(0)
            image_encode = base64.b64encode(buffer.read()).decode("utf-8")
            image.close()
            buffer.close()
            logger.info("Image retrieved successfully")
            return image_encode, image_size

        except Exception as e:
            if attempt == max_retries - 1:  # Last attempt
                raise
            time.sleep(5 ** attempt)  # Exponential backoff: 1s, 3s, 9s

# extract ocr and hocr from json
def ocr_and_hocr(json_result, image_size, pid, page_id="page_1"):
    img_w, img_h = image_size
    lines = []
    plaintext = []
    for i, region in enumerate(json_result):
        if not region.get('bbox_2d') or not region.get('content'):
            continue
        bbox = region['bbox_2d']
        x1 = int(bbox[0] / 999 * img_w)
        y1 = int(bbox[1] / 999 * img_h)
        x2 = int(bbox[2] / 999 * img_w)
        y2 = int(bbox[3] / 999 * img_h)
        content = html.escape(region.get('content', '').strip())
        plaintext.append(content)
        lines.append(
            f"<span class='ocr_line' id='line_{i}' "
            f"title=\"bbox {x1} {y1} {x2} {y2}\">"
            f"{content}</span>"
        )

    ocr = '\n'.join(plaintext)

    body = "\n".join(lines)
    hocr = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head>
  <title>{pid}</title>
  <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
  <meta name='ocr-system' content='glm-ocr'/>
  <meta name='ocr-capabilities' content='ocr_page ocr_line'/>
</head>
<body>
  <div class='ocr_page' id='{page_id}'
       title='image {page_id}; bbox 0 0 {img_w} {img_h}'>
{body}
  </div>
</body>
</html>"""

    return ocr, hocr

# resize and save layout vis
def get_layout_vis_bytes(result, quality=60, scale=0.5):
    if not result.layout_vis_dir:
        return None
    vis_dir = Path(result.layout_vis_dir)
    # single page = layout_page0.jpg
    candidates = sorted(vis_dir.glob("layout_page*.*"))
    if not candidates:
        return None
    img = Image.open(candidates[0])
    w, h = img.size
    img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=quality, optimize=True)
    return buf.getvalue()

def log_error(pid, e, task, error_count, consecutive_errors, error_results):
    error_count += 1
    consecutive_errors += 1
    logger.error(f"Error processing {pid}: {str(e)}")

    error_results.append({
        'pid': pid,
        'error': str(e),
        'timestamp': datetime.now().isoformat()
    })

    # Mark task as failed (removes from processing queue)
    fail_task(task)

    return consecutive_errors, error_count, error_results

# Setup output files
worker_id = os.environ.get('HOSTNAME', 'worker-unknown')

# Ensure output directory exists
os.makedirs('/shared-output', exist_ok=True)

# Main processing loop
logger.info(f"Worker {worker_id} starting...")
processed_count = 0
error_count = 0
consecutive_errors = 0

# Initialize result lists
error_results = []
task = None
pid = None

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S%f')
with GlmOcr(config_path="glmocr-config.yaml") as parser:
    with zipfile.ZipFile(f"/shared-output/{worker_id}_{timestamp}.zip", 'a') as zf:

        while True:

            try:
                # Get next task
                task = get_next_task()

                if task == "QUEUE_EMPTY":
                    logger.info("Queue is empty, worker exiting")
                    break
                elif task == "REDIS_ERROR":
                    logger.error("Redis connection issues, worker exiting")
                    sys.exit(1)
                elif task is None:
                    logger.info("No tasks available, waiting...")
                    time.sleep(10)  # Wait before checking again
                    continue

                pid = task['pid']

                logger.info(f"Processing {pid} (task {processed_count + 1})")

                # retrieve and encode image
                image_enc, image_size = get_encoded_image(pid)

                # process with glmocr
                result = parser.parse(f"data:image/jpeg;base64,{image_enc}")

                # save outputs
                ocr_text, hocr_text = ocr_and_hocr(result.json_result[0], image_size, pid)
                fn = pid.replace(':','_')
                zf.writestr(f"{fn}_ocr.txt", ocr_text)
                zf.writestr(f"{fn}_hocr.html", hocr_text)
                zf.writestr(f"{fn}_data.json", json.dumps(result.json_result))
                vis = get_layout_vis_bytes(result)
                if vis:
                    zf.writestr(f"{fn}_layout.jpg", vis)

                # cleanup layout dir after writing to zip:
                if result.layout_vis_dir:
                    shutil.rmtree(result.layout_vis_dir, ignore_errors=True)
                del image_enc, result, vis

                # remove task from redis queue
                complete_task(task)

                processed_count += 1
                consecutive_errors = 0  # Reset error counter on success
                logger.info(f"Successfully processed {pid} ({processed_count} total)")

            except KeyboardInterrupt:
                logger.info("Worker interrupted by user")
                break

            except Exception as e:
                consecutive_errors, error_count, error_results = log_error(pid, e, task, error_count, consecutive_errors, error_results)
                logger.info(e)
                if consecutive_errors >= 10:
                    logger.error("Too many consecutive errors, exiting")
                    with open(f"/shared-output/errors_{worker_id}_{timestamp}.json", "w") as errs:
                        json.dump(error_results, errs)
                    break
                continue

# Final save and summary
logger.info("Saving final results...")

if error_results:
    with open(f"/shared-output/errors_{worker_id}_{timestamp}.json", "w") as errs:
        json.dump(error_results, errs)

logger.info(f"Worker {worker_id} completed. Processed: {processed_count}, Errors: {error_count}")

# Final queue status check
try:
    r = get_redis_connection()
    main_remaining = r.llen('archival-ocr')
    processing_remaining = r.llen('archival-ocr:processing')
    logger.info(f"Final queue status: main={main_remaining}, processing={processing_remaining}")
except:
    pass

logger.info(f"Worker {worker_id} exiting")
