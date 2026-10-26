
import csv
import os
import time
import hashlib
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
CSV_FILES = [
    "cameras_zip_92617.csv",
    "cameras_zip_92637.csv"
]

OUTPUT_DIR = "collected_data"

COLLECTION_HOURS = 48
POLL_INTERVAL = 15
REQUEST_TIMEOUT = 10
MAX_WORKERS = 20

# This camera has been confirmed offline.
SKIP_STREAM_IDS = {"Axis-DeerCanyon1"}

ZIP_CODES = ("92617", "92637")

# Create separate output directories for each ZIP code.
for zip_code in ZIP_CODES:
    os.makedirs(
        os.path.join(OUTPUT_DIR, zip_code, "images"),
        exist_ok=True
    )

# Load cameras and combine duplicate stream IDs.
cameras_by_id = {}

for csv_file in CSV_FILES:
    with open(csv_file, "r", newline="", encoding="utf-8") as file:
        for camera in csv.DictReader(file):
            stream_id = camera["streamId"]
            zip_code = camera["zipCode"]

            if stream_id not in cameras_by_id:
                camera["zipCodes"] = {zip_code}
                cameras_by_id[stream_id] = camera
            else:
                cameras_by_id[stream_id]["zipCodes"].add(zip_code)

cameras = list(cameras_by_id.values())

if not cameras:
    raise RuntimeError("No cameras found in the input CSV files.")

print(f"Unique cameras found: {len(cameras)}")
print(f"Known offline cameras skipped: {len(SKIP_STREAM_IDS)}")

for camera in cameras:
    print(
        f'  {camera["streamId"]}: '
        f'ZIP codes {", ".join(sorted(camera["zipCodes"]))}'
    )

# Track hashes per camera so the same image is not saved repeatedly.
seen_hashes = {
    camera["streamId"]: set()
    for camera in cameras
}

# Each ZIP code gets its own log.
log_fields = [
    "downloadTime",
    "zipCode",
    "cameraName",
    "streamId",
    "viewTime",
    "requestDurationSeconds",
    "httpStatus",
    "httpLastModified",
    "downloadSuccess",
    "isNewImage",
    "sha256",
    "imageFile",
    "error"
]

log_files = {}

for zip_code in ZIP_CODES:
    log_path = os.path.join(
        OUTPUT_DIR, zip_code, "collection_log.csv"
    )
    log_files[zip_code] = log_path

    with open(log_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=log_fields)
        writer.writeheader()


def fetch_camera(camera):
    """Download one camera image; safe to run in a worker thread."""

    request_start = datetime.now().astimezone().isoformat(
        timespec="milliseconds"
    )
    start = time.monotonic()

    result = {
        "camera": camera,
        "downloadTime": request_start,
        "requestDurationSeconds": 0.0,
        "httpStatus": "",
        "httpLastModified": "",
        "downloadSuccess": False,
        "content": None,
        "sha256": "",
        "error": ""
    }

    stream_id = camera["streamId"]

    if stream_id in SKIP_STREAM_IDS:
        result["httpStatus"] = "SKIPPED_OFFLINE"
        result["error"] = "Camera confirmed offline; request skipped."
        return result

    try:
        with requests.get(
            camera["imageURL"],
            timeout=REQUEST_TIMEOUT,
            headers={
                "Cache-Control": "no-cache",
                "Pragma": "no-cache"
            }
        ) as response:

            result["httpStatus"] = response.status_code
            result["httpLastModified"] = response.headers.get(
                "Last-Modified", ""
            )

            response.raise_for_status()
            content = response.content

        # Verify that the response begins with JPEG data.
        if not content.startswith(b"\xff\xd8"):
            raise ValueError("Response is not a JPEG image")

        result["content"] = content
        result["sha256"] = hashlib.sha256(content).hexdigest()
        result["downloadSuccess"] = True

    except (requests.RequestException, ValueError) as error:
        result["error"] = str(error)

    except Exception as error:
        result["error"] = f"Unexpected error: {error}"

    finally:
        result["requestDurationSeconds"] = round(
            time.monotonic() - start, 3
        )

    return result


def save_log_row(zip_code, result, is_new, image_path):
    """Append one polling result to the appropriate ZIP-code log."""

    camera = result["camera"]

    row = {
        "downloadTime": result["downloadTime"],
        "zipCode": zip_code,
        "cameraName": camera["cameraName"],
        "streamId": camera["streamId"],
        "viewTime": camera.get("viewTime", ""),
        "requestDurationSeconds": result["requestDurationSeconds"],
        "httpStatus": result["httpStatus"],
        "httpLastModified": result["httpLastModified"],
        "downloadSuccess": result["downloadSuccess"],
        "isNewImage": is_new,
        "sha256": result["sha256"],
        "imageFile": image_path,
        "error": result["error"]
    }

    with open(
        log_files[zip_code], "a", newline="", encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(file, fieldnames=log_fields)
        writer.writerow(row)


end_time = time.monotonic() + COLLECTION_HOURS * 60 * 60
cycle_number = 0

try:
    # Use a pool of workers so cameras are polled concurrently.
    with ThreadPoolExecutor(
        max_workers=min(MAX_WORKERS, len(cameras))
    ) as executor:

        while time.monotonic() < end_time:
            cycle_start = time.monotonic()
            cycle_number += 1

            print(f"\nCollection cycle {cycle_number}")

            futures = [
                executor.submit(fetch_camera, camera)
                for camera in cameras
            ]

            new_count = 0
            duplicate_count = 0
            failure_count = 0

            # Process each result as soon as its request finishes.
            for future in as_completed(futures):
                result = future.result()
                camera = result["camera"]
                stream_id = camera["streamId"]
                is_new = False
                saved_paths = {}

                if result["downloadSuccess"]:
                    image_hash = result["sha256"]

                    if image_hash not in seen_hashes[stream_id]:
                        seen_hashes[stream_id].add(image_hash)
                        is_new = True
                        new_count += 1

                        timestamp = datetime.now().astimezone().strftime(
                            "%Y%m%d_%H%M%S_%f"
                        )

                        # Save into each ZIP code this camera covers.
                        for zip_code in sorted(camera["zipCodes"]):
                            image_dir = os.path.join(
                                OUTPUT_DIR, zip_code, "images"
                            )

                            image_name = (
                                f"{stream_id}_{timestamp}.jpg"
                            )
                            image_path = os.path.join(
                                image_dir, image_name
                            )

                            with open(image_path, "wb") as image_file:
                                image_file.write(result["content"])

                            saved_paths[zip_code] = image_path

                        print(f'  {camera["cameraName"]}: new image')

                    else:
                        duplicate_count += 1
                        print(f'  {camera["cameraName"]}: duplicate')

                else:
                    failure_count += 1
                    print(
                        f'  {camera["cameraName"]}: '
                        f'FAILED - {result["error"]}'
                    )

                # Record one row per ZIP code, even for shared cameras.
                for zip_code in sorted(camera["zipCodes"]):
                    save_log_row(
                        zip_code,
                        result,
                        is_new,
                        saved_paths.get(zip_code, "")
                    )

            elapsed = time.monotonic() - cycle_start

            print(
                f"Cycle summary: {new_count} new, "
                f"{duplicate_count} duplicate, "
                f"{failure_count} failed; "
                f"{elapsed:.1f} seconds"
            )

            wait_time = max(0, POLL_INTERVAL - elapsed)

            if elapsed > POLL_INTERVAL:
                print(
                    "Warning: cycle exceeded 15 seconds; "
                    "the next cycle will start immediately."
                )

            # Do not sleep beyond the 48-hour deadline.
            remaining = end_time - time.monotonic()

            if remaining > 0:
                time.sleep(min(wait_time, remaining))

except KeyboardInterrupt:
    print("\nCollection stopped manually.")

finally:
    print("\nCollection ended.")

    for zip_code in ZIP_CODES:
        print(
            f"{zip_code} images: "
            f"{os.path.join(OUTPUT_DIR, zip_code, 'images')}"
        )
        print(f"{zip_code} log: {log_files[zip_code]}")
