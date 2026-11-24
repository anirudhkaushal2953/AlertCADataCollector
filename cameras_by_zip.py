import requests
import csv
import json
import time

zip_code = input("Enter ZIP code: ").strip()

if len(zip_code) != 5 or not zip_code.isdigit():
    raise ValueError("ZIP code must contain exactly 5 digits.")

# 1. Retrieve the ZIP code boundary from the Census API
zip_url = (
    "https://tigerweb.geo.census.gov/arcgis/rest/services/"
    "TIGERweb/tigerWMS_Census2020/MapServer/84/query"
)

zip_params = {
    "where": f"ZCTA5 = '{zip_code}'",
    "outFields": "ZCTA5,GEOID,BASENAME",
    "returnGeometry": "true",
    "outSR": "4326",
    "f": "json"
}

response = requests.get(zip_url, params=zip_params, timeout=30)
response.raise_for_status()

zip_data = response.json()

if "error" in zip_data:
    raise RuntimeError(zip_data["error"])

zip_features = zip_data.get("features", [])

if not zip_features:
    raise RuntimeError(
        f"No Census ZIP Code Tabulation Area found for {zip_code}."
    )

zip_geometry = zip_features[0]["geometry"]

print(f"\nZIP code: {zip_code}")
print("Retrieved Census ZIP boundary.")

# 2. Query ALERTCalifornia camera viewsheds
viewshed_url = (
    "https://services8.arcgis.com/X84q166Srnyl4JMV/"
    "ArcGIS/rest/services/ALERTCalifornia_Camera_Feed/"
    "FeatureServer/1/query"
)

search_params = {
    "where": "1=1",
    "geometry": json.dumps(zip_geometry),
    "geometryType": "esriGeometryPolygon",
    "inSR": "4326",
    "spatialRel": "esriSpatialRelIntersects",
    "returnIdsOnly": "true",
    "f": "json"
}

# POST the spatial query, retrying if the request fails
search_data = None

for attempt in range(3):
    try:
        response = requests.post(
            viewshed_url,
            data=search_params,
            timeout=90
        )
        response.raise_for_status()

        search_data = response.json()

        if "error" in search_data:
            raise RuntimeError(search_data["error"])

        break

    except (requests.RequestException, ValueError, RuntimeError) as error:
        print(f"Attempt {attempt + 1}/3 failed: {error}")

        if attempt == 2:
            raise

        time.sleep(2 * (attempt + 1))

# 3. Get the IDs of viewsheds intersecting the ZIP boundary
object_ids = search_data.get("objectIds") or []

print(f"Intersecting viewsheds found: {len(object_ids)}")

# 4. Retrieve camera metadata in batches of 50
cameras = []

for start in range(0, len(object_ids), 50):
    batch_ids = object_ids[start:start + 50]

    params = {
        "objectIds": ",".join(map(str, batch_ids)),
        "outFields": "*",
        "returnGeometry": "false",
        "f": "json"
    }

    response = requests.get(
        viewshed_url,
        params=params,
        timeout=30
    )
    response.raise_for_status()

    batch_data = response.json()

    if "error" in batch_data:
        raise RuntimeError(batch_data["error"])

    cameras.extend(batch_data.get("features", []))

# 5. Save the matching camera information to a CSV
output_file = f"cameras_zip_{zip_code}.csv"

fields = [
    "zipCode",
    "cameraName",
    "siteId",
    "streamId",
    "isOnline",
    "latitude",
    "longitude",
    "viewTime",
    "imageURL",
    "cameraURL"
]

with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fields)
    writer.writeheader()

    for camera in cameras:
        attributes = camera.get("attributes", {})

        writer.writerow({
            "zipCode": zip_code,
            "cameraName": attributes.get("cameraName"),
            "siteId": attributes.get("siteId"),
            "streamId": attributes.get("streamId"),
            "isOnline": attributes.get("isOnline"),
            "latitude": attributes.get("latitude"),
            "longitude": attributes.get("longitude"),
            "viewTime": attributes.get("viewTime"),
            "imageURL": attributes.get("imageURL"),
            "cameraURL": attributes.get("cameraURL")
        })

print(f"Retrieved {len(cameras)} camera records.")
print(f"Saved results to {output_file}")