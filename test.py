import ee
import os
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

service_account = 'gee-service-account@wise-scene-427306-q3.iam.gserviceaccount.com'
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
KEY_PATH = os.path.join(CURRENT_DIR, "gee-key.json")

print("Checking for service account key file...")
if not os.path.exists(KEY_PATH):
    raise FileNotFoundError(f"Service account key file not found at: {KEY_PATH}")

print("Initializing Earth Engine...")
if not ee.data._credentials:
    credentials = ee.ServiceAccountCredentials(service_account, KEY_PATH)
    ee.Initialize(credentials)
print("Earth Engine initialized successfully")

print("Loading GeoJSON file...")
geojson_path = os.path.join(CURRENT_DIR, "Tumkur.geojson")
with open(geojson_path, "r", encoding='utf-8') as f:
    geojson_data = json.load(f)
AOI = ee.Geometry(geojson_data["features"][0]["geometry"])
print(f"AOI loaded from GeoJSON: {geojson_path}")

START_DATE = "2018-09-01"
END_DATE = "2018-09-30"
CSV_FILE_PATH = "./ragi_2018_09.csv"

base_name = os.path.basename(CSV_FILE_PATH)
file_name, _ = os.path.splitext(base_name)

FILTER_OUTPUT_CSV_PATH = os.path.join(CURRENT_DIR, f"filtered_{file_name}.csv")
OUTPUT_CSV_PATH = os.path.join(CURRENT_DIR, f"Processed_Sentinel_Data_{START_DATE}_{END_DATE}.csv")

print(f"Processing data for period: {START_DATE} to {END_DATE}")

print("Loading Sentinel-1 data...")
s1 = (ee.ImageCollection('COPERNICUS/S1_GRD_FLOAT')
    .filter(ee.Filter.eq('instrumentMode', 'IW'))
    .filter(ee.Filter.eq('resolution_meters', 10))
    .filterDate(START_DATE, END_DATE)
    .filterBounds(AOI))
print(f"Sentinel-1 collection size: {s1.size().getInfo()} images")

def add_vh_vv_ratio(image):
    print("Adding VH/VV ratio to image...")
    vh_vv_ratio = image.expression('VH / VV', {
        'VH': image.select('VH'),
        'VV': image.select('VV')
    }).rename('VH_VV_ratio')
    return image.addBands(vh_vv_ratio)

print("Processing Sentinel-1 with VH/VV ratio...")
s1_with_ratio = s1.map(add_vh_vv_ratio)
s1_median = s1_with_ratio.median().clip(AOI)
print("Sentinel-1 median image created")

CLOUD_FILTER = 70
CLD_PRB_THRESH = 70
NIR_DRK_THRESH = 0.15
CLD_PRJ_DIST = 1
BUFFER = 40
SR_BAND_SCALE = 1e4

def get_s2_collection(aoi, start_date, end_date, cloud_filter):
    print("Fetching Sentinel-2 collection...")

    #For 2018
    s2_sr_col = (ee.ImageCollection('COPERNICUS/S2')
        .filterBounds(aoi)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', cloud_filter)))
    print(f"S2 SR collection size: {s2_sr_col.size().getInfo()} images")

    #For 2019 onwards
    # s2_sr_col = (ee.ImageCollection('COPERNICUS/S2_SR')
    #     .filterBounds(aoi)
    #     .filterDate(start_date, end_date)
    #     .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', cloud_filter)))
    # print(f"S2 SR collection size: {s2_sr_col.size().getInfo()} images")

    s2_cloudless_col = (ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')
        .filterBounds(aoi)
        .filterDate(start_date, end_date))
    print(f"S2 cloudless collection size: {s2_cloudless_col.size().getInfo()} images")

    joined = ee.ImageCollection(ee.Join.saveFirst('s2cloudless').apply(**{
        'primary': s2_sr_col,
        'secondary': s2_cloudless_col,
        'condition': ee.Filter.equals(**{
            'leftField': 'system:index',
            'rightField': 'system:index'
        })
    }))
    print(f"Joined collection size: {joined.size().getInfo()} images")
    return joined

def add_cloud_bands(img):
    print("Adding cloud bands...")
    cld_prb = ee.Image(img.get('s2cloudless')).select('probability')
    is_cloud = cld_prb.gt(CLD_PRB_THRESH).rename('clouds')
    return img.addBands(ee.Image([cld_prb, is_cloud]))

def add_shadow_bands(img):
    print("Adding shadow bands...")
    ndwi = img.normalizedDifference(['B3', 'B8']).rename('NDWI')
    not_water = ndwi.lt(0.3)
    dark_pixels = (img.select('B8')
        .lt(NIR_DRK_THRESH * SR_BAND_SCALE)
        .multiply(not_water)
        .rename('dark_pixels'))
    
    shadow_azimuth = ee.Number(90).subtract(ee.Number(img.get('MEAN_SOLAR_AZIMUTH_ANGLE')))
    cld_proj = (img.select('clouds')
        .directionalDistanceTransform(shadow_azimuth, CLD_PRJ_DIST * 10)
        .reproject(**{'crs': img.select(0).projection(), 'scale': 100})
        .select('distance')
        .mask()
        .rename('cloud_transform'))
    
    shadows = cld_proj.multiply(dark_pixels).rename('shadows')
    return img.addBands(ee.Image([dark_pixels, cld_proj, shadows]))

def add_cloud_shadow_mask(img):
    print("Adding cloud/shadow mask...")
    is_cld_shdw = (img.select('clouds')
        .add(img.select('shadows'))
        .gt(0))
    
    is_cld_shdw = (is_cld_shdw.focalMin(2)
        .focalMax(BUFFER * 2 / 20)
        .reproject(**{'crs': img.select(0).projection(), 'scale': 20})
        .rename('cloudmask'))
    
    return img.addBands(is_cld_shdw)

def apply_mask(img):
    print("Applying mask...")
    not_cld_shdw = img.select('cloudmask').Not()
    return (img.select(['B2', 'B3', 'B4', 'B8'])
        .updateMask(not_cld_shdw))

def process_s2_data(aoi, start_date, end_date, cloud_filter):
    print("Starting Sentinel-2 processing...")
    s2_collection = get_s2_collection(aoi, start_date, end_date, cloud_filter)
    
    s2_with_clouds = s2_collection.map(add_cloud_bands)
    print("Cloud bands added")
    s2_with_shadows = s2_with_clouds.map(add_shadow_bands)
    print("Shadow bands added")
    s2_with_mask = s2_with_shadows.map(add_cloud_shadow_mask)
    print("Cloud/shadow mask added")
    s2_clean = s2_with_mask.map(apply_mask)
    print("Mask applied")
    
    s2_median = s2_clean.median().clip(aoi)
    print("Sentinel-2 median image created")
    return s2_median

def load_crop_data(csv_path):
    print(f"Loading crop data from {csv_path}...")
    df = pd.read_csv(csv_path)
    print(f"Crop data loaded with {len(df)} rows")
    
    # Filter out rows with null Longitude or Latitude
    df = df.dropna(subset=['Longitude', 'Latitude'])
    print(f"Rows after filtering null coordinates: {len(df)}")
    
    # Convert filtered DataFrame to FeatureCollection with numeric index
    features = [
        ee.Feature(
            ee.Geometry.Point([row['Longitude'], row['Latitude']]),
            {'Longitude': str(row['Longitude']), 'Latitude': str(row['Latitude']), 'index': i}
        ) for i, (_, row) in enumerate(df.iterrows())
    ]
    crop_data = ee.FeatureCollection(features)
    print("Filtered crop data converted to FeatureCollection with numeric index")
    
    return crop_data, len(df)

def process_batch(crop_data, start_index, batch_size, combined_image):
    print(f"Processing batch starting at index {start_index}...")
    batch = crop_data.filter(
        ee.Filter.And(
            ee.Filter.gte('index', start_index),
            ee.Filter.lt('index', start_index + batch_size)
        )
    )
    
    try:
        # Extract values for the batch
        print(f"Reducing regions for batch at {start_index}...")
        processed_batch = combined_image.reduceRegions(**{
            'collection': batch,
            'reducer': ee.Reducer.mean(),
            'scale': 10,
            'tileScale': 4
        })
        
        # Fetch the batch data locally
        print(f"Fetching data for batch at {start_index}...")
        batch_data = processed_batch.getInfo()
        
        # Convert to DataFrame
        print(f"Converting batch at {start_index} to DataFrame...")
        features = batch_data['features']
        batch_df = pd.DataFrame([
            {
                'Longitude': feature['properties']['Longitude'],
                'Latitude': feature['properties']['Latitude'],
                'VV': feature['properties'].get('VV'),
                'VH': feature['properties'].get('VH'),
                'NDVI': feature['properties'].get('NDVI')
            } for feature in features
        ])
        
        print(f"Completed batch at index {start_index} with {len(batch_df)} rows")
        return batch_df
    except Exception as e:
        print(f"Error processing batch at index {start_index}: {str(e)}")
        raise

def process_data(crop_data, total_size, start_date, end_date, batch_size=1000, num_threads=8):
    print("Starting data processing with batching and threading...")
    
    print(f"Total features to process: {total_size}")
    
    # Calculate number of batches
    num_batches = (total_size + batch_size - 1) // batch_size
    print(f"Data split into {num_batches} batches with batch size {batch_size}")
    
    # Prepare combined image
    print("Preparing combined Sentinel-1 and Sentinel-2 image...")
    s1_image = s1_median.select(['VV', 'VH'])
    s2_ndvi = s2_median.normalizedDifference(['B8', 'B4']).rename('NDVI')
    combined_image = s1_image.addBands(s2_ndvi)
    print("Combined image prepared")
    
    # Generate batch start indices
    batch_indices = list(range(0, total_size, batch_size))
    print(f"Generated {len(batch_indices)} batch indices")
    
    # Process batches using ThreadPoolExecutor
    processed_dfs = []
    completed_batches = 0
    
    print(f"Starting {num_threads} threads to process batches...")
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit all batch tasks
        future_to_index = {
            executor.submit(process_batch, crop_data, start_index, batch_size, combined_image): start_index
            for start_index in batch_indices
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_index):
            start_index = future_to_index[future]
            try:
                batch_df = future.result()
                processed_dfs.append(batch_df)
                completed_batches += 1
                print(f"Collected {completed_batches} out of {num_batches} batches (index {start_index})")
            except Exception as e:
                print(f"Batch at index {start_index} failed with error: {str(e)}")
    
    # Combine all batches into a single DataFrame
    print("Combining all batches into a single DataFrame...")
    processed_data_df = pd.concat(processed_dfs, ignore_index=True)
    print(f"All {num_batches} batches processed and combined into a single DataFrame")
    
    # Save to CSV
    print(f"Saving data to CSV: {OUTPUT_CSV_PATH}...")
    processed_data_df.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"Data saved to CSV file: {OUTPUT_CSV_PATH}")
    
    return processed_data_df

def filter_data(processedData):
    df = processedData
    df = df.dropna(subset=['NDVI'])  
    df = df[df['NDVI'] != 0]
    df.to_csv(FILTER_OUTPUT_CSV_PATH, index=False)

print("Starting main processing...")
s2_median = process_s2_data(AOI, START_DATE, END_DATE, CLOUD_FILTER)
crop_data, total_size = load_crop_data(CSV_FILE_PATH)
processedData = process_data(crop_data, total_size, START_DATE, END_DATE, batch_size=1000, num_threads=8)
filter_data(processedData)
print(f"Final Filtering Completed")
print("Main processing completed")