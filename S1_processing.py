#Updates existing file

from osgeo import gdal, osr
import numpy as np

# Input and Output File Paths
input_tiff_file = './Tumkue_bottom_S1/S1A_IW_GRDH_1SDV_20250209T004023_20250209T004048_057812_07211B_352B.SAFE/measurement/s1a-iw-grd-vh-20250209t004023-20250209t004048-057812-07211b-002.tiff'
output_tiff_file = './Tumkue_bottom_S1/output_file_preprocessed.tif'

# Function to extract GCPs from GeoTIFF
def extract_gcps(tiff_file):
    dataset = gdal.Open(tiff_file)
    gcps = dataset.GetGCPs()

    if not gcps:
        raise ValueError("No GCPs found in the GeoTIFF.")
    
    gcp_list = []
    for gcp in gcps:
        gcp_list.append((gcp.GCPX, gcp.GCPY, gcp.GCPPixel, gcp.GCPLine))
    
    dataset = None  # Close the file
    return gcp_list

# Function to calculate affine transformation from GCPs
def calculate_affine_transform(gcps):
    # Use first and last GCPs for affine transform estimation
    gcp_start = gcps[0]
    gcp_end = gcps[-1]
    
    # Extract coordinates
    x1, y1, col1, row1 = gcp_start
    x2, y2, col2, row2 = gcp_end
    
    # Calculate pixel size
    pixel_width = (x2 - x1) / (col2 - col1)
    pixel_height = (y2 - y1) / (row2 - row1)
    
    # Calculate affine transformation
    geo_transform = (x1, pixel_width, 0, y1, 0, pixel_height)
    return geo_transform

# Function to update GeoTIFF with affine transformation and CRS
def update_geotiff(tiff_file, geo_transform):
    # Open the GeoTIFF file
    dataset = gdal.Open(tiff_file, gdal.GA_Update)
    
    # Set affine transformation
    dataset.SetGeoTransform(geo_transform)
    
    # Set CRS to WGS 84 (EPSG:4326)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    dataset.SetProjection(srs.ExportToWkt())
    
    # Save changes
    dataset.FlushCache()
    dataset = None  # Close the file

# Extract GCPs from TIFF file
gcps = extract_gcps(input_tiff_file)

# Calculate Affine Transformation
geo_transform = calculate_affine_transform(gcps)
print(f"Calculated GeoTransform: {geo_transform}")

# Update GeoTIFF with GeoTransform and CRS
update_geotiff(input_tiff_file, geo_transform)
print("GeoTIFF updated successfully with CRS and affine transformation.")
