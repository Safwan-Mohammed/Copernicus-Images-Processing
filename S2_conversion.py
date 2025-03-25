import os
import rasterio
import numpy as np
import time
from rasterio.merge import merge
from rasterio.plot import show
from rasterio.enums import Resampling

start_time = time.time()

safe_folder = './Tumkur_bottom_left_S2/S2B_MSIL2A_20250209T051859_N0511_R062_T43PFQ_20250209T075235.SAFE'

bands = {
    '10m': ['B02_10m.jp2', 'B03_10m.jp2', 'B04_10m.jp2', 'B08_10m.jp2'], 
    '20m': ['B05_20m.jp2', 'B11_20m.jp2', 'B12_20m.jp2']
}

band_paths = []

for resolution, band_list in bands.items():
    for root, dirs, files in os.walk(safe_folder):
        for file in files:
            if any(band in file for band in band_list):
                band_paths.append(os.path.join(root, file))

band_paths.sort()

stacked_bands = []
for band in band_paths:
    with rasterio.open(band) as src:
        if src.res[0] != 10:
            scale_factor = src.res[0] / 10  
            new_width = int(src.width * scale_factor)
            new_height = int(src.height * scale_factor)
            data = src.read(
                out_shape=(src.count, new_height, new_width),
                resampling=Resampling.bilinear
            )
            transform = src.transform * src.transform.scale(
                (src.width / data.shape[-1]),
                (src.height / data.shape[-2])
            )
        else:
            data = src.read()
            transform = src.transform
        
        stacked_bands.append(data[0])  

stacked_array = np.stack(stacked_bands, axis=0)


out_meta = src.meta.copy()
out_meta.update({
    "driver": "GTiff",
    "height": stacked_array.shape[1],
    "width": stacked_array.shape[2],
    "transform": transform,
    "count": len(stacked_bands),  
    "dtype": 'float32'  
})

output_file = './Tumkur_bottom_left_S2/S2_Composite(1).tif'

with rasterio.open(output_file, "w", **out_meta) as dest:
    for i in range(len(stacked_bands)):  
        dest.write(stacked_array[i], i + 1)  


print(f"GeoTIFF saved at: {output_file}")

end_time = time.time()
print(f"Execution Time : {end_time - start_time} seconds")