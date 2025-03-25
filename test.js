var AOI = region;
var START_DATE = '2018-11-01';
var END_DATE = '2018-11-30';

// Sentinel-1
var s1 = ee.ImageCollection('COPERNICUS/S1_GRD_FLOAT')
    .filter(ee.Filter.eq('instrumentMode', 'IW'))
    .filter(ee.Filter.eq('resolution_meters', 10))
    .filterDate(START_DATE, END_DATE)
    .filterBounds(AOI);

var add_vh_vv_ratio = function(image) {
  var vh_vv_ratio = image.expression('VH / VV', {
    'VH': image.select('VH'), 
    'VV': image.select('VV')
  }).rename('VH_VV_ratio');
  return image.addBands(vh_vv_ratio);
};

var s1_with_ratio = s1.map(add_vh_vv_ratio);
var s1_median = s1_with_ratio.median().clip(AOI);

// Sentinel-2 Cloud and Shadow Masking
var CLOUD_FILTER = 70;
var CLD_PRB_THRESH = 70;
var NIR_DRK_THRESH = 0.15;
var CLD_PRJ_DIST = 1;
var BUFFER = 40;

var SR_BAND_SCALE = 1e4;

//Use this for 2018
var s2_sr_col = ee.ImageCollection('COPERNICUS/S2').filterBounds(AOI).filterDate(START_DATE, END_DATE).filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', CLOUD_FILTER));

//Use this for 2019 onwards
// var s2_sr_col = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED').filterBounds(AOI).filterDate(START_DATE, END_DATE).filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', CLOUD_FILTER));

print(s2_sr_col);

var s2_cloudless_col = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')
    .filterBounds(AOI)
    .filterDate(START_DATE, END_DATE);

var joined = ee.ImageCollection(ee.Join.saveFirst('s2cloudless').apply({
    primary: s2_sr_col,
    secondary: s2_cloudless_col,
    condition: ee.Filter.equals({
        leftField: 'system:index',
        rightField: 'system:index'
    })
}));

var s2_sr_cld = joined.map(function(img) {
    var cld_prb = ee.Image(img.get('s2cloudless')).select('probability');
    var is_cloud = cld_prb.gt(CLD_PRB_THRESH).rename('clouds');
    return img.addBands(ee.Image([cld_prb, is_cloud]));
});

var s2_sr_cld_shdw = s2_sr_cld.map(function(img) {
    var ndwi = img.normalizedDifference(['B3', 'B8']).rename('NDWI');
    var not_water = ndwi.lt(0.3);  // Pixels with NDWI < 0.3 are non-water
    var dark_pixels = img.select('B8').lt(NIR_DRK_THRESH * SR_BAND_SCALE)
        .multiply(not_water)
        .rename('dark_pixels');
    var shadow_azimuth = ee.Number(90).subtract(ee.Number(img.get('MEAN_SOLAR_AZIMUTH_ANGLE')));
    var cld_proj = img.select('clouds')
        .directionalDistanceTransform(shadow_azimuth, CLD_PRJ_DIST * 10)
        .reproject({crs: img.select(0).projection(), scale: 100})
        .select('distance')
        .mask()
        .rename('cloud_transform');
    var shadows = cld_proj.multiply(dark_pixels).rename('shadows');
    return img.addBands(ee.Image([dark_pixels, cld_proj, shadows]));
});

var s2_sr_cld_shdw_masked = s2_sr_cld_shdw.map(function(img) {
    var is_cld_shdw = img.select('clouds')
        .add(img.select('shadows'))
        .gt(0);

    is_cld_shdw = is_cld_shdw.focalMin(2)
        .focalMax(BUFFER * 2 / 20)
        .reproject({crs: img.select([0]).projection(), scale: 20})
        .rename('cloudmask');

    return img.addBands(is_cld_shdw);
});

var s2_sr_clean = s2_sr_cld_shdw_masked.map(function(img) {
    var not_cld_shdw = img.select('cloudmask').not();
    return img
        .select(['B2', 'B3', 'B4', 'B8'])
        .updateMask(not_cld_shdw);
});

var s2_median = s2_sr_clean.median().clip(AOI);

// Extract values at ground points
var filteredData = cropData.filter(ee.Filter.notNull(['Longitude', 'Latitude']));

var extractVV_VH_NDVI = function(feature) {
    var longitude = ee.Number.parse(feature.get('Longitude'));
    var latitude = ee.Number.parse(feature.get('Latitude'));
    var point = ee.Geometry.Point([longitude, latitude]); 
    
    var vv_vh_values = s1_median.reduceRegion({
        reducer: ee.Reducer.mean(),
        geometry: point,
        scale: 10,
        maxPixels: 1e13
    });

    var ndvi_value = s2_median.normalizedDifference(['B8', 'B4']).rename('NDVI')
        .reduceRegion({
            reducer: ee.Reducer.mean(),
            geometry: point,
            scale: 10,
            maxPixels: 1e13
        });

    // Ensure no null values for NDVI, VV, or VH
    var vv = ee.Algorithms.If(ee.Algorithms.IsEqual(vv_vh_values.get('VV'), null), 0, vv_vh_values.get('VV'));
    var vh = ee.Algorithms.If(ee.Algorithms.IsEqual(vv_vh_values.get('VH'), null), 0, vv_vh_values.get('VH'));
    var ndvi = ee.Algorithms.If(ee.Algorithms.IsEqual(ndvi_value.get('NDVI'), null), 0, ndvi_value.get('NDVI'));

    return feature.set({
        'VV': vv,
        'VH': vh,
        'NDVI': ndvi
    });
};


var processedData = filteredData.map(extractVV_VH_NDVI).filter(ee.Filter.notNull(['VV', 'VH', 'NDVI']));

print(processedData)

// Convert FeatureCollection to a CSV file and export to Google Drive
var exportCSV = ee.FeatureCollection(processedData).map(function(feature) {
    return feature.select(['Longitude', 'Latitude', 'VV', 'VH', 'NDVI']);
});


var vizParams = {
    bands: ['B4', 'B3', 'B2'],
    min: 0,
    max: 2500,
    gamma: 1.1
};

Map.centerObject(region, 10);
Map.addLayer(region, {}, 'Region');

// //Without Cloud Masking
// Map.addLayer(s2_sr_col, vizParams, 'S2 Cloud-Free (Filtered Bands)');

//With Cloud Masking
Map.addLayer(s2_median, vizParams, 'S2 Cloud-Free (Filtered Bands)');


// Google Drive Code
Export.table.toDrive({
    collection: exportCSV,
    description: 'Processed_Sentinel_Data_' + START_DATE + '_' + END_DATE,
    folder: 'GEE_Exports',  // Optional: specify a folder in Google Drive
    fileFormat: 'CSV'
});