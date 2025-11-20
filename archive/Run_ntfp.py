####### This is non-timber/wood forest product (NTFP) script for GEP project ##########
####### The script uses data from CWON2024 technical report of NTFP provided by World Bank= economic value of NWFP per hecter per country between 1995-2020 in current USD#######
### This script multiplies value * ha of accessible forest (Forest cover from ESA CCI, accessibility by world roads and rivers in 10km buffer). For quick validation, 
# script compares results with results from the CWON 2024 NTFP technical report. The end results are total values per country per year between 1995-2020 and maps##
#Marta Sylla

# Dependencies 
import os
import logging
import pygeoprocessing
import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import mapping
import shapely
import rasterio
import pandas as pd
import numpy as np
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling

logging.basicConfig(level=logging.INFO)

print("All libraries imported successfully!")

def reproject_raster(in_raster_path, out_raster_path, target_crs):
    """
    Reproject a raster to the target CRS using rasterio.
    """
    with rasterio.open(in_raster_path) as src:
        transform, width, height = calculate_default_transform(
            src.crs, target_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': target_crs,
            'transform': transform,
            'width': width,
            'height': height
        })

        with rasterio.open(out_raster_path, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=target_crs,
                    resampling=Resampling.nearest
                )


def reproject_vector(in_vector_path, out_vector_path, target_crs_wkt):
    """
    Reproject a vector to target CRS (WKT) using pygeoprocessing’s reproject_vector.
    """
    pygeoprocessing.reproject_vector(
    base_vector_path=in_vector_path,
    target_projection_wkt=target_crs_wkt,
    target_path=out_vector_path,   # <--- use target_path
    driver_name='GPKG'
)

def buffer_vector(in_vector_path, out_vector_path, buffer_distance_m, use_geodesic=False):
    """
    Buffer a vector layer by `buffer_distance_m`. 
    Then dissolve all features into one (or few) polygons before writing out.
    Optionally demonstrates geodesic buffering using pyproj/shapely.
    """
    import geopandas as gpd
    from shapely.ops import unary_union
    
    gdf = gpd.read_file(in_vector_path)

    if use_geodesic:
        from pyproj import Geod
        geod = Geod(ellps="WGS84")

        def geodesic_buffer(geometry, dist_m):
            # A truly geodesic buffer can be more involved; 
            # placeholder approach shown below (not purely geodesic).
            # For demonstration, one might project around local centroid, etc.
            return geometry.buffer(dist_m)

        gdf['geometry'] = gdf['geometry'].apply(lambda geom: geodesic_buffer(geom, buffer_distance_m))
    else:
        # Planar buffer (ensure data is in a suitable projected CRS for correct distances)
        gdf['geometry'] = gdf.buffer(buffer_distance_m)

    # ----------------------------
    # DISSOLVE the buffered result
    # ----------------------------
    # If you need a single merged geometry for the entire layer, unary_union is simplest:
    merged_geom = unary_union(gdf.geometry)
    
    # Convert to a new single-feature GeoDataFrame
    dissolved_gdf = gpd.GeoDataFrame(geometry=[merged_geom], crs=gdf.crs)
    
    # Save to file
    dissolved_gdf.to_file(out_vector_path, driver='GPKG')


def union_buffers(buffer_paths, out_union_path):
    """
    Merge/union multiple buffer layers into one or few polygons using unary_union.
    """
    # Combine all
    merged_polygons = []
    for path in buffer_paths:
        gdf = gpd.read_file(path)
        merged_polygons.append(unary_union(gdf.geometry))

    unioned_poly = unary_union(merged_polygons)
    # Convert to a GeoDataFrame
    out_gdf = gpd.GeoDataFrame(geometry=[unioned_poly], crs=gdf.crs)
    out_gdf.to_file(out_union_path, driver='GPKG')


def mask_raster_by_polygon(in_raster_path, in_polygon_path, out_raster_path):
    """
    Mask a raster by a polygon. Keep only pixels inside the polygon.
    """
    with rasterio.open(in_raster_path) as src:
        polygon_gdf = gpd.read_file(in_polygon_path)
        # Ensure matching CRS
        if polygon_gdf.crs != src.crs:
            polygon_gdf = polygon_gdf.to_crs(src.crs)
        shapes = [mapping(geom) for geom in polygon_gdf.geometry]
        out_image, out_transform = mask(src, shapes, crop=True)
        out_meta = src.meta.copy()
        out_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform
        })
        with rasterio.open(out_raster_path, "w", **out_meta) as dest:
            dest.write(out_image)


def area_by_country(masked_raster_path, countries_path, value_csv_path, out_csv_path):
    """
    Calculate total forest area (in hectares) by country, then multiply 
    by per-hectare value from CSV.
    """
    # Read countries
    gdf_countries = gpd.read_file(countries_path)

    # Read the value CSV
    df_values = pd.read_csv(value_csv_path)  # columns: country_name, value_per_hectare (example)

    # Raster stats approach
    from rasterstats import zonal_stats
    with rasterio.open(masked_raster_path) as src:
        # Reproject countries to match the raster
        if gdf_countries.crs != src.crs:
            gdf_countries = gdf_countries.to_crs(src.crs)

        stats = zonal_stats(
            gdf_countries,
            masked_raster_path,
            stats=["sum"],
            nodata=0
        )
        # "sum" => sum of pixel values if the raster is a 1/0 forest mask
        pixel_area_m2 = src.res[0] * src.res[1]
        pixel_area_ha = pixel_area_m2 / 10000.0

    # Build results
    output_rows = []
    for i, stat in enumerate(stats):
        country_name = gdf_countries.iloc[i]['country_name']  # or use your field name
        forest_pixel_sum = stat['sum'] if stat['sum'] else 0
        forest_area_ha = forest_pixel_sum * pixel_area_ha
        output_rows.append({
            'country_name': country_name,
            'forest_area_ha': forest_area_ha
        })

    df_area = pd.DataFrame(output_rows)

    # Join values
    df_merged = pd.merge(df_area, df_values, on='country_name', how='left')
    df_merged['total_value'] = df_merged['forest_area_ha'] * df_merged['value_per_hectare']

    df_merged.to_csv(out_csv_path, index=False)
    print(f"Area-by-country results saved to {out_csv_path}")


def main():
    # ---------------------------------------------------------------------
    # 1. Define file paths (modify these)
    # ---------------------------------------------------------------------
    roads_shp = "../../../Files/base_data/submissions/ntfp/global_roads/globalroads.shp"
    rivers_shp = "../../../Files/base_data/submissions/ntfp/global_rivers/ne_10m_rivers_lake_centerlines.shp"
    forest_tif = "../../../Files/global_invest/projects/ntfp/lulc_forest_50_90.tif"  # e.g., 1 for forest, 0 for non-forest
    countries_shp = "../../../Files/base_data/cartographic/ee/ee_r264_correspondence.gpkg"
    value_csv = "../../../Files/base_data/submissions/ntfp/nontimber_price_iucn.csv"
    
    # Temporary / output files
    roads_reproj = "../../../Files/global_invest/projects/ntfp/roads_proj.gpkg"
    rivers_reproj = "../../../Files/global_invest/projects/ntfp/rivers_proj.gpkg"
    buffer_roads = "../../../Files/global_invest/projects/ntfp/roads_buffer_10km.gpkg"
    buffer_rivers = "../../../Files/global_invest/projects/ntfp/rivers_buffer_10km.gpkg"
    union_buffers_path = "../../../Files/global_invest/projects/ntfp/union_buffers.gpkg"
    forest_reproj_tif = "../../../Files/global_invest/projects/ntfp/forest_proj.tif"
    masked_forest_tif = "../../../Files/global_invest/projects/ntfp/forest_10km_masked.tif"
    out_area_value_csv = "../../../Files/global_invest/projects/ntfp/forest_area_value_by_country.csv"

    #################### Cut out forest land cover classes from lulc ESA 2020 - later to set a batch process for cutting forest for maps in year 1992-2020
    lulc_raster_path = "../../../Files/base_data/lulc/esa/lulc_esa_2020.tif"
    forest_tif = "../../../Files/global_invest/projects/ntfp/lulc_forest_50_90.tif"

    with rasterio.open(lulc_raster_path) as src:
        profile = src.profile.copy()
    
        # Optionally set a NoData value or keep 0 as a 'background' class
        # If you want to use an official NoData marker, pick something like 0 or 255
        # for Byte data, or -9999 for int32, etc.
        #profile.update(nodata=0)  
    
        #Open a new file for writing
        with rasterio.open(forest_tif, 'w', **profile) as dst:
        
            #Iterate over blocks for band 1
            for idx, window in src.block_windows(1):
                #Read the data for this window
                data = src.read(1, window=window)
            
                #Create a mask for forest classes
                forest_mask = (data >= 50) & (data <= 90)
            
                #Option 1: Keep the original value if forest, else 0
                data_out = np.where(forest_mask, data, 0).astype(profile['dtype'])
            
                #Write to the output raster, same window
                dst.write(data_out, 1, window=window)

    print(f"Forest-only raster written to: {forest_tif}")

    # ---------------------------------------------------------------------
    # 2. Choose an appropriate projected CRS (WKT) for distance buffering
    #    e.g., Mollweide or a local/regional projection. Example:
    # ---------------------------------------------------------------------
    # This is an example WKT for Mollweide (ESRI:54009). 
    # You can get the official WKT from many sources or from pyproj.CRS("ESRI:54009").to_wkt()
    mollweide_wkt = (
        'PROJCS["World_Mollweide",'
        'GEOGCS["GCS_WGS_1984",'
        'DATUM["WGS_1984",'
        'SPHEROID["WGS_84",6378137,298.257223563]],'
        'PRIMEM["Greenwich",0],'
        'UNIT["Degree",0.017453292519943295]],'
        'PROJECTION["Mollweide"],'
        'PARAMETER["False_Easting",0],'
        'PARAMETER["False_Northing",0],'
        'PARAMETER["Central_Meridian",0],'
        'UNIT["Meter",1]]'
    )

    # Buffer distance in meters (10,000 m = 10 km)
    buffer_distance_m = 10000

    # ---------------------------------------------------------------------
    # 3. Reproject roads and rivers to the chosen CRS
    # ---------------------------------------------------------------------
    logging.info("Reprojecting roads...")
    reproject_vector(roads_shp, roads_reproj, mollweide_wkt)
    logging.info("Reprojecting rivers...")
    reproject_vector(rivers_shp, rivers_reproj, mollweide_wkt)

    # ---------------------------------------------------------------------
    # 4. Create 10 km buffers using pygeoprocessing or geopandas
    # ---------------------------------------------------------------------
    logging.info("Buffering roads...")
    buffer_vector(roads_reproj, buffer_roads, buffer_distance_m, use_geodesic=False)
    logging.info("Buffering rivers...")
    buffer_vector(rivers_reproj, buffer_rivers, buffer_distance_m, use_geodesic=False)

    # ---------------------------------------------------------------------
    # 5. Union the buffers
    # ---------------------------------------------------------------------
    logging.info("Union of roads and rivers buffers...")
    union_buffers([buffer_roads, buffer_rivers], union_buffers_path)

    # ---------------------------------------------------------------------
    # 6. Reproject the forest raster to the same CRS (Mollweide)
    # ---------------------------------------------------------------------
    logging.info("Reprojecting forest raster to Mollweide...")
    reproject_raster(forest_tif, forest_reproj_tif, target_crs=mollweide_wkt)

    # ---------------------------------------------------------------------
    # 7. Mask the forest raster by the union buffer
    # ---------------------------------------------------------------------
    logging.info("Masking forest raster by 10 km buffer zone...")
    mask_raster_by_polygon(forest_reproj_tif, union_buffers_path, masked_forest_tif)

    # ---------------------------------------------------------------------
    # 8. Calculate forest area per country and multiply by CSV values
    # ---------------------------------------------------------------------
    logging.info("Calculating forest area per country...")
    area_by_country(masked_forest_tif, countries_shp, value_csv, out_area_value_csv)


if __name__ == '__main__':
    main()








#################### Cut out forest land cover classes from lulc ESA 2020 - later to set a batch process for cutting forest for maps in year 1992-2020
# raster_path = "../../../Files/base_data/lulc/esa/lulc_esa_2020.tif"
# output_path = "../../../Files/global_invest/projects/NTFP/lulc_forest_50_90.tif"

# with rasterio.open(raster_path) as src:
#     profile = src.profile.copy()
    
#     # Optionally set a NoData value or keep 0 as a 'background' class
#     # If you want to use an official NoData marker, pick something like 0 or 255
#     # for Byte data, or -9999 for int32, etc.
#     #profile.update(nodata=0)  
    
#     #Open a new file for writing
#     with rasterio.open(output_path, 'w', **profile) as dst:
        
#         #Iterate over blocks for band 1
#         for idx, window in src.block_windows(1):
#             #Read the data for this window
#             data = src.read(1, window=window)
            
#             #Create a mask for forest classes
#             forest_mask = (data >= 50) & (data <= 90)
            
#             #Option 1: Keep the original value if forest, else 0
#             data_out = np.where(forest_mask, data, 0).astype(profile['dtype'])
            
#             #Write to the output raster, same window
#             dst.write(data_out, 1, window=window)

# print(f"Forest-only raster written to: {output_path}")


####################




#2.1. Load and Reproject rivers to Web Mercator
#import geopandas as gpd
#import os

#rivers_in = "Input_data/ne_10m_rivers_lake_centerlines.shp"
#rivers_out = "output_data/rivers_webmerc.shp"  # <--- includes a directory
#os.makedirs(os.path.dirname(rivers_out), exist_ok=True)

#rivers_gdf = gpd.read_file(rivers_in)
#rivers_webmerc = rivers_gdf.to_crs("EPSG:3857")
#rivers_webmerc.to_file(rivers_out)
#print(f"Reprojected roads saved to: {rivers_out}")

#3. Buffer the Roads by 10 km (10,000 meters) ->I did this poin in QGIS
# 1) Load the already reprojected roads in EPSG:3857
#roads_webmerc_shp = "output_data/roads_webmerc.shp"
#roads_webmerc = gpd.read_file(roads_webmerc_shp)
# Confirm the CRS is EPSG:3857
#print(roads_webmerc.crs)  # should print EPSG:3857 or equivalent
# 2) Buffer the roads by 10 km (10,000 meters)
#buffer_distance_m = 10000  # 10 km
#roads_buffer = roads_webmerc.buffer(buffer_distance_m)

# If you want to save this buffer as its own shapefile, you can:
#roads_buffer_shp = "roads_buffer_10km_webmerc.shp"
#gpd.GeoDataFrame(geometry=roads_buffer, crs=roads_webmerc.crs).to_file(roads_buffer_shp)

#print(f"Created 10 km buffer around roads: {roads_buffer_shp}")

#4. Join river and road buffer of 10 km
#import geopandas as gpd
#from shapely.ops import unary_union

# 1. Read each layer
#rivers_gdf = gpd.read_file("output_data/buffer_rivers.gpkg")
#roads_gdf = gpd.read_file("output_data/buffer_roads.gpkg")

# 2. Combine all geometries and create a single union
#all_geometries = list(rivers_gdf.geometry) + list(roads_gdf.geometry)
#union_geom = unary_union(all_geometries)

# 3. Create a GeoDataFrame with one row (the union)
#union_gdf = gpd.GeoDataFrame(geometry=[union_geom], crs=rivers_gdf.crs)

# 4. Save to GeoPackage (single feature)
#union_gdf.to_file("union_buffers.gpkg", driver="GPKG")

#print("Union of both buffers saved to union_buffers.gpkg")



#5. Intersect (Clip) the Forest Raster With the Buffer Mask
#import rasterio
#import numpy as np

#forest_in = "lulc_forest_50_90_webmerc.tif"
#buffer_mask_in = "roads_buffer_10km_mask_webmerc.tif"
#forest_out_clipped = "lulc_forest_50_90_webmerc_buffered.tif"

#with rasterio.open(forest_in) as src:
    #forest_data = src.read(1)
    #profile = src.profile
    
#with rasterio.open(buffer_mask_in) as msk:
    #mask_data = msk.read(1)

# Intersect: keep forest_data where mask_data==1, else set 0 (or NoData)
#forest_clipped = np.where(mask_data == 1, forest_data, 0).astype(forest_data.dtype)

# If you want NoData instead of 0, do:
# nodata_val = src.nodata if src.nodata else -9999
# forest_clipped = np.where(mask_data == 1, forest_data, nodata_val).astype(forest_data.dtype)
# Then update profile["nodata"] = nodata_val

#with rasterio.open(forest_out_clipped, 'w', **profile) as dst:
    #dst.write(forest_clipped, 1)

#print(f"Forest clipped by 10 km roads buffer => {forest_out_clipped}")

####
# Command: Load FAO own consumption data
#def loadNTFPData():
    #df = pd.read_csv('input_data/nontimber_ecosystem_services_iucn.csv', delimiter=';', encoding='utf-8')
    #df = df.drop(df.columns[df.columns.str.contains('unnamed', case=False)], axis=1)  # remove unnamed columns
    #print("CSV loaded successfully! Here's a preview:")
    #print(df.head(10))
    #return df

# --- CALL THE FUNCTION BELOW ---
#loadNTFPData()