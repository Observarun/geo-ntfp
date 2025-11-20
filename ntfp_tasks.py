import os
import ntfp_functions
from ntfp_functions_v2_0 import *


def task_create_forest_mask(p):
    """
    Task to create the forest-only raster from the raw LULC ESA raster.
    """

    p.forest_tif_path = os.path.join(p.project_dir, "lulc_forest_50_90.tif")

    ntfp_functions.create_forest_mask(
        p.raw_lulc_path,
        p.forest_tif_path
    )
    print("Forest mask created:", p.forest_tif_path)


def task_reproject_inputs(p):
    """
    Task to reproject roads, rivers, and the forest raster to Mollweide.
    """

    p.roads_reproj = os.path.join(p.project_dir, "roads_proj.gpkg")
    p.rivers_reproj = os.path.join(p.project_dir, "rivers_proj.gpkg")
    p.forest_reproj = os.path.join(p.project_dir, "forest_proj.tif")

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

    ntfp_functions.reproject_vector(p.roads_shp, p.roads_reproj, mollweide_wkt)
    ntfp_functions.reproject_vector(p.rivers_shp, p.rivers_reproj, mollweide_wkt)
    ntfp_functions.reproject_raster(p.forest_tif_path, p.forest_reproj, mollweide_wkt)
    print("Reprojected all inputs to Mollweide projection.")


def task_buffer_and_union(p):
    """
    Task to buffer roads and rivers and union them.
    """

    p.buffer_distance_m = 10000
    p.buffer_roads = os.path.join(p.project_dir, "roads_buffer_10km.gpkg")
    p.buffer_rivers = os.path.join(p.project_dir, "rivers_buffer_10km.gpkg")
    p.union_buffers_path = os.path.join(p.project_dir, "union_buffers.gpkg")
    
    ntfp_functions.buffer_vector(p.roads_reproj, p.buffer_roads, p.buffer_distance_m)
    ntfp_functions.buffer_vector(p.rivers_reproj, p.buffer_rivers, p.buffer_distance_m)
    ntfp_functions.union_buffers([p.buffer_roads, p.buffer_rivers], p.union_buffers_path)
    print("Buffered roads/rivers and unioned the buffers.")

def task_mask_and_calculate_stats(p):
    """
    Task to mask the forest raster and calculate value per country.
    """

    p.masked_forest_tif = os.path.join(p.project_dir, "forest_10km_masked.tif")
    p.out_area_value_csv = os.path.join(p.project_dir, "forest_area_value_by_country.csv")

    ntfp_functions.mask_raster_by_polygon(p.forest_reproj, p.union_buffers_path, p.masked_forest_tif)
    ntfp_functions.area_by_country(p.masked_forest_tif, p.countries_shp, p.value_csv, p.out_area_value_csv)
    print("Masked forest raster and calculated NTFP area/value by country.")
