import os
import logging
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
from shapely.ops import unary_union
from shapely.geometry import mapping
import pygeoprocessing
from rasterstats import zonal_stats

logging.basicConfig(level=logging.INFO)


def create_forest_mask(input_lulc_path, output_forest_path):
    """
    Reads LULC raster and
    creates a binary mask (or filtered raster) for forest classes (50-90).
    """

    if os.path.exists(output_forest_path):
        logging.info(f"Forest mask already exists at {output_forest_path}")
        return

    with rasterio.open(input_lulc_path) as src:
        profile = src.profile.copy()
        with rasterio.open(output_forest_path, 'w', **profile) as dst:
            for idx, window in src.block_windows(1):
                data = src.read(1, window=window)
                forest_mask = (data >= 50) & (data <= 90)
                data_out = np.where(forest_mask, data, 0).astype(profile['dtype'])
                dst.write(data_out, 1, window=window)


def reproject_raster(in_raster_path, out_raster_path, target_crs):
    """
    Reproject a raster to the target CRS using rasterio.
    """

    with rasterio.open(in_raster_path) as src:
        transform, width, height = calculate_default_transform(
            src.crs,
            target_crs,
            src.width,src.height,
            *src.bounds
        )

        kwargs = src.meta.copy()
        kwargs.update({'crs': target_crs, 'transform': transform, 'width': width, 'height': height})
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
    Reproject a vector to target CRS (WKT).
    """

    pygeoprocessing.reproject_vector(
        base_vector_path=in_vector_path,
        target_projection_wkt=target_crs_wkt,
        target_path=out_vector_path,
        driver_name='GPKG'
    )


def buffer_vector(in_vector_path, out_vector_path, buffer_distance_m):
    """
    Buffer a vector layer by `buffer_distance_m` and dissolve.
    """

    gdf = gpd.read_file(in_vector_path)
    gdf['geometry'] = gdf.buffer(buffer_distance_m)

    #Dissolve
    merged_geom = unary_union(gdf.geometry)
    dissolved_gdf = gpd.GeoDataFrame(geometry=[merged_geom], crs=gdf.crs)
    dissolved_gdf.to_file(out_vector_path, driver='GPKG')


def union_buffers(buffer_paths, out_union_path):
    """
    Merge multiple buffer layers into one polygon.
    """

    merged_polygons = []
    crs = None
    for path in buffer_paths:
        gdf = gpd.read_file(path)
        if crs is None:
            crs = gdf.crs
        merged_polygons.append(unary_union(gdf.geometry))
    
    unioned_poly = unary_union(merged_polygons)
    out_gdf = gpd.GeoDataFrame(geometry=[unioned_poly], crs=crs)
    out_gdf.to_file(out_union_path, driver='GPKG')


def mask_raster_by_polygon(in_raster_path, in_polygon_path, out_raster_path):
    """
    Mask a raster by a polygon. Keep only pixels inside the polygon.
    """

    with rasterio.open(in_raster_path) as src:
        polygon_gdf = gpd.read_file(in_polygon_path)
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
    """Calculate forest area by country and multiply by economic value."""

    gdf_countries = gpd.read_file(countries_path)
    df_values = pd.read_csv(value_csv_path)

    with rasterio.open(masked_raster_path) as src:
        if gdf_countries.crs != src.crs:
            gdf_countries = gdf_countries.to_crs(src.crs)
        
        stats = zonal_stats(
            gdf_countries,
            masked_raster_path,
            stats=["sum"],
            nodata=0
        )

        pixel_area_ha = (src.res[0] * src.res[1]) / 10000.0
    
    output_rows = []
    for i, stat in enumerate(stats):
        country_name = gdf_countries.iloc[i]['country_name']  # Ensure your shapefile has this column
        forest_pixel_sum = stat['sum'] if stat['sum'] else 0
        forest_area_ha = forest_pixel_sum * pixel_area_ha
        output_rows.append(
            {'country_name': country_name,
            'forest_area_ha': forest_area_ha}
        )
    
    df_area = pd.DataFrame(output_rows)
    df_merged = pd.merge(df_area, df_values, on='country_name', how='left')
    df_merged['total_value'] = df_merged['forest_area_ha'] * df_merged['value_per_hectare']
    df_merged.to_csv(out_csv_path, index=False)
    logging.info(f"Area-by-country results saved to {out_csv_path}")
