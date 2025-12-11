import os
import logging
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.ops import unary_union
import hazelbean as hb
import pygeoprocessing as pgp
from rasterstats import zonal_stats
import subprocess


logging.basicConfig(level=logging.INFO)
L = logging.getLogger('ntfp_functions')


def create_forest_mask(input_lulc_path, output_forest_path):
    """
    Creates a binary mask for forest classes (50-90).
    Returns 1 for forest pixels, 0 for non-forest.
    """

    if os.path.exists(output_forest_path):
        L.info(f"Forest mask already exists at {output_forest_path}")
        return

    nodata_val = 0

    def forest_mask_op(lulc_array):
        """
        Pixel-level operation: classify LULC values into forest (classes 50-90) vs. non-forest.
        Returns 1 (not class IDs, so that zonal_stats.sum() counts pixels) for forest, 0 for non-forest.
        """
        return np.where((lulc_array >= 50) & (lulc_array <= 90), 1, 0).astype(np.uint8)
    
    # Apply the forest mask operation to the input raster
    # using hazelbean's memory-efficient raster calculator.
    # Processes the raster in blocks (typically 256x256 pixels)
    # to fit in RAM.
    hb.raster_calculator_hb(
        [(input_lulc_path, 1)],
        forest_mask_op,  # custom pixel operation function
        output_forest_path,
        hb.gdal_number_to_gdal_type[1],  # GDT_Byte
        nodata_val,  # nodata value for o/p
        gtiff_creation_options=hb.globals.DEFAULT_GTIFF_CREATION_OPTIONS,
        calc_raster_stats=False
    )

    L.info(f"Forest mask created at {output_forest_path}")


def reproject_raster(in_raster_path, out_raster_path, target_crs_wkt, target_pixel_size, target_bbox=None):
    """
    Reproject a raster to a new coordinate reference system (CRS)
    and resample to target pixel size.
    Preserve the nodata value from input during reprojection.
    """

    if os.path.exists(out_raster_path):
        L.info(f"Reprojected raster already exists at {out_raster_path}")
        return

    # Retrieve metadata from input raster to preserve nodata value
    input_info = hb.get_raster_info_hb(in_raster_path)
    input_nodata = input_info['nodata'][0] if input_info['nodata'] else 0
    
    L.info(f"Reprojecting {in_raster_path}")
    L.info(f"  Input nodata: {input_nodata}")
    L.info(f"  Target pixel size: {target_pixel_size}")

    # Perform the actual reprojection using hazelbean's warp function
    hb.warp_raster_hb(
        in_raster_path,
        target_pixel_size,
        out_raster_path,
        'near',  # nearest-neighbor resampling (preserves categorical data like forest mask)
        target_sr_wkt=target_crs_wkt,
        target_bb=target_bbox,
        gtiff_creation_options=hb.globals.DEFAULT_GTIFF_CREATION_OPTIONS,
        calc_raster_stats=False
    )

    # After reprojection, GDAL may lose nodata metadata.
    # gdal_edit.py is used to explicitly set the nodata value in the output raster.
    # Ensures that zonal_stats will correctly ignore nodata pixels when
    # calculating area statistics. Without this step, zonal_stats treats nodata
    # as valid 0 values, returning incorrect results.
    try:
        cmd = ['gdal_edit.py', '-a_nodata', str(int(input_nodata)), out_raster_path]
        subprocess.run(cmd, check=True, capture_output=True)
        L.info(f"Set nodata metadata to {int(input_nodata)} on reprojected raster")
    except FileNotFoundError:
        L.warning("gdal_edit.py not found. Skipping nodata metadata fix.")
        L.warning("This may cause zonal_stats to return all zeros.")
    except Exception as e:
        L.warning(f"Could not set nodata metadata: {e}")

    # Verify
    output_info = hb.get_raster_info_hb(out_raster_path)
    L.info(f"Reprojected raster saved to {out_raster_path}")
    L.info(f"  Output nodata: {output_info['nodata']}")


def reproject_vector(in_vector_path, out_vector_path, target_crs_wkt):
    """Reprojects a vector to target CRS."""

    if os.path.exists(out_vector_path):
        L.info(f"Reprojected vector already exists at {out_vector_path}")
        return

    # Use pygeoprocessing's vector reprojection function.
    pgp.reproject_vector(
        base_vector_path=in_vector_path,
        target_projection_wkt=target_crs_wkt,
        target_path=out_vector_path,
        driver_name='GPKG'
    )
    L.info(f"Reprojected vector to {out_vector_path}")


def buffer_vector(in_vector_path, out_vector_path, buffer_distance_m):
    """
    Buffer a vector layer by a specified distance
    and dissolve overlapping buffers into one polygon.
    """

    if os.path.exists(out_vector_path):
        L.info(f"Buffered and dissolved vector already exists at {out_vector_path}")
        return

    gdf = gpd.read_file(in_vector_path)
    gdf['geometry'] = gdf.buffer(buffer_distance_m)  # expand each line/polygon by the specified distance
    merged_geom = unary_union(gdf.geometry)  # merge all buffered geometries into one polygon
    dissolved_gdf = gpd.GeoDataFrame(geometry=[merged_geom], crs=gdf.crs)  # create output GDF with single dissolved geometry
    dissolved_gdf.to_file(out_vector_path, driver='GPKG')

    L.info(f"Buffered and dissolved vector saved to {out_vector_path}")


def union_buffers(buffer_paths, out_union_path):
    """
    Union multiple buffer layers into one polygon.
    """

    if os.path.exists(out_union_path):
        L.info(f"Union of buffers already exists at {out_union_path}")
        return

    # Collect all geometries from all buffer files
    all_geoms = []
    crs = None
    for path in buffer_paths:
        gdf = gpd.read_file(path)
        if crs is None:
            crs = gdf.crs
        all_geoms.extend(gdf.geometry.tolist())
    
    unioned = unary_union(all_geoms)  # union all geometries into one

    out_gdf = gpd.GeoDataFrame(geometry=[unioned], crs=crs)
    out_gdf.to_file(out_union_path, driver='GPKG')

    L.info(f"Unioned buffers saved to {out_union_path}")


def mask_raster_by_polygon(in_raster_path, in_polygon_path, out_raster_path):
    """Mask a raster to keep only pixels within a polygon
    while setting external pixels to nodata."""

    if os.path.exists(out_raster_path):
        L.info(f"Masked raster already exists at {out_raster_path}")
        return
    
    # Get raster metadata (data type, nodata value, dimensions)
    raster_info = hb.get_raster_info(in_raster_path)

    # Create temporary mask raster (binary: 1 inside polygon, 0 outside)
    temp_mask_path = out_raster_path.replace('.tif', '_temp_mask.tif')

    # Rasterize the polygon to create a binary mask aligned with the input raster
    hb.create_valid_mask_from_vector_path(
        in_polygon_path,
        in_raster_path,
        temp_mask_path,
        all_touched=False
    )

    def apply_mask(mask_array, raster_array):
        """
        Apply binary mask to raster.
        Keep raster values where mask==1, set to nodata where mask==0.
        """
        return np.where(mask_array == 1, raster_array, raster_info['nodata'][0])
    
    # Apply the mask operation using raster_calculator_hb
    hb.raster_calculator_hb(
        [(temp_mask_path, 1), (in_raster_path, 1)],
        apply_mask,
        out_raster_path,
        raster_info['datatype'],
        raster_info['nodata'][0],
        calc_raster_stats=False
    )

    if os.path.exists(temp_mask_path):
        os.remove(temp_mask_path)  # clean up temporary mask raster
    
    L.info(f"Masked raster saved to {out_raster_path}")


def area_by_country(masked_raster_path, countries_reproj_path,
                    value_csv_path, out_csv_path):
    """
    Calculate forest area by country using zonal statistics and match to prices via ISO3.
    Handles multiple polygons per country by aggregating to one row per ISO3.
    Filters out countries with zero NTFP value.
    """

    if os.path.exists(out_csv_path):
        L.info(f"Output already exists: {out_csv_path}")
        return

    L.info("=" * 80)
    L.info("=== Starting Area-by-Country Calculation (merge on ISO3) ===")
    L.info("=" * 80)

    # Load data
    gdf = gpd.read_file(countries_reproj_path)
    df_price = pd.read_csv(value_csv_path)
    raster_info = hb.get_raster_info(masked_raster_path)
    nodata_val = raster_info['nodata'][0]

    L.info(f"Loaded {len(gdf)} country polygons from {countries_reproj_path}")
    L.info(f"Loaded {len(df_price)} unique countries from {value_csv_path}")
    L.info(f"Raster extent: {raster_info['bounding_box']}")
    L.info(f"Raster pixel size: {raster_info['pixel_size']}")
    L.info(f"GeoDataFrame columns: {gdf.columns.tolist()}")
    L.info(f"CSV columns: {df_price.columns.tolist()}")

    # Validate required columns
    if 'iso3_r250_label' not in gdf.columns:
        raise ValueError(
            f"Vector missing 'iso3_r250_label' column. Available: {gdf.columns.tolist()}"
        )
    if 'iso3_r250_label' not in df_price.columns:
        raise ValueError(
            f"CSV missing 'iso3_r250_label' column. Available: {df_price.columns.tolist()}"
        )

    gdf_reset = gdf.reset_index(drop=True)

    L.info("\n=== Vector Info ===")
    L.info(f"Unique ISO3 codes in vector: {gdf_reset['iso3_r250_label'].nunique()}")
    L.info(f"Sample iso3_r250_label values:\n{gdf_reset[['iso3_r250_label']].head(10)}")

    # ===== Compute Zonal Statistics =====
    L.info("\n=== Computing Zonal Statistics ===")

    stats_list = zonal_stats(
        gdf_reset,
        masked_raster_path,
        stats=['sum', 'count'],
        nodata=nodata_val
    )

    df_stats = pd.DataFrame(stats_list)
    df_stats['iso3_r250_label'] = gdf_reset['iso3_r250_label'].values

    L.info("\n=== Aggregating Forest Statistics by ISO3 ===")
    L.info(f"Before aggregation: {len(df_stats)} rows (one per polygon)")

    df_stats_agg = (
        df_stats
        .groupby('iso3_r250_label', as_index=False)
        .agg({
            'sum': 'sum',  # total forest pixels per country
            'count': 'sum'  # total polygons counted per country
        })
    )  # aggregate by ISO3 to eliminate duplicates

    L.info(f"After aggregation: {len(df_stats_agg)} rows (one per unique ISO3)")
    df_stats = df_stats_agg

    L.info(f"Zonal stats aggregated. Sample:\n{df_stats[['iso3_r250_label', 'sum', 'count']].head()}")

    # ===== Area Calculation =====
    pixel_area_m2 = abs(raster_info['pixel_size'][0] * raster_info['pixel_size'][1])
    pixel_area_ha = pixel_area_m2 / 10000.0

    df_stats['forest_pixel_count'] = df_stats['sum'].fillna(0).astype(int)
    df_stats['forest_area_ha'] = df_stats['forest_pixel_count'] * pixel_area_ha

    L.info(f"\nPixel area: {pixel_area_ha:.2f} ha")
    L.info(f"Total forest pixels found: {df_stats['forest_pixel_count'].sum()}")
    L.info(f"Total forest area (all countries): {df_stats['forest_area_ha'].sum():.2f} ha")

    # ===== Price Integration =====
    L.info("\n=== Preparing Price Data ===")

    df_price_clean = df_price[
        df_price['iso3_r250_label'].notna()
        ].copy()  # keep only records with valid ISO3 and numeric prices
    
    df_price_clean['value_per_hectare'] = pd.to_numeric(
        df_price_clean['2019'], errors='coerce'
    )  # extract 2019 NTFP value per hectare

    L.info(f"Sample price data:\n"
           f"{df_price_clean[['iso3_r250_id', 'iso3_r250_label', 'iso3_r250_name', 'value_per_hectare']].head()}")

    # ===== Merge Forest Area with Price Data =====
    L.info("\n=== Merging Forest Area with Price Data (on ISO3) ===")

    df_final = pd.merge(
        df_stats[['iso3_r250_label', 'forest_area_ha']],
        df_price_clean[['iso3_r250_id', 'iso3_r250_label', 'iso3_r250_name', 'value_per_hectare']],
        on='iso3_r250_label',
        how='inner'
    )

    L.info(f"After merge: {len(df_final)} countries with both forest area AND price data")

    # ===== Value Calculation =====
    df_final['ntfp'] = (
        df_final['forest_area_ha'] * df_final['value_per_hectare']
    )

    L.info("\n=== Filtering Zero-Value Records ===")
    before_filter = len(df_final)
    df_final = df_final[
        #(df_final['ntfp'] > 0) & 
        (df_final['ntfp'].notna())
    ].copy()  # remove rows with zero or NaN NTFP
    after_filter = len(df_final)
    
    L.info(f"Removed {before_filter - after_filter} countries with ntfp == 0 or NaN")
    L.info(f"Final output: {after_filter} countries")

    # ===== Export with iso3_r250_id =====
    df_final = df_final[
        ['iso3_r250_id', 'iso3_r250_label', 'iso3_r250_name', 
         'forest_area_ha', 'value_per_hectare', 'ntfp']
    ]

    df_final.to_csv(out_csv_path, index=False)

    L.info("\n=== Output Saved ===")
    L.info(f"Results saved to: {out_csv_path}")

    L.info("\n=== Summary Statistics ===")
    L.info(f"Total countries in output: {len(df_final)}")
    L.info(f"Countries with forest area > 0: "
           f"{(df_final['forest_area_ha'] > 0).sum()}")
    L.info(f"Countries with valid price data: "
           f"{(df_final['value_per_hectare'].notna()).sum()}")

    total_forest = df_final['forest_area_ha'].sum()
    total_value = df_final['ntfp'].sum()
    L.info(f"Global forest area (with data): {total_forest:.2f} ha")
    L.info(f"Global total NTFP value: ${total_value:,.0f}")

    L.info("\n=== First 10 Rows ===")
    L.info(f"\n{df_final.head(10).to_string()}")
    L.info("\n=== Last 10 Rows ===")
    L.info(f"\n{df_final.tail(10).to_string()}")

    return df_final