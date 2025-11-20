import numpy as np
import pygeoprocessing
import os
from osgeo import gdal, ogr, osr

#def forest_noforest_op(lulc_array):
    #"""
    #Return a binary array where forest = 1 and non-forest = 0, 
    #given that forest classes are between 50 and 90 inclusive.
    #"""
    # Handle nodata by checking if it's valid 
    # (we'll account for nodata in the raster_calculator call)
    # Then mask forest classes
    #return np.where((lulc_array >= 50) & (lulc_array <= 90), 1, 0)

#def main():
    # Paths to input and output
    #lulc_path = r"Input_data/lulc_esa_2020.tif"
    #binary_forest_path = r"output_data/forest_esa_2020.tif"

    # Read metadata of the input raster to get nodata value
    #input_raster_info = pygeoprocessing.get_raster_info(lulc_path)
    #input_nodata = input_raster_info['nodata'][0]

    # We can define an output nodata value for the binary raster
    # Typically 255 or -1 is used for Byte data, but 255 is common
    # or you can use 0 if you want no separation for "NoData".
    #out_nodata = 255

    # Use pygeoprocessing.raster_calculator to create the binary raster
    #pygeoprocessing.raster_calculator(
        #[(lulc_path, 1),  # (path, band_index)
        #(input_nodata, 'raw')],  # pass in the nodata so we can handle it
        #lambda lulc_array, in_nodata: np.where(
            #np.isclose(lulc_array, in_nodata),  # if it's nodata
            #out_nodata,  # preserve nodata in the output
            #forest_noforest_op(lulc_array)  # otherwise classify as 0/1
        #),
        #binary_forest_path,
        #gdal.GDT_Byte,  # output data type
        #out_nodata
    #)

    #print(f"Binary forest raster created: {binary_forest_path}")

#if __name__ == '__main__':
    #main()

import os
import numpy as np
import pygeoprocessing
from osgeo import gdal, ogr, osr

# -------------------------------------------------------------------
# Merge vector data into a GeoPackage instead of a Shapefile,
# while ensuring the geometry type is MULTILINESTRING and fixing empty field names.
# -------------------------------------------------------------------
def merge_shapefiles_ogr(shapefile_list, out_gpkg):
    """
    Merge multiple ESRI Shapefiles into one GeoPackage, using OGR Python bindings.
    Overwrites the output if it already exists.
    This function also forces geometries to MULTILINESTRING and renames any fields
    with an empty name.
    """
    # Use the GeoPackage driver instead of "ESRI Shapefile"
    driver = ogr.GetDriverByName("GPKG")

    # Remove existing output GeoPackage if it exists
    if os.path.exists(out_gpkg):
        os.remove(out_gpkg)

    # Open the first shapefile to copy schema & spatial reference
    in_ds = ogr.Open(shapefile_list[0], 0)
    if not in_ds:
        raise RuntimeError(f"Could not open {shapefile_list[0]}")
    in_layer = in_ds.GetLayer()
    in_srs = in_layer.GetSpatialRef()
    geom_type = in_layer.GetGeomType()
    # If the first layer is defined as LINESTRING, force the type to MULTILINESTRING
    if geom_type == ogr.wkbLineString:
        geom_type = ogr.wkbMultiLineString

    # Create the output GeoPackage
    out_ds = driver.CreateDataSource(out_gpkg)
    if not out_ds:
        raise RuntimeError(f"Could not create {out_gpkg}")

    # Use the base name of the GeoPackage (without extension) as the layer name.
    layer_name = os.path.splitext(os.path.basename(out_gpkg))[0]
    out_layer = out_ds.CreateLayer(layer_name, srs=in_srs, geom_type=geom_type)

    # Copy field definitions from the first input layer, checking for empty field names.
    in_layer_defn = in_layer.GetLayerDefn()
    for i in range(in_layer_defn.GetFieldCount()):
        field_defn = in_layer_defn.GetFieldDefn(i)
        field_name = field_defn.GetNameRef()
        if not field_name:
            # Rename the field if the name is empty.
            new_field_name = f"unnamed_field_{i}"
            field_defn.SetName(new_field_name)
        out_layer.CreateField(field_defn)

    # Copy features from the first shapefile
    for feature in in_layer:
        geom = feature.GetGeometryRef()
        # If the geometry is a LINESTRING, convert it to a MULTILINESTRING.
        if geom is not None and geom.GetGeometryName() == 'LINESTRING':
            geom = ogr.ForceToMultiLineString(geom)
            feature.SetGeometry(geom)
        out_feature = feature.Clone()
        out_layer.CreateFeature(out_feature)
    in_ds = None  # close the first dataset

    # Append features from the remaining shapefiles
    for shp in shapefile_list[1:]:
        ds = ogr.Open(shp, 0)
        if not ds:
            raise RuntimeError(f"Could not open {shp}")
        lyr = ds.GetLayer()
        for feature in lyr:
            geom = feature.GetGeometryRef()
            if geom is not None and geom.GetGeometryName() == 'LINESTRING':
                geom = ogr.ForceToMultiLineString(geom)
                feature.SetGeometry(geom)
            out_layer.CreateFeature(feature.Clone())
        ds = None

    out_ds = None  # close & flush to disk
    print(f"Merged {len(shapefile_list)} shapefiles into {out_gpkg}")

# -------------------------------------------------------------------
# Example usage for vector merging and the full processing workflow:
# -------------------------------------------------------------------

# Paths to input vector files (roads and rivers)
roads_shp = r"Input_data/globalroads.shp"
rivers_shp = r"Input_data/ne_10m_rivers_lake_centerlines.shp"
# Output GeoPackage for merged vector data
merged_gpkg = r"output_data/merged_roads_rivers.gpkg"

# Merge the input shapefiles into one GeoPackage
merge_shapefiles_ogr([roads_shp, rivers_shp], merged_gpkg)


def main():
    """
    Processing workflow:
    1) Merge roads + rivers vector data into one GeoPackage.
    2) Create a 0.18° resolution raster (WGS84) with roads+rivers burned as '1'.
    3) Compute an approximate distance transform.
    4) Threshold the distance transform.
    5) Reproject the thresholded raster to ~300m resolution.
    
    NOTE: This workflow uses approximate degrees for buffering.
    """

    # -----------------------------------------------------------------
    # 1) Input vector is the merged GeoPackage created above.
    # -----------------------------------------------------------------
    merged_vector = merged_gpkg

    # Define paths for intermediate and final rasters.
    roads_rivers_018deg = r"temp_data/roads_rivers_0.18deg.tif"
    roads_rivers_018deg_dist = r"temp_data/roads_rivers_0.18deg_distance.tif"
    roads_rivers_018deg_buffer = r"temp_data/roads_rivers_0.18deg_buffer.tif"
    roads_rivers_300m = r"output_data/buffer_300m.tif"

    # -----------------------------------------------------------------
    # 2) Create a 0.18° Raster in WGS84 and Rasterize the Vector Data
    # -----------------------------------------------------------------
    # Define a global bounding box: [-180, -90, 180, 90] with 0.18° pixels.
    minx, miny, maxx, maxy = -180.0, -90.0, 180.0, 90.0
    pixel_size_x = 0.18
    pixel_size_y = 0.18
    xsize = int((maxx - minx) / pixel_size_x)  # 360 / 0.18 = 2000
    ysize = int((maxy - miny) / pixel_size_y)  # 180 / 0.18 = 1000

    # Set up the geotransform (note the negative pixel height for lat/lon)
    geotransform = [
        minx,          # top-left x
        pixel_size_x,  # pixel width
        0.0,           # rotation
        maxy,          # top-left y
        0.0,           # rotation
        -pixel_size_y  # pixel height (negative)
    ]

    # Define the spatial reference (WGS84)
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)
    wgs84_wkt = wgs84_srs.ExportToWkt()

    # Create a new blank raster (GeoTIFF) to hold the rasterized vector.
    driver = gdal.GetDriverByName('GTiff')
    target_raster = driver.Create(
        roads_rivers_018deg, 
        xsize, 
        ysize, 
        1, 
        gdal.GDT_Byte,
        options=["TILED=YES", "COMPRESS=DEFLATE"]
    )
    target_raster.SetGeoTransform(geotransform)
    target_raster.SetProjection(wgs84_wkt)
    band = target_raster.GetRasterBand(1)
    band.SetNoDataValue(255)
    band.Fill(0)  # initialize with 0
    band = None
    target_raster = None  # flush and close

    # Rasterize the merged vector data (burn value 1)
    pygeoprocessing.rasterize(
        vector_path=merged_vector,
        target_raster_path=roads_rivers_018deg,
        burn_values=[1],
        option_list=["ALL_TOUCHED=TRUE"]
    )
    print(f"Rasterized roads + rivers into: {roads_rivers_018deg}")

    # -----------------------------------------------------------------
    # 3) Compute a Distance Transform (Approximate, in Degrees)
    # -----------------------------------------------------------------
    pygeoprocessing.distance_transform_edt(
        (roads_rivers_018deg, 1),
        roads_rivers_018deg_dist,
        working_no_data=255  # nodata value from the raster
    )
    print(f"Distance transform created (in degrees): {roads_rivers_018deg_dist}")

    # -----------------------------------------------------------------
    # 4) Threshold the Distance to ~0.09° (~10 km at the equator)
    # -----------------------------------------------------------------
    buffer_degs = 0.09  # threshold distance in degrees

    def threshold_distance_op(distance_array):
        # Preserve nodata (255); if distance <= 0.09, output 1; otherwise, 0.
        return np.where(
            distance_array == 255,
            255,
            np.where(distance_array <= buffer_degs, 1, 0)
        )

    pygeoprocessing.raster_calculator(
        [(roads_rivers_018deg_dist, 1)],
        threshold_distance_op,
        roads_rivers_018deg_buffer,
        gdal.GDT_Byte,
        255  # nodata value
    )
    print(f"Created buffer (0/1) raster at 0.18°: {roads_rivers_018deg_buffer}")

    # -----------------------------------------------------------------
    # 5) Reproject & Warp the Buffer Raster to ~300m Resolution
    # -----------------------------------------------------------------
    # Approximately, 1° ≈ 111 km, so ~300 m is about 0.0027°.
    target_pixel_size_deg = 0.0027

    pygeoprocessing.warp_raster(
        roads_rivers_018deg_buffer,
        new_cell_size=(target_pixel_size_deg, -target_pixel_size_deg),
        target_raster_path=roads_rivers_300m,
        resample_method='near',
        target_bb=[minx, miny, maxx, maxy],
        target_projection_wkt=wgs84_wkt,
        n_threads=1,
        gtiff_creation_options=["TILED=YES", "COMPRESS=DEFLATE"],
        working_dir=os.path.dirname(roads_rivers_300m)
    )
    print(f"Reprojected buffer raster to ~300 m resolution: {roads_rivers_300m}")

    print("\nAll steps complete.")

if __name__ == '__main__':
    main()
