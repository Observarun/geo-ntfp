import os
import hazelbean as hb
import ntfp_tasks


# Build Task Tree

def build_task_tree(p):

    # Create the forest mask from the raw LULC 2020 data
    p.task_preprocess = p.add_task(ntfp_tasks.task_create_forest_mask)

    # Reproject all inputs (Roads, Rivers, Forest) to Mollweide
    p.task_reproject = p.add_task(ntfp_tasks.task_reproject_inputs)
    
    # Create buffers and union them
    p.task_buffer = p.add_task(ntfp_tasks.task_buffer_and_union)
    
    # Mask forest and calculate final stats
    p.task_stats = p.add_task(ntfp_tasks.task_mask_and_calculate_stats)


if __name__ == '__main__':
    
    # Create the project flow object
    p = hb.ProjectFlow()


    # Set directories.
    p.user_dir = os.path.expanduser('~')
    p.extra_dirs = ['Files', 'global_invest', 'projects']
    p.project_name = 'ntfp_' + hb.pretty_time()
    p.project_dir = os.path.join(p.user_dir, os.sep.join(p.extra_dirs), p.project_name)
    p.set_project_dir(p.project_dir)

    # Set base_data_dir. Will download required files here.
    p.base_data_dir = os.path.join(p.user_dir, 'Files', 'base_data', 'submissions', 'ntfp')

    # Set model paths for global processing
    p.aoi = 'global'

    # Set model paths
    p.raw_lulc_path = p.get_path(os.path.join(p.base_data_dir, "../../lulc/esa/lulc_esa_2019.tif"))
    p.roads_shp = p.get_path(os.path.join(p.base_data_dir, "global_roads/globalroads.shp"))
    p.rivers_shp = p.get_path(os.path.join(p.base_data_dir, "global_rivers/ne_10m_rivers_lake_centerlines.shp"))
    p.countries_shp = p.get_path(os.path.join(p.base_data_dir, "../../cartographic/ee/ee_r264_correspondence.gpkg"))
    p.value_csv = p.get_path(os.path.join(p.base_data_dir, "/nontimber_price_iucn_edited.csv"))
    p.buffer_distance_m = 10000

    # Build the task tree and execute it.
    build_task_tree(p)
    p.execute()