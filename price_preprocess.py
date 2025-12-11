import pandas as pd

# Load both CSV files
gep_df = pd.read_csv('ecosystem-service_gep.csv')
nontimber_df = pd.read_csv('nontimber_price_iucn.csv')

# Merge iso3_r250_id from GEP ES file into nontimber price file
merged_df = nontimber_df.merge(
    gep_df[['iso3_r250_label', 'iso3_r250_name', 'iso3_r250_id']], 
    left_on='iso3', 
    right_on='iso3_r250_label', 
    how='left'
)

# Drop duplicate iso3_r250_label column from merge
merged_df = merged_df.drop('iso3_r250_label', axis=1)

# Rename columns as requested
merged_df = merged_df.rename(columns={'iso3': 'iso3_r250_label'})

# Replace country_name with iso3_r250_name from GEP file
merged_df = merged_df.drop('country_name', axis=1)

# Delete rows where columns 1995 to 2020 have zero values
year_cols = [str(year) for year in range(1995, 2021)]

# mask_all_zero = (merged_df[year_cols] == 0).all(axis=1)  # uncomment to filter out rows where ALL year columns are zero
initial_rows = len(merged_df)
# merged_df = merged_df[~mask_all_zero]  # uncomment to filter out rows where ALL year columns are zero
# rows_deleted = initial_rows - len(merged_df)  # uncomment to filter out rows where ALL year columns are zero

# Reorder columns to have identifiers first
id_cols = ['iso3_r250_id', 'iso3_r250_label', 'iso3_r250_name']
other_cols = [col for col in merged_df.columns if col not in id_cols]
merged_df = merged_df[id_cols + other_cols]

# Save the result
merged_df.to_csv('nontimber_price_iucn_edited.csv', index=False)