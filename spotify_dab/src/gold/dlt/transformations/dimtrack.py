import dlt

@dlt.table
def dimtrack_staging():
    df = spark.readStream.table("spotify_cata.silver.dimtrack")
    return df
# here the staging table will be created and here we didn't mentioned table, so it automatically take table name as a function name(dimuser_staging)



dlt.create_streaming_table("dimtrack")
# here we creating actual table 


dlt.create_auto_cdc_flow(
   target="dimtrack",
   source="dimtrack_staging",
   keys=["track_id"], # Unique identifier(s)
   sequence_by="updated_at", # Ordering column for CDC events
   stored_as_scd_type=2, # SCD Type 1 for overwrite behavior
   track_history_except_column_list = None,
   name = None,
   once = False
)