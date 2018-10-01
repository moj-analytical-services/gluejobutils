# Changelog

## v1.0.1 - 2018-10-01

### Removed

- Deleted dates module

## v1.0.0 - 2018-08-30

### Changed

- `create_spark_schema_from_metadata` and `create_spark_schema_from_metadata_file` functions: `exclude_cols` input parameter has been renamed to `drop_columns` to be clearer.
- `align_df_to_meta` function: ignore_columns and drop_columns have been added see doc string.
- `standard_datetime` format variables have been moved to utils module from dea_record_datetimes module as felt it was more fitting to have them in utils. 

## v0.0.1 - 2018-08-23

### Added

- Initial release