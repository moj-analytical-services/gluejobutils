# Changelog

## v2.0.0 - 2019-06-13

### Changed

- dea_record_datetimes has been renamed to record_datetimes
- dropped `dea_` prefix from record_datetimes module
- functions now give option to add col_prefix (can set to `dea_`) to get old functionality (aka expected colnames from previous versions)
- Applied python black to scripts

## v1.0.4 - 2019-06-13

### Changed

- Deleted `setup.py` added `pyproject.toml`. Published to Pypi.

## v1.0.3 - 2019-04-05

### Changed

- `write_json_to_s3` function now takes two additional arguments `indent` and `seperators` (optional). These are passed to the `json.dump` function. Defaults are set to what they were inside the function so backwards compatible with previous versions.

## v1.0.2 - 2018-12-03

### Added new module

- `df_transforms` module now has one function called apply_overwrite_dict_to_df. Which applies a json dictionary of fixes to a spark df.

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