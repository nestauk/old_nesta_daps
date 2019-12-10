Working with the Data Quality Package
=====================================

A quick introduction on the data quality package.

Setting up
------------

### Prerequisites- Clone Repo
```
git clone <repo_url>
cd <directory of repo>
git checkout data_quality
pip install -e . 
```
Using the Package
------------

### The Modules
As these are python modules, this will ideally be used in a jupyter notebook or terminal. These modules are:
- `general` functions: - includes functions that give a missing data diagnosis.
  - `missing_values`
  - `missing_value_percentage_column_count`
  - `missing_value_count_pair_both`
  - `missing_value_count_pair_either`
  
- `text` - functions that look at string-like objects
  - `string_counter`
  - `string_length`
  
- `nested` - to work with data that consists of list objects.
  - `array_length`
  - `word_array_calc`
  - `word_arrays`

- `dt` - to work with DateTime objects.
  - `calendar_day_distribution`
  - `month_year_distribution`
  - `year_distribution`

- `geo` - fucntions to work with lat/lon coordinates.
  - `latlon_distribution`
  - `_geojson_to_columns`
  
#### Importing

```
from nesta.packages.dq import general, text, nested, dt, geo
```
