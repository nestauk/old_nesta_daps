Working with the Data Quality Package
=====================================

A quick introduction on the data quality package.

Setting up
------------
Import packages using Python

### Prerequisites- Clone Repo
```
git clone <repo_url>
cd <directory of repo>
git checkout data_quality
pip install -e . 
```
Using the Package
------------

## The Modules
As these are python modules, this will ideally be used in a jupyter notebook or terminal. These modules are:
- `general` - includes functions that give a missing data diagnosis.
- `text` - functions that look at string-like objects.
- `nested` - to work with data that consists of list objects.
- `dt` - to work with DateTime objects.
- `geo` - fucntions to work with lat/lon coordinates.


### Importing

```
from nesta.packages.dq.general import *
from nesta.packages.dq.text import *
from nesta.packages.dq.nested import *
from nesta.packages.dq.dt import *
from nesta.packages.dq.geo import *
```
