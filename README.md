Interface to the SAR avalanche dataset. 

**NOTE**: Currently only working locally on `tos-1040`. 

# Setup: 
Pull mongo docker image: 
```bash
docker pull mongo:latest
```

Run database server: 
```bash
docker run -d -p 27017:27017 -v /ssd_data/skreddata/db:/data/db --name skreddata-mongo mongo:latest
```

# Usage
## Access database through skreddata python API
Make conda environment with appropriate python dependencies: 
```bash
conda create --name skreddata-db ipython pymongo shapely python-dateutil
```

Activate conda environment: 
```bash
conda activate skreddata-db
```
Access database in python: 
```python
from skreddata import database
db = database.Database()
```

Test interfacing the database, for example, by getting the total length: 
```python
print(db.get_length())
```


## Access database with mongo shell directly

```bash 
docker exec -it skreddata-mongo mongosh
```

