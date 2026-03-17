Interface to the SAR avalanche dataset. 

**NOTE**: Currently only working locally on `tos-1040`. 

# Setup
Pull mongo docker image:
```bash
docker pull mongo:latest
```

Run database server:
```bash
docker run -d -p 27017:27017 -v /ssd_data/skreddata/db:/data/db --name skreddata-mongo mongo:latest
```

# Usage
## Start database
Unless running, start the database:
```bash
docker start skreddata-mongo
```


## Access database through skreddata python API
Create the local environment with UV:
```bash
uv sync
```

Access the database in Python:
```python
from skreddata import database
db = database.Database()
```

Test interfacing the database, for example, by getting the total length:
```python
print(db.get_length())
```

You can also run one-off commands without activating a shell:
```bash
uv run python -c "from skreddata import database; print(database.Database().get_length())"
```

## Build docker image
Build the image with Docker:
```bash
docker build -t skreddata .
```

## Access database with mongo shell directly

```bash 
docker exec -it skreddata-mongo mongosh
```
