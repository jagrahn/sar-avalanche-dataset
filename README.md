# skreddata

Small Python API and CLI for the `skreddata` MongoDB database.

## 1. Create UV environment

```bash
uv sync
```

## 2. Start database

```bash
podman run -d \
  --name skreddata-mongo \
  -p 27017:27017 \
  -v skreddata-mongodb-data:/data/db \
  docker.io/library/mongo:latest
```

Defaults:
`host=localhost`, `port=27017`, `database=skreddata`, `collection=avl-v20230607`

## 3. Access database via Python

```python
from skreddata import Database

db = Database()
print(db.get_length())
print(db.get_by_uuid("SOME_UUID"))
```

## 4. Access database via CLI

```bash
uv run skreddata ping
uv run skreddata count
uv run skreddata get SOME_UUID
uv run skreddata list --limit 5
```

## 5. Back up database

```bash
uv run skreddata backup /NORCE/Data/600/60090/long_lived_JGRA/from_lysorgel/skreddata/database/mongo/skreddata.archive.gz --container skreddata-mongo
uv run skreddata restore /NORCE/Data/600/60090/long_lived_JGRA/from_lysorgel/skreddata/database/mongo/skreddata.archive.gz --container skreddata-mongo --drop
```

Without `--container`, `skreddata` expects `mongodump` and `mongorestore` on `PATH`.
