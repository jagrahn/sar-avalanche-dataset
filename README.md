# skreddata

Small Python API and CLI for the `skreddata` MongoDB database.

## 1. Create UV environment

```bash
uv sync
```

## 2. Start MongoDB

Create a local directory for the database files:

```bash
mkdir -p /localscratch/work/jgra/skreddata/mongo
```

Use either Podman or Docker:

```bash
podman run -d \
  --name skreddata-mongo \
  -p 27017:27017 \
  -v /localscratch/work/jgra/skreddata/mongo:/data/db \
  docker.io/library/mongo:latest
```

```bash
docker run -d \
  --name skreddata-mongo \
  -p 27017:27017 \
  -v /localscratch/work/jgra/skreddata/mongo:/data/db \
  mongo:latest
```

Defaults:
`host=localhost`, `port=27017`, `database=skreddata`, `collection=avl-v20230607`

## 3. Optional: Add existing data

If someone sent you an existing `skreddata.archive.gz`:

```bash
uv run skreddata restore /NORCE/Data/600/60090/long_lived_JGRA/skreddata/database/mongo/skreddata.archive.gz --container skreddata-mongo --drop
```

If you also received curated dataset files for `skredlab`, keep them in a separate dataset directory, for example:

```bash
/NORCE/Data/600/60090/long_lived_JGRA/skreddata/
```

## 4. Access database via Python

```python
from skreddata import Database

db = Database(
    host="localhost",
    port=27017,
    database="skreddata",
    collection="avl-v20230607",
)

print(db.get_length())
print(db.get_by_uuid("54B9904B-B837-4EC8-ADBA-50A16AE759EA"))
```

## 5. Access database via CLI

```bash
uv run skreddata ping
uv run skreddata count
uv run skreddata get 54B9904B-B837-4EC8-ADBA-50A16AE759EA
uv run skreddata list --limit 5
```

## 6. Back up database

```bash
mkdir -p /NORCE/Data/600/60090/long_lived_JGRA/skreddata/database/mongo

uv run skreddata backup /NORCE/Data/600/60090/long_lived_JGRA/skreddata/database/mongo/skreddata.archive.gz --container skreddata-mongo
```

Without `--container`, `skreddata` expects `mongodump` and `mongorestore` on `PATH`.
The `--container` option works the same with a MongoDB container started by Podman or Docker.
