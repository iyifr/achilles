# ChromaDB Compatibility

## Compatible

* Collection methods: `add`, `get`, `query`, `update`, `upsert`, `delete`, `peek`, `count`
* Parameter names and types
* Result object structure
* Embedding function interface
* Basic metadata filtering (`$and`, `$or`, `$in`, `$eq`, `$gt`, etc.)

---

## Differences

### 1. Database Management

**ChromaDB:**
```python
client = chromadb.Client()
collection = client.create_collection("docs")
```

**AchillesDB:**
```python
admin = AdminClient(host="localhost", port=8180)
admin.create_database("mydb")

client = Client(host="localhost", port=8180, database="mydb")
collection = client.create_collection("docs")
```

---

### 2. Client Structure

* **ChromaDB**: Single `Client()` for everything
* **AchillesDB**: `AdminClient()` for databases, `Client()` for collections

---

### 3. Metadata Filtering

**AchillesDB adds:**
* `$arrContains`: Check array field contains any of multiple values
  ```json
  {"tags": {"$arrContains": ["python", "go"]}}
  ```

**ChromaDB uses:**
* `$contains`: Check array field contains single value
  ```python
  {"tags": {"$contains": "python"}}
  ```

---

### 4. Port

* **ChromaDB**: Default port 8000
* **AchillesDB**: Default port 8180

---

## Migration Steps

1. Replace `chromadb.Client()` with `AchillesClient(database="db_name")`
2. Add database creation step using `AdminClient`
3. Update host/port configuration
4. Collection operations remain unchanged

Collection-level code requires minimal changes.
