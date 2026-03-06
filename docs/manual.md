# !!! Documentation is being finalized !!!

## Connection.

You can connect to the database in two ways: the usual way and using the context manager.

Usual way:
```python
from lildb import DB

db = DB("local.db")

# Disconnect
db.close()
```

Context manager:
```python
from lildb import DB


with DB("local.db") as db:
    # do anything
    ...
# Disconnect
```

DB automatically collects information about existing tables, and allows you to present data in the form of dict or dataclass.

By default db returns data as dict, you can change that with 'use_datacls' flag.
```python
from lildb import DB

# Dict rows
db = DB("local.db")

# DataClass rows
db = DB("local.db", use_datacls=True)
```

## About table
DB will collect data about the tables automatically and you can use them using the DB attributes. For example, if there is a 'Person' table in the database, then you can work with it through the 'person' attribute.
```python
db = DB("local.db")

db.person
print(db.person)
# <Table: Person>

```


## Create table
Simple create table without column types:
```python
db.create_table("Person", ("name", "post", "email", "salary", "img"))

# Equivalent to 'CREATE TABLE IF NOT EXISTS Person(name, post, email, salary, img)'
```


#### Advanced create table
If you want use more features take this:
```python
from lildb.column_types import Integer, Real, Text, Blob

db.create_table(
    "Person",
    {
        "id": Integer(primary_key=True),
        "name": Text(nullable=True),
        "email": Text(unique=True),
        "post": Text(default="Admin"),
        "salary": Real(default=10000),
        "img": Blob(nullable=True),
    },
)

# Equivalent to 'CREATE TABLE IF NOT EXISTS Person (id INTEGER PRIMARY KEY NOT NULL, name TEXT, email TEXT NOT NULL UNIQUE, post TEXT DEFAULT 'Admin' NOT NULL, salary REAL DEFAULT 10000 NOT NULL, img BLOB)'


db.create_table(
    "Post",
    {
        "id": Integer(),
        "name": Text(),
    },
    table_primary_key=("id", "name"),
)

# Equivalent to 'CREATE TABLE IF NOT EXISTS Post (id INTEGER NOT NULL, name TEXT NOT NULL, PRIMARY KEY(id,name))'
```

## Insert data

Add new row:
```python
db.person.insert({
    "name": "David",
    "email": "tst@email.com",
    "salary": 15.5,
    "post": "Manager",
})

# or
db.person.add({
    "name": "David",
    "email": "tst@email.com",
    "salary": 15.5,
})

# Equivalent to 'INSERT INTO Person (name, email, salary) VALUES(?, ?, ?)'
```

Add many rows:
```python
persons = [
    {"name": "Ann", "email": "a@tst.com", "salary": 15, "post": "Manager"},
    {"name": "Jim", "email": "b@tst.com", "salary": 10, "post": "Security"},
    {"name": "Sam", "email": "c@tst.com", "salary": 1.5, "post": "DevOps"},
]

db.person.insert(persons)

# or
db.person.add(persons)
```

## Update data

Change one row"
```python
row = db.person[1]

# if use dict row
row["post"] = "Developer"
row.change()

# if use data class row
row.post = "Developer"
row.change()
```

Update column value in all rows
```python
db.person.update({"salary": 100})
```

```python
# Change David post
db.person.update({"post": "Admin"}, id=1)
```

Simple filter
```python
db.person.update({"post": "Developer", "salary": 1}, id=1, name="David")

db.person.update(
    {"post": "Admin", "salary": 1},
    name="Ann",
    id=1,
    operator="or",
)
# Equivalent to 'UPDATE Person SET post = "Ann", salary = 1 WHERE name = 'Ann' or id = 1'
```

## Delete data

Delete one row
```python
row = db.person[1]
row.delete()
```

Simple filter delete
```python
db.person.delete(id=1, name="David")
```

Delete all rows with salary = 1
```python
db.person.delete(salary=1)

db.person.delete(salary=10, name="Sam", operator="OR")
# Equivalent to 'DELETE FROM Person WHERE salary = 10 OR name = "Sam"'
```