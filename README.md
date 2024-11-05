# LilDB
LilDB provides a simplified wrapper for SQLite3.

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
If you are not using a custom table (more on this below), then DB will collect data about the tables automatically and you can use them using the DB attributes. For example, if there is a 'Person' table in the database, then you can work with it through the 'person' attribute.
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

## Select data

Get all data from table:
```python
db.person.all()

# Equivalent to 'SELECT * FROM Person'

```

Get first three rows:
```python
db.person.select(size=3)
```

Iterate through the table:
```python
for row in db.person:
    row
```

Simple filter:
```python
db.person.select(salary=10, post="DevOps")

# Equivalent to 'SELECT * FROM Person WHERE salary = 10 AND post = "DevOps"'

db.person.select(id=1, post="DevOps", operator="OR")

# Equivalent to 'SELECT * FROM Person WHERE salary = 10 OR post = "DevOps"'
```

Get one row by id or position if id does not exist:
```python
db.person[1]

# or
db.person.get(id=1)
db.person.get(name="Ann")
```

Select specific columns:
```python
db.person.select(columns=["name", "id"])

# Equivalent to 'SELECT name, id FROM Person'
```

For more complex queries, use:
```python
db.person.select(condition="salary < 15")
# Equivalent to 'SELECT * FROM Person WHERE salary < 15'


db.person.select(columns=["name"], condition="salary < 15 or name = 'Ann'")
# Equivalent to 'SELECT name FROM Person WHERE salary < 15 or name = 'Ann''
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

## Multithreaded
You can use multithreaded using ThreadDB, example:
```python
from lildb import ThreadDB
from concurrent.futures import ThreadPoolExecutor, wait


db = ThreadDB("local.db")

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

persons = [
    {"name": "Sam", "email": "c@tst.com", "salary": 1.5, "post": "DevOps"},
    {"name": "Ann", "email": "a@tst.com", "salary": 15, "post": "Manager"},
    {"name": "Jim", "email": "b@tst.com", "salary": 10, "post": "Security"},
    {"name": "David", "email": "d@tst.com", "salary": 16, "post": "Developer"},
]

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(db.person.add, person) for person in persons]
    wait(futures)

# for close connection
db.close()
```

### How it work
#### Singleton
The Singleton pattern restricts the instantiation of a class to just one object. When you create an instance of the ThreadDB class, it checks if an instance already exists for the specified database. If one does, it returns the existing instance; otherwise, it creates a new instance for the specified database. This design pattern is particularly useful for managing database connections, as it provides a centralized point of access.

#### Thread Safety
To ensure multi-thread safety and prevent potential deadlocks, lildb utilizes an execution pipe. Whenever CRUD methods (Create, Read, Update, Delete) or custom SQL queries are called, the execution requests are sent to this pipe instead of directly accessing the database.

1) When a CRUD method or custom SQL query is invoked, lildb places the request in a queue that serves as the execution pipe.
2) In a separate execution thread, the requests are processed one by one from the execution pipe.
3) The separate thread reads the requests and executes them sequentially on the SQLite database.

## Custom rows, tables, db
If you want to create a custom class of rows or tables, then you can do it as follows:
```python
# We create custom row for table Post
from lildb.rows import dataclass_table
from lildb import Table
from lildb import DB
from lildb.column_types import Integer
from lildb.column_types import Text


@dataclass_table
class CustomPostRow:
    """Any custom data class row."""

    id: int
    name: str

    def title_post(self) -> str:
        """Any custom method."""
        return self.name.title()


class CustomPostTable(Table):
    """Any custom table class."""

    # Table name in DB
    name = "post"

    # Use custom data class row
    row_cls = CustomPostRow


class CustomDB(DB):
    """Custom DB."""

    post = CustomPostTable()


# Work with custom obj
db = CustomDB("post.db")

# Create table
db.create_table(
    "Post",
    {
        "id": Integer(),
        "name": Text(),
    },
    table_primary_key=("id", "name"),
)


print(db.post)
# <CustomPostTable: Post>

db.post.add({"id": 1, "name": "manager"})
db.post.add({"id": 2, "name": "developer"})

print(db.post.all())
# [CustomPostRow(id=1, name=manager), CustomPostRow(id=2, name=developer)]


row = db.post.get(id=1)
print(row.title_post())
# Manager

row.name = "admin"
row.change()

print(row.title_post())
# Admin

row.delete()

print(db.post.all())
# [CustomPostRow(id=2, name=developer)]
```

### dataclass_table
dataclass_table (from lildb.rows import dataclass_table) works the same way as 'dataclass' (from dataclasses), the only difference is that 'dataclass_table' adds two arguments and a mixin to work correctly.

If you don't want to use 'dataclass_table' then make your row-class as follows:
```python
from dataclasses import dataclass
from lildb import _RowDataClsMixin
from lildb import Table


@dataclass
class CustomPostRow(_RowDataClsMixin):
    """Any custom data class row."""

    id: int
    name: str

    # Required fields for row-cls
    table: Table
    changed_columns: set

    def title_post(self) -> str:
        """Any custom method."""
        return self.name.title()
```

### Custom Dict row
If you want to use dict instead of dataclass, you can do it like this
```python
from lildb import RowDict


class CustomPostRow(RowDict):
    """Any custom data class row."""

    def title_post(self) -> str:
        """Any custom method."""
        return self["name"].title()
```

### Custom select, insert, delete and update
The corresponding class is responsible for each CRUD operation. You can create your own instances in the following way

```python
# operation classes
from lildb import Select, Insert, Update, Delete

class CustomSelect(Select):
    """Custom select."""

    def get_manager(self) -> list:
        """Get all managers."""
        return self(name="manager")


class CustomPostTable(Table):
    """Any custom table class."""

    # Table name in DB
    name = "post"

    # Use custom data class row
    row_cls = CustomPostRow

    # Custom select
    select = CustomSelect

    # Custom other operation
    # insert = CustomInsert
    # update = CustomUpdate
    # delete = CustomDelete

```
