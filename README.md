# LilDB
LilDB provides a simplified wrapper for SQLite3.

## Connection.
Data from tables can be presented by dict or dataclass. You can change that with 'use_datacls' flag.
```
from lildb import DB

# Dict rows
db = DB("local.db")

# DataClass rows
db = DB("local.db", use_datacls=True)
```


## Create table
Simple create table without column types:
```
db.create_table("Person", ("name", "post", "email", "salary", "img"))

# Equivalent to 'CREATE TABLE IF NOT EXISTS Person(name, post, email, salary, img)'
```


#### Advanced create table
If you want use more features take this:
```
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

Add one row:
```
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
```
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
```
db.person.all()

# Equivalent to 'SELECT * FROM Person'

```

Get first three rows:
```
db.person.select(size=3)
```

Iterate through the table:
```
for row in db.person:
    row
```

Simple filter:
```
db.person.select(salary=10, post="DevOps")

# Equivalent to 'SELECT * FROM Person WHERE salary = 10 AND post = "DevOps"'

db.person.select(id=1, post="DevOps", operator="OR")

# Equivalent to 'SELECT * FROM Person WHERE salary = 10 OR post = "DevOps"'
```

Get one row by id or position if id does not exist:
```
db.person[1]

# or
db.person.get(id=1)
db.person.get(name="Ann")
```

Select specific columns:
```
db.person.select(columns=["name", "id"])

# Equivalent to 'SELECT name, id FROM Person'
```

For more complex queries, use:
```
db.person.select(condition="salary < 15")
# Equivalent to 'SELECT * FROM Person WHERE salary < 15'


db.person.select(columns=["name"], condition="salary < 15 or name = 'Ann'")
# Equivalent to 'SELECT name FROM Person WHERE salary < 15 or name = 'Ann''
```

## Update data

Change one row"
```
row = db.person[1]

# if use dict row
row["post"] = "Developer"
row.change()

# if use data class row
row.post = "Developer"
row.change()
```

Update column value in all rows
```
db.person.update({"salary": 100})
```

```
# Change David post
db.person.update({"post": "Admin"}, id=1)
```

Simple filter
```
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
```
row = db.person[1]
row.delete()
```

Simple filter delete
```
db.person.delete(id=1, name="David")
```

Delete all rows with salary = 1
```
db.person.delete(salary=1)

db.person.delete(salary=10, name="Sam", operator="OR")
# Equivalent to 'DELETE FROM Person WHERE salary = 10 OR name = "Sam"'
```
