# LilDB
LilDB provides a simplified wrapper for SQLite3.

## Connection.
Data from tables can be obtained either in the form of dict or in the form of dataclass
```
from lildb import DB

# Dict rows
db = DB("local.db")

# DataClass rows
db = DB("local.db", use_datacls=True)
```


## Create table
#### Simple create table.
```
db.create_table("Person", ("name", "post", "email", "salary", "img"))
```


#### Advanced create table
```
from lildb.column_types import Integer, Real, Text, Blob

db.create_table(
    "Person",
    {
        "id": Integer(primary_key=True),
        "name": Text(nullable=True),
        "email": Text(unique=True),
        "post": Text(default=""),
        "salary": Real(default=10000),
        "img": Blob(nullable=True),
    },
)

db.create_table(
    "Post",
    {
        "id": Integer(),
        "name": Text(),
    },
    table_primary_key=("id", "name"),
)
```

## Insert data

#### Add one row
```
db.person.insert({
    "name": "David",
    "email": "tst@email.com",
    "salary": 15.5,
    "post": "Manager",
})
```

#### Add many rows
```
persons = [
    {"name": "Ann", "email": "a@tst.com", "salary": 15, "post": "Manager"},
    {"name": "Jim", "email": "b@tst.com", "salary": 10, "post": "Security"},
    {"name": "Sam", "email": "c@tst.com", "salary": 1.5, "post": "DevOps"},
]

db.person.insert(persons)
```

## Select and view data

#### Get all data from table
```
db.person.all()
```

#### Get first rows by size
```
db.person.select(size=3)
```

#### Iterate through the table
```
for row in db.person:
    row
```

#### Simple filter
```
db.person.select(id=1, post="DevOps", operator="OR")
db.person.select(salary=10, post="DevOps")
```

#### Get one row by id or position if id does not exist
```
db.person[1]
```

## Update data

#### Change one row
```
row = db.person[1]

# if use dict row
row["post"] = "Developer"
row.change()

# if use data class row
row.post = "Developer"
row.change()
```

#### Update column value in all rows
```
db.person.update({"salary": 100})
```

```
# Change David post
db.person.update({"post": "Admin"}, id=1)

# Change Ann post and salary
db.person.update({"post": "Admin", "salary": 1}, name="Ann")
```

#### Simple filter
```
db.person.update({"post": "Developer", "salary": 1}, id=1, name="David")
```

## Delete row

#### Delete one row
```
row = db.person[1]
row.delete()
```

#### Simple filter delete
```
db.person.delete(id=1, name="David")
```

#### Delete all rows with salary = 1
```
db.person.delete(salary=1)
```
