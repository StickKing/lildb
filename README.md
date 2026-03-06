# LilDB
LilDB provides a simplified wrapper for SQLite3.


## Quick start

### ORM

```python
from __future__ import annotations

from typing import Optional, List
from lildb.column_types import Integer, Text
from lildb.orm import MColumn, RelationForeignKey, Relation, TColumn
from lildb import DB


class Base:
    """Base model columns."""

    id: TColumn[int] = MColumn(Integer(primary_key=True))


@DB.register_table
class Tag(Base):
    """Tag model."""

    title: TColumn[str]
    persons: TColumn[List["Person"]] = Relation("PersonTag", "tag", "person")


@DB.register_table
class PersonTag:
    """M2m table."""

    person_id: TColumn[int]
    tag_id: TColumn[int]
    person: TColumn["Person"] = RelationForeignKey("person_id", "Person", "id")
    tag: TColumn["Tag"] = RelationForeignKey("tag_id", "Tag", "id")


@DB.register_table
class Person(Base):
    """Person model."""

    name: TColumn[Optional[str]]
    orders: TColumn[List["Order"]] = Relation("Order", "person")
    tags: TColumn[List[Tag]] = Relation("PersonTag", "person", "tag")


@DB.register_table
class Order(Base):
    """Order model."""

    title: TColumn[Optional[str]]

    person_id: TColumn[Optional[int]]
    person: TColumn[Person] = RelationForeignKey("person_id", "Person", "id")


db = DB("person.db")  # Tables will be created automatically

new_person = Person(
    name="David",
    orders=[Order(title="hello"), Order(title="world")],
    tags=[Tag(title="best"), Tag(title="good")],
)

db.add(new_person)  # Add new person

david = db.query(Person).first()  # Get first person

print(david)
# PersonModel(id=1, name=David)

print(david.orders)
# [OrderModel(id=1, person_id=1, title=hello), OrderModel(id=2, person_id=1, title=world)]

print(david.tags)
# [TagModel(id=1, title=best), TagModel(id=2, title=good)]

david.name = "Tom"
david.tags = [Tag(title="other tag")]

david.change()  # Update model

tom = db.query(Person).where(id=1).first()
print(tom)
# PersonModel(id=1, name=Tom)

print(tom.tags)
# [TagModel(id=3, title=other tag)]

tom.delete()  # Delete person

print(db.query(Person).all())
# []

```

### Manual
```python
from lildb import DB
from lildb.column_types import Integer, Text, ForeignKey


db = DB("person_manual.db")

# Create tables

db.create_table(
    "Tag",
    {
        "id": Integer(primary_key=True),
        "title": Text(),
    },
)

db.create_table(
    "PersonTag",
    {
        "person_id": Integer(),
        "tag_id": Integer(),
    },
    foreign_keys=(
        ForeignKey("person_id", "Person", "id"),
        ForeignKey("tag_id", "Tag", "id"),
    ),
    table_primary_key=(
        "person_id",
        "tag_id",
    )
)

db.create_table(
    "Person",
    {
        "id": Integer(primary_key=True),
        "name": Text(),
    },
)

db.create_table(
    "Order",
    {
        "id": Integer(primary_key=True),
        "title": Text(),
        "person_id": Integer(nullable=True),
    },
)


db.person.add({"name": "David"})  # Add person

db.order.add(  # Add person orders
    {"title": "hello", "person_id": 1},
    {"title": "world", "person_id": 1},
)

db.tag.add(  # Add person tags
    {"title": "best"},
    {"title": "good"},
)

db.persontag.add(
    {"person_id": 1, "tag_id": 1},
    {"person_id": 1, "tag_id": 2},
)

david = db.person.query().first()  # Get first person

print(david)
# RowPersonDataClass(id=1, name=David)

david_orders = db.order.query().where(person_id=david.id).all()
print(david_orders)
# [RowOrderDataClass(id=1, title=hello, person_id=1), RowOrderDataClass(id=2, title=world, person_id=1)]


# Get tags
person_tab_tb = db.persontag

david_tag_ids_query = person_tab_tb.query(
    person_tab_tb.c.tag_id,
).where(
    person_id=david.id,
)

david_tag_ids = [
    row.tag_id
    for row in david_tag_ids_query.all()
]

david_tags = db.tag.query().where(db.tag.c.id.in_(david_tag_ids)).all()
print(david_tags)
# [RowTagDataClass(id=1, title=best), RowTagDataClass(id=2, title=good)]


david.name = "Tom"
david.change()  # Update person
db.persontag.delete(person_id=david.id)
new_tag = db.tag.add({"title": "other tag"}, returning=True)
db.persontag.add({"tag_id": new_tag.id, "person_id": david.id})

david.delete()

print(db.person.query().all())
# []
```




## Query for ORM
You can use 'query' to create a more complex sql-query. But it unstable.

```python
# Return all data from person table
db.query(Person).all()

# Return first row from person table
db.query(Person).first()


# Use id and name column
db.query(Person.id, Person.name)

# Use sql func on column
db.query(Person.name.length())
db.query(Person.name.lower())
db.query(Person.id.max())
db.query(Person).where(Person.id.is_(None))
db.query(Person).where(Person.id.in_([1, 2]))

# Use other funcs
from lildb.sql import func

db.query(func.abs(Person.id))
db.query(func.distinct(Person.name))
db.query(func.lower(Person.name))
db.query(Person).where(
    func.like(Person.name, "Dav%") | (Person.id == 3)
)

db.query(Person.name.upper().label("upper_name"))
# SELECT UPPER(`Person`.name) AS upper_name FROM Person

# Return data with id = 1
db.query(Person).where(id=1)
# Alternative
db.query(Person).where(Person.id == 1)
db.query(Person).where(condition="id = 1")

db.query(Person).where(id=1, name="David", filter_operator="AND")

# Various conditions
db.query(Person).where(
    (Person.name == "David") | (Person.id == 2)
)

query = db.query(Person)
# Limit data
query = query.limit(10).offset(2)

# Group by data
query = query.group_by(Person.id)

# Order data
query = query.order_by(Person.id)

# Check exists
query.exists()

# Check row count
query.count()
```

## Query for manual
You can use 'query' to create a more complex sql-query. But it unstable.

```python
# Fetch person table obj
person_tb = db.person

# 'all' and 'first' executing query

# Return all data from person table
db.person.query().all()

# Return first row from person table
db.person.query().first()


# Use id and name column
db.person.query(person_tb.c.id, person_tb.c.name)

# Use sql func on column
db.person.query(person_tb.c.name.length())
db.person.query(person_tb.c.name.lower())
db.person.query(person_tb.c.id.max())
db.person.query(person_tb.c.id.is_(None))
db.person.query(person_tb.c.id.in_([1, 2]))

# Use other funcs
from lildb.sql import func

db.person.query(func.abs(person_tb.c.id))
db.person.query(func.distinct(person_tb.c.name))
db.person.query(func.lower(person_tb.c.name))
db.person.query().where(
    func.like(person_tb.c.name, "Dav%") | (person_tb.c.id == 3)
)

db.person.query(person_tb.c.name.upper().label("upper_name"))
# SELECT UPPER(`Person`.name) AS upper_name FROM Person

# Return data with id = 1
db.person.query().where(id=1)
# Alternative
db.person.query().where(person_tb.c.id == 1)
db.person.query().where(condition="id = 1")

db.person.query().where(id=1, name="David", filter_operator="AND")

# Various conditions
db.person.query().where(
    (person_tb.c.name == "David") | (person_tb.c.id == 2)
)

query = db.person.query()
# Limit data
query.limit(10).offset(2)

# Group by data
query.group_by(person_tb.c.salary)

# Order data
query.order_by(person_tb.c.id)

# Check exists
query.exists()

# Check row count
query.count()
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


## Other docs
1. [manual docs](./docs/manual.md)