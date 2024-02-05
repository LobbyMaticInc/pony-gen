# Pony-Gen

Pony-Gen is a CLI tool designed to streamline the process of generating Pony ORM models from existing databases. With support for SQLite, MySQL, and PostgreSQL, u

## Acknowledgements

Pony-Gen builds upon the insights and methodologies developed by [Andrei Betkin](https://github.com/abetkin) in the [pony-inspect](https://github.com/abetkin/pony-inspect) project. We are grateful for the foundational work provided by pony-inspect, which has significantly contributed to the development of Pony-Gen.

## Contributing

Contributions are welcome! If you have ideas for improvements or have found a bug, please open an issue or submit a pull request.

## License

Pony-Gen is released under the MIT License. See the LICENSE file for more details.

## Installation

Please install this package in editable mode

```
pip install -e .
```

**Currently requires Python 3.11**

## Usage

The utility accepts a path to `pony.orm.Database` in a format similiar to gunicorn and uvicorn app factory pattern.

For example if you have

```python
# src/db.py

db = Database(provider='postgres', **the_rest)
```

Then run

```
pony-gen src.db:db
```

**Current limitations:**

- Many-to-many relations are not recognized, an intermediary table is generated
- Relations to tables with a composite primary key are not generated correctly
