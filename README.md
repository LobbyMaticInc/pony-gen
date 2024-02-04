# Pony-Gen

Pony-Gen is a CLI tool designed to streamline the process of generating Pony ORM models from existing databases. With support for SQLite, MySQL, and PostgreSQL, u

## Acknowledgements

Pony-Gen builds upon the insights and methodologies developed by [Andrei Betkin](https://github.com/abetkin) in the [pony-inspect](https://github.com/abetkin/pony-inspect) project. We are grateful for the foundational work provided by pony-inspect, which has significantly contributed to the development of Pony-Gen.

## Contributing

Contributions are welcome! If you have ideas for improvements or have found a bug, please open an issue or submit a pull request.

## License

Pony-Gen is released under the MIT License. See the LICENSE file for more details.

## Installation

Please install this package in editable mode and `git pull` frequently

```
pip install -e .
```

**Currently requires Python 3 and PostgreSQL database**

## Usage

The utility accepts path to `pony.orm.Database` object for `--database` argument.

For example if you have

```python
# app/db.py

db = Database(provider='postgres', **the_rest)
```

Then run

```
python -m introspect --database app.db.db
```

There are some **examples** in the examples dir. To run them:

```
cd examples
# Fill the parameters of your database connection in simple.py & corporate_directory.py
python -m introspect --database=corporate_directory.db > out/corporate_directory.py
# or
python -m introspect --database=simple.db > out/simple.py
```

**Current limitations:**

- Many-to-many relations are not recognized, an intermediary table is generated
- Relations to tables with a composite primary key are not generated correctly
