<h1 align="center" style="border-bottom: none; text-align: center;">SGHI ETL Commons</h1>
<h3 align="center" style="text-align: center;">Collection of utilities for working with SGHI ETL Worflows.</h3>
<p align="center" style="text-align: center;">
    <a href="https://microsoft.github.io/pyright/">
        <img alt="Checked with pyright" src="https://microsoft.github.io/pyright/img/pyright_badge.svg">
    </a>
    <a href="https://github.com/astral-sh/ruff">
        <img alt="Ruff" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json">
    </a>
    <a href="https://github.com/pre-commit/pre-commit">
        <img alt="pre-commit" src="https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white">
    </a>
    <a href="https://github.com/savannahghi/sghi-etl-commons/blob/main/LICENSE">
        <img alt="GitHub License" src="https://img.shields.io/badge/License-MIT-blue.svg">
    </a>
</p>
<p align="center" style="text-align: center;">
    <a href="https://github.com/savannahghi/sghi-etl-commons/actions/workflows/ci.yml">
        <img alt="CI" src="https://github.com/savannahghi/sghi-etl-commons/actions/workflows/ci.yml/badge.svg">
    </a>
    <a href="https://coveralls.io/github/savannahghi/sghi-etl-commons?branch=main">
        <img alt="Coverage Status" src="https://coveralls.io/repos/github/savannahghi/sghi-etl-commons/badge.svg?branch=main">
    </a>
</p>

---

This project is part of [SGHI ETL](https://github.com/savannahghi/sghi-etl-core/)
projects. Specifically, this is a collection of common implementations of the
interfaces defined by the [sghi-etl-core](https://github.com/savannahghi/sghi-etl-core/)
project as well as other useful utilities.

## Contribute

Clone the project and run the following command to install dependencies:

```bash
pip install -e .[dev,test,docs]
```

Set up pre-commit hooks:
```bash
pre-commit install
```

## License

[MIT License](https://github.com/savannahghi/sghi-etl-commons/blob/main/LICENSE)

Copyright (c) 2024, Savannah Informatics Global Health Institute
