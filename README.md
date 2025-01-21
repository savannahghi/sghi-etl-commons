<h1 align="center" style="border-bottom: none; text-align: center;">SGHI ETL Commons</h1>
<h3 align="center" style="text-align: center;">Collection of utilities for working with SGHI ETL Workflows.</h3>

<div align="center" style="text-align: center;">

![Python Version from PEP 621 TOML](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Fsavannahghi%2Fsghi-etl-commons%2Fdevelop%2Fpyproject.toml&logo=python&labelColor=white)
[![Checked with pyright](https://microsoft.github.io/pyright/img/pyright_badge.svg)](https://microsoft.github.io/pyright/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Semantic Release: conventionalcommits](https://img.shields.io/badge/semantic--release-conventionalcommits-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
[![GitHub License](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/savannahghi/sghi-etl-commons/blob/develop/LICENSE)

</div>

<div align="center" style="text-align: center;">

[![CI](https://github.com/savannahghi/sghi-etl-commons/actions/workflows/ci.yml/badge.svg)](https://github.com/savannahghi/sghi-etl-commons/actions/workflows/ci.yml)
[![Coverage Status](https://img.shields.io/coverallsCoverage/github/savannahghi/sghi-etl-commons?branch=develop&logo=coveralls)](https://coveralls.io/github/savannahghi/sghi-etl-commons?branch=develop)

</div>

---

This project is part of [SGHI ETL](https://github.com/savannahghi/sghi-etl-core/)
projects. Specifically, this is a collection of common implementations of the
interfaces defined by the [sghi-etl-core](https://github.com/savannahghi/sghi-etl-core/)
project as well as other useful utilities.

## Getting Started

`sghi-etl-commons` supports Python 3.11+.

To install the latest stable release, use:

```shell
pip install sghi-etl-commons@git+https://github.com/savannahghi/sghi-etl-commons.git@v1.1.0
```

To install the latest development version, use:

```shell
pip install sghi-etl-commons@git+https://github.com/savannahghi/sghi-etl-commons.git@develop
```

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
