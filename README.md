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

### Installation

`sghi-etl-commons` supports Python 3.11+.

To install the latest stable release, use:

```shell
pip install sghi-etl-commons@git+https://github.com/savannahghi/sghi-etl-commons.git@v1.2.0
```

To install the latest development version, use:

```shell
pip install sghi-etl-commons@git+https://github.com/savannahghi/sghi-etl-commons.git@develop
```

### Usage

**Example 1**

Here is the famous hello-world version of SGHI ETL.

```python
from sghi.etl.commons import *

wb = WorkflowBuilder[str, str](id="say_hello", name="Hello World")

@source
def say_hello() -> str:
    return "Hello, World!"

wb.draw_from(say_hello).drain_to(sink(print))
run_workflow(wb)
```
We start by importing the components needed to define an SGHI ETL workflow.
The package `sghi.etl.commons` contains everything we need to define and run
simple SGHI ETL workflows. The `WorkflowBuilder` class, as its name suggests,
is a helper for constructing SGHI ETL Workflows declaratively. We also have the
`source` and `sink` decorators. The `source` decorator is used to mark or wrap
functions that supply data. The `sink` decorator is used to mark or wrap
functions that consume data.

All SGHI ETL Workflows are required to have a unique identifier and a
name(preferably human-readable). In the example above, we create a
`WorkflowBuilder` instance whose `id` is `say_hello` and whose name is
`Hello World`. The generic type hints indicate that the created
`WorkflowBuilder` instance accepts a source that produces a string and accepts
a sink that consumes a string. There are cases where you might have a sink that
produces one type and a sink that consumes a different type, but let us not get
ahead of ourselves.

In our example, we define a simple source that returns the string "Hello,
World!". We then add it to the `WorkflowBuilder` instance using the
`WorkflowBuilder.draw_from` method. Next, we wrap the Python built-in `print`
function with the sink decorator thereby creating a suitable sink for our
workflow. This is then connected to the `WorkflowBuilder` using the `drain_to`
method. Finally, we execute the workflow using the `run_workflow` function.
This causes the text "Hello, World!" to be printed.

---

**Example 2**

Working with large datasets.

```python
import random
from collections.abc import Iterable

from sghi.etl.commons import *

wb: WorkflowBuilder[Iterable[int], Iterable[int]]
wb = WorkflowBuilder(id="print_10_ints", name="Print 10 Integers")

@source
def supply_ints() -> Iterable[int]:
    for _ in range(10):
        yield random.randint(0, 9)

@sink
def print_each(values: Iterable[int]) -> None:
    for value in values:
        print(value)

wb.draw_from(supply_ints).drain_to(print_each)
run_workflow(wb)
```

This example builds on the previous example but uses streams of data instead of
single/scalar values.
The `supply_ints` source could potentially produce millions of values,
but the workflow would remain memory efficient since it works with only one
value at a time.

---

**Example 3**

Complex workflows.

```python
import random
from collections.abc import Iterable, Sequence

from sghi.etl.commons import *

wb: WorkflowBuilder[Iterable[int], Sequence[str]]
wb = WorkflowBuilder(
    id="complex_workflow",
    name="Complex Workflow",
    composite_processor_factory=ProcessorPipe,
    composite_sink_factory=ScatterSink,
)

# SOURCES
# ----------------------------------------------------------------------
@wb.draws_from
@source
def supply_ints() -> Iterable[int]:
    for _ in range(10):
        yield random.randint(0, 9)

# PROCESSORS
# ----------------------------------------------------------------------
@wb.applies_processor
@processor
def add_100(values: Iterable[int]) -> Iterable[int]:
    for v in values:
        yield v + 100

@wb.applies_processor
@processor
def ints_as_strings(ints: Iterable[int]) -> Iterable[str]:
    yield from map(chr, ints)

@wb.applies_processor
@processor
def values_to_sequence(values: Iterable[str]) -> Sequence[str]:
    return list(values)

# SINKS
# ----------------------------------------------------------------------
@wb.drains_to
@sink
def print_each(values: Sequence[str]) -> None:
    for value in values:
        print(value)

@wb.drains_to
@sink
def print_all(values: Sequence[str]) -> None:
    print(f"[{", ".join(list(values))}]")

run_workflow(wb)
```

This example demonstrates how complex workflows can be constructed by combining
multiple components, namely sources, sinks, and processors. It also introduces
a new SGHI ETL Workflow component, the `processor`. A `processor` is applied to
data drawn from a source to transform it into a more suitable form before the
data is drained to a sink. Occasionally, a processor can output data of a
different type than it received, hence the two generic type hints on the
`WorkflowBuilder` class.

When multiple components of the same kind are added to the same workflow, they
form a composite component. In our example, the three processors are combined
to create a `ProcessorPipe`, as indicated by the `composite_processor_factory`
constructor parameter of the `WorkflowBuilder` class. A `ProcessorPipe` "pipes"
data through its child processors, similar to the UNIX pipe. That is, the output
of one child processor is passed as the input to the next one.
The two sinks are combined to form a `ScatterSink`, as indicated by the
`composite_sink_factory` constructor parameter of the `WorkflowBuilder` class.
A `ScatterSink` drains (the same) data to its child sinks concurrently. There
exist other composite processors, sinks, and even sources. You can also build
your own.

Finally, this example illustrates that we can define a workflow by decorating
workflow components using specific methods of a `WorkflowBuilder` instance.
Note that the names of these methods are in the third-person singular form.

## Contribute

Clone the project and run the following command to install dependencies:

```shell
cd sghi-etl-commons
pip install -e .[dev,test,docs]

# Set up pre-commit hooks:
pre-commit install
```
### Testing and linting

To run tests and lint the project, make sure you have installed the `test`
optional dependencies:

```shell
cd sghi-etl-commons
pip install -e .[test]
tox -e py
# Alternatively, you can run the following commands directly:
# ruff check .
# ruff format --check .
# pyright .
# pytest .
```

### Building the documentation

The documentation uses [Sphinx](http://www.sphinx-doc.org/):

```shell
cd sghi-etl-commons
pip install -e .[docs]
tox -e docs

# Serve the docs
python -m http.server -d docs/build/html
```

## License

[MIT License](https://github.com/savannahghi/sghi-etl-commons/blob/main/LICENSE)

Copyright (c) 2024, Savannah Informatics Global Health Institute
