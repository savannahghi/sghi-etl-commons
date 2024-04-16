## [1.0.0-rc.1](https://github.com/savannahghi/sghi-etl-commons/compare/...v1.0.0-rc.1) (2024-04-16)


### Dependency Updates

* **deps:** upgrade project dependencies ([#2](https://github.com/savannahghi/sghi-etl-commons/issues/2)) ([6bee92c](https://github.com/savannahghi/sghi-etl-commons/commit/6bee92caa6464c52ee03c0a609dec5fd5b919fea))


### Features

* **processors:** add a `ProcessorPipe` ([#10](https://github.com/savannahghi/sghi-etl-commons/issues/10)) ([82a006e](https://github.com/savannahghi/sghi-etl-commons/commit/82a006ee58bfacc723e3bed2612114f002276719))
* **processors:** add a NOOP `Processor` ([#3](https://github.com/savannahghi/sghi-etl-commons/issues/3)) ([ecdf529](https://github.com/savannahghi/sghi-etl-commons/commit/ecdf529bbf091dc3d88060331ba6dc66703118f5))
* **processors:** add a processor decorator ([#4](https://github.com/savannahghi/sghi-etl-commons/issues/4)) ([9f3b039](https://github.com/savannahghi/sghi-etl-commons/commit/9f3b0393630c2bae8df568f4a0fd977f1d34effc))
* **sinks:** add a `NullSink` ([#9](https://github.com/savannahghi/sghi-etl-commons/issues/9)) ([2794f6a](https://github.com/savannahghi/sghi-etl-commons/commit/2794f6ab198d9a2e211cd57f3e6ab6c6aab0a815))
* **sinks:** add a sink decorator ([#7](https://github.com/savannahghi/sghi-etl-commons/issues/7)) ([6a89c85](https://github.com/savannahghi/sghi-etl-commons/commit/6a89c85d30e8a28abc995823df9fc06aa8d52f14))
* **sources:** add a source decorator ([#5](https://github.com/savannahghi/sghi-etl-commons/issues/5)) ([b42328e](https://github.com/savannahghi/sghi-etl-commons/commit/b42328e7c94bf975df4e2ddde454bdad13d8bae7))
* **utils:** add result gatherers ([#1](https://github.com/savannahghi/sghi-etl-commons/issues/1)) ([7c4e34f](https://github.com/savannahghi/sghi-etl-commons/commit/7c4e34f738e9b8774d7c1d7ba3e6386229b25786))
* **worflow_def:** add a `SimpleWorkflowDefinition` ([#11](https://github.com/savannahghi/sghi-etl-commons/issues/11)) ([ce1a9b0](https://github.com/savannahghi/sghi-etl-commons/commit/ce1a9b03a6fa6e0b6ab75a8635c98e0b8267be86))


### Refactors

* **processors:** mark `processors._ProcessorOfCallable` as final ([#6](https://github.com/savannahghi/sghi-etl-commons/issues/6)) ([9e5d6c6](https://github.com/savannahghi/sghi-etl-commons/commit/9e5d6c6d50a8e92cb2fe35537edf866dd1098d15))
* **sinks:** rename `sinks._SourceOfCallable` class ([#8](https://github.com/savannahghi/sghi-etl-commons/issues/8)) ([e6f08e5](https://github.com/savannahghi/sghi-etl-commons/commit/e6f08e5f9edb25067529980d9d13f7e3d75a3847))
