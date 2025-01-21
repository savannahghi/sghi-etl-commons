## [1.2.0-rc.2](https://github.com/savannahghi/sghi-etl-commons/compare/v1.2.0-rc.1...v1.2.0-rc.2) (2025-01-20)


### Features

* **utils:** add retrying support when running workflows ([#37](https://github.com/savannahghi/sghi-etl-commons/issues/37)) ([7912aac](https://github.com/savannahghi/sghi-etl-commons/commit/7912aac35b30c25c44b2ede8e7fbeb35e59bb1a4))

## [1.2.0-rc.1](https://github.com/savannahghi/sghi-etl-commons/compare/v1.1.0...v1.2.0-rc.1) (2024-12-28)


### Dependency Updates

* **deps:** upgrade project dependencies ([#31](https://github.com/savannahghi/sghi-etl-commons/issues/31)) ([3279164](https://github.com/savannahghi/sghi-etl-commons/commit/3279164ee701f33228d22e033a985c1a8ccd9cda))


### Features

* **core:** add support for `prologue` and `epilogue` workflow properties ([#34](https://github.com/savannahghi/sghi-etl-commons/issues/34)) ([b8fa894](https://github.com/savannahghi/sghi-etl-commons/commit/b8fa89428c6218119e1e1b6fe051e984db265aaf))
* support running workflow instances directly ([#35](https://github.com/savannahghi/sghi-etl-commons/issues/35)) ([8363850](https://github.com/savannahghi/sghi-etl-commons/commit/8363850b3e1d115b7522926bf3a55c6e9bc4a3df))

## [1.1.0](https://github.com/savannahghi/sghi-etl-commons/compare/v1.0.0...v1.1.0) (2024-11-17)


### Features

* **processors:** add a `ScatterGatherProcessor` ([#19](https://github.com/savannahghi/sghi-etl-commons/issues/19)) ([a7d4387](https://github.com/savannahghi/sghi-etl-commons/commit/a7d43871ebbf071324ccac2acf86ae61a71b0b2d))
* **processors:** add a `SplitGatherProcessor` ([#17](https://github.com/savannahghi/sghi-etl-commons/issues/17)) ([4439c11](https://github.com/savannahghi/sghi-etl-commons/commit/4439c11724fe1630d2f53e3cc4be8ebc798ff352))
* **sinks:** add a `ScatterSink` ([#21](https://github.com/savannahghi/sghi-etl-commons/issues/21)) ([6274a31](https://github.com/savannahghi/sghi-etl-commons/commit/6274a31ec774e5fd720938360702af8e44994d63))
* **sinks:** add a `SplitSink` ([#20](https://github.com/savannahghi/sghi-etl-commons/issues/20)) ([ce4d4ee](https://github.com/savannahghi/sghi-etl-commons/commit/ce4d4ee0e3bf1d7b302f4c08f98acd12993ce832))
* **sources:** add a `GatherSource` ([#15](https://github.com/savannahghi/sghi-etl-commons/issues/15)) ([d64635a](https://github.com/savannahghi/sghi-etl-commons/commit/d64635a66fa058c9c26007d281620491de8b9a7e))
* **utils:** add a workflow runner utility ([4f26bc9](https://github.com/savannahghi/sghi-etl-commons/commit/4f26bc9f2b24c2db1acff289c7545b0dc68d0203))
* **workflow-builder:** add a `WorkflowBuilder` ([#27](https://github.com/savannahghi/sghi-etl-commons/issues/27)) ([53aee40](https://github.com/savannahghi/sghi-etl-commons/commit/53aee40cb9f8573b9b681a455fb745bf665f5195))


### Refactors

* **deps-dev:** bump braces from 3.0.2 to 3.0.3 ([#29](https://github.com/savannahghi/sghi-etl-commons/issues/29)) ([118f4ba](https://github.com/savannahghi/sghi-etl-commons/commit/118f4ba9498cf2267c679e65ae5474d01b247140))
* **processors:** delay embedded processors disposal ([#25](https://github.com/savannahghi/sghi-etl-commons/issues/25)) ([035261c](https://github.com/savannahghi/sghi-etl-commons/commit/035261c83e498ce6f902e8bc6931cc60619b89fa))
* **sinks:** delay embedded sinks disposal ([#26](https://github.com/savannahghi/sghi-etl-commons/issues/26)) ([5df425d](https://github.com/savannahghi/sghi-etl-commons/commit/5df425d210076518c9ba327df476e8504e31c5a9))
* **sources:** delay embedded sources disposal ([#24](https://github.com/savannahghi/sghi-etl-commons/issues/24)) ([7f1e585](https://github.com/savannahghi/sghi-etl-commons/commit/7f1e58560e79cfd8b5e9a3b9c87a3b7c98b212d3))

## [1.1.0-rc.3](https://github.com/savannahghi/sghi-etl-commons/compare/v1.1.0-rc.2...v1.1.0-rc.3) (2024-11-17)


### Features

* **workflow-builder:** add a `WorkflowBuilder` ([#27](https://github.com/savannahghi/sghi-etl-commons/issues/27)) ([53aee40](https://github.com/savannahghi/sghi-etl-commons/commit/53aee40cb9f8573b9b681a455fb745bf665f5195))


### Refactors

* **processors:** delay embedded processors disposal ([#25](https://github.com/savannahghi/sghi-etl-commons/issues/25)) ([035261c](https://github.com/savannahghi/sghi-etl-commons/commit/035261c83e498ce6f902e8bc6931cc60619b89fa))
* **sinks:** delay embedded sinks disposal ([#26](https://github.com/savannahghi/sghi-etl-commons/issues/26)) ([5df425d](https://github.com/savannahghi/sghi-etl-commons/commit/5df425d210076518c9ba327df476e8504e31c5a9))
* **sources:** delay embedded sources disposal ([#24](https://github.com/savannahghi/sghi-etl-commons/issues/24)) ([7f1e585](https://github.com/savannahghi/sghi-etl-commons/commit/7f1e58560e79cfd8b5e9a3b9c87a3b7c98b212d3))

## [1.1.0-rc.2](https://github.com/savannahghi/sghi-etl-commons/compare/v1.1.0-rc.1...v1.1.0-rc.2) (2024-06-04)


### Features

* **processors:** add a `ScatterGatherProcessor` ([#19](https://github.com/savannahghi/sghi-etl-commons/issues/19)) ([a7d4387](https://github.com/savannahghi/sghi-etl-commons/commit/a7d43871ebbf071324ccac2acf86ae61a71b0b2d))
* **sinks:** add a `ScatterSink` ([#21](https://github.com/savannahghi/sghi-etl-commons/issues/21)) ([6274a31](https://github.com/savannahghi/sghi-etl-commons/commit/6274a31ec774e5fd720938360702af8e44994d63))
* **sinks:** add a `SplitSink` ([#20](https://github.com/savannahghi/sghi-etl-commons/issues/20)) ([ce4d4ee](https://github.com/savannahghi/sghi-etl-commons/commit/ce4d4ee0e3bf1d7b302f4c08f98acd12993ce832))
* **utils:** add a workflow runner utility ([4f26bc9](https://github.com/savannahghi/sghi-etl-commons/commit/4f26bc9f2b24c2db1acff289c7545b0dc68d0203))

## [1.1.0-rc.1](https://github.com/savannahghi/sghi-etl-commons/compare/v1.0.0...v1.1.0-rc.1) (2024-05-08)


### Features

* **processors:** add a `SplitGatherProcessor` ([#17](https://github.com/savannahghi/sghi-etl-commons/issues/17)) ([4439c11](https://github.com/savannahghi/sghi-etl-commons/commit/4439c11724fe1630d2f53e3cc4be8ebc798ff352))
* **sources:** add a `GatherSource` ([#15](https://github.com/savannahghi/sghi-etl-commons/issues/15)) ([d64635a](https://github.com/savannahghi/sghi-etl-commons/commit/d64635a66fa058c9c26007d281620491de8b9a7e))

## [1.0.0](https://github.com/savannahghi/sghi-etl-commons/compare/...v1.0.0) (2024-04-16)


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
