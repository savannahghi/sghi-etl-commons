"""A :class:`WorkflowDefinition` builder class."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from attrs import define, field, validators

from sghi.etl.core import Processor, Sink, Source
from sghi.utils import ensure_not_none, ensure_predicate

from .processors import NOOPProcessor, ScatterGatherProcessor
from .sinks import NullSink, ScatterSink
from .sources import GatherSource
from .workflow_definitions import SimpleWorkflowDefinition

if TYPE_CHECKING:
    from sghi.etl.core import WorkflowDefinition

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Processed Data Type."""

_RDT = TypeVar("_RDT")
"""Raw Data Type."""

_CompositeProcessorFactory = Callable[
    [Sequence[Processor[Any, Any]]],
    Processor[_RDT, _PDT],
]

_CompositeSourceFactory = Callable[[Sequence[Source[Any]]], Source[_RDT]]

_CompositeSinkFactory = Callable[[Sequence[Sink[Any]]], Sink[_PDT]]

_ProcessorFactory = Callable[[], Processor[_RDT, _PDT]]

_SinkFactory = Callable[[], Sink[_PDT]]

_SourceFactory = Callable[[], Source[_RDT]]


# =============================================================================
# WORKFLOW BUILDER
# =============================================================================


@define
class WorkflowBuilder(Generic[_RDT, _PDT]):
    """A DSL for defining :class:`workflow definitions<WorkflowDefinition>`."""

    id: str = field(
        validator=[validators.instance_of(str), validators.min_len(2)],
    )
    name: str = field(validator=validators.instance_of(str))
    description: str | None = field(
        default=None,
        kw_only=True,
        validator=validators.optional(validator=validators.instance_of(str)),
    )
    source_factories: Sequence[_SourceFactory[_RDT]] | None = field(
        default=None,
        kw_only=True,
        repr=False,
        validator=validators.optional(
            validators.deep_iterable(
                iterable_validator=validators.instance_of(Sequence),
                member_validator=validators.is_callable(),
            ),
        ),
    )
    processor_factories: Sequence[_ProcessorFactory[_RDT, _PDT]] | None = (
        field(
            default=None,
            kw_only=True,
            repr=False,
            validator=validators.optional(
                validators.deep_iterable(
                    iterable_validator=validators.instance_of(Sequence),
                    member_validator=validators.is_callable(),
                ),
            ),
        )
    )
    sink_factories: Sequence[_SinkFactory[_PDT]] | None = field(
        default=None,
        kw_only=True,
        repr=True,
        validator=validators.optional(
            validators.deep_iterable(
                iterable_validator=validators.instance_of(Sequence),
                member_validator=validators.is_callable(),
            ),
        ),
    )
    default_processor_factory: _ProcessorFactory[_RDT, _PDT] = field(
        default=NOOPProcessor,
        kw_only=True,
        repr=True,
        validator=validators.is_callable(),
    )
    default_sink_factory: _SinkFactory[_PDT] = field(
        default=NullSink,
        kw_only=True,
        repr=True,
        validator=validators.is_callable(),
    )
    composite_source_factory: _CompositeSourceFactory[_RDT] = field(
        default=GatherSource,
        kw_only=True,
        repr=True,
        validator=validators.is_callable(),
    )
    composite_processor_factory: _CompositeProcessorFactory[_RDT, _PDT] = (
        field(
            default=ScatterGatherProcessor,
            kw_only=True,
            repr=True,
            validator=validators.is_callable(),
        )
    )
    composite_sink_factory: _CompositeSinkFactory[_PDT] = field(
        default=ScatterSink,
        kw_only=True,
        repr=True,
        validator=validators.is_callable(),
    )
    _source_factories: list[_SourceFactory[_RDT]] = field(
        factory=list, init=False, repr=False
    )
    _processor_factories: list[_ProcessorFactory[_RDT, _PDT]] = field(
        factory=list,
        init=False,
        repr=False,
    )
    _sink_factories: list[_SinkFactory[_PDT]] = field(
        factory=list,
        init=False,
        repr=False,
    )

    def __attrs_post_init__(self) -> None:  # noqa: D105
        self._source_factories.extend(self.source_factories or ())
        self._processor_factories.extend(self._processor_factories or ())
        self._sink_factories.extend(self._sink_factories or ())

    def __call__(self) -> WorkflowDefinition[_RDT, _PDT]:
        """Create a :class:`WorkflowDefinition` instance.

        Delegates the actual call to :meth:`build`.

        :return: A new ``WorkflowDefinition`` instance.
        """
        return self.build()

    def build(self) -> WorkflowDefinition[_RDT, _PDT]:
        """Create a :class:`WorkflowDefinition` instance.

        :return: A new ``WorkflowDefinition`` instance.
        """
        return SimpleWorkflowDefinition(
            id=self.id,
            name=self.name,
            description=self.description,
            source_factory=self._build_source_factory(),
            processor_factory=self._build_processor_factory(),
            sink_factory=self._build_sink_factory(),
        )

    def draw_from(
        self,
        source: Source[_RDT] | _SourceFactory[_RDT],
    ) -> Source[_RDT] | _SourceFactory[_RDT]:
        """Add a new :class:`Source` or ``Source`` factory to draw from.

        :param source: A ``Source`` instance or factory function that returns
            a ``Source`` instance to draw from.

        :return:
        """
        ensure_not_none(source, "'source' MUST not be None.")

        match source:
            case Source():
                self._source_factories.append(lambda: source)
            case _ if callable(source):
                self._source_factories.append(source)
            case _:
                _err_msg: str = (
                    "'source' MUST be a 'sghi.etl.core.Source' instance or a "
                    "factory function that returns an instance of the same "
                    "type."
                )
                raise ValueError(_err_msg)

        return source

    def drain_to(
        self,
        sink: Sink[_PDT] | _SinkFactory[_PDT],
    ) -> Sink[_PDT] | _SinkFactory:
        """Add a new :class:`Sink` or ``Sink`` factory to drain to.

        :param sink: A ``Sink`` instance or factory function that returns a
            ``Sink`` instance to drain to.

        :return:
        """
        ensure_not_none(sink, "'sink' MUST not be None.")

        match sink:
            case Sink():
                self._sink_factories.append(lambda: sink)
            case _ if callable(sink):
                self._sink_factories.append(sink)
            case _:
                _err_msg: str = (
                    "'sink' MUST be a 'sghi.etl.core.Sink' instance or a "
                    "factory function that returns an instance of the same "
                    "type."
                )
                raise ValueError(_err_msg)

        return sink

    def apply_processor(
        self,
        processor: Processor[_RDT, _PDT] | _ProcessorFactory[_RDT, _PDT],
    ) -> Processor[_RDT, _PDT] | _ProcessorFactory[_RDT, _PDT]:
        """Add a new ``Processor`` or ``Processor`` factory to process using.

        :param processor: A ``Processor`` instance or factory function that
            returns a ``Processor`` instance to use when processing the
            extracted data.

        :return:
        """
        ensure_not_none(processor, "'processor' MUST not be None.")

        match processor:
            case Processor():
                self._processor_factories.append(lambda: processor)
            case _ if callable(processor):
                self._processor_factories.append(processor)
            case _:
                _err_msg: str = (
                    "'processor' MUST be a 'sghi.etl.core.Processor' instance "
                    "or a factory function that returns an instance of the "
                    "same type."
                )
                raise ValueError(_err_msg)

        return processor

    def _build_source_factory(self) -> _SourceFactory[_RDT]:
        ensure_predicate(
            bool(self._source_factories),
            message=(
                "No sources available. At least once source MUST be provided."
            ),
            exc_factory=RuntimeError,
        )

        match self._source_factories:
            case (_, _, *_):

                def _factory() -> Source[_RDT]:
                    return self.composite_source_factory(
                        [_sf() for _sf in self._source_factories]
                    )

                return _factory
            case _:
                return self._source_factories[0]

    def _build_processor_factory(self) -> _ProcessorFactory[_RDT, _PDT]:
        match self._processor_factories:
            case (_, _, *_):

                def _factory() -> Processor[_RDT, _PDT]:
                    return self.composite_processor_factory(
                        [_pf() for _pf in self._processor_factories]
                    )

                return _factory
            case (entry, *_):
                return entry
            case _:
                return self.default_processor_factory

    def _build_sink_factory(self) -> _SinkFactory[_PDT]:
        match self._sink_factories:
            case (_, _, *_):

                def _factory() -> Sink[_PDT]:
                    return self.composite_sink_factory(
                        [_sf() for _sf in self._sink_factories]
                    )

                return _factory
            case (entry, *_):
                return entry
            case _:
                return self.default_sink_factory
