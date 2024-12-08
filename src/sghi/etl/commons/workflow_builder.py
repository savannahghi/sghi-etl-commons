"""A DSL for building :class:`sghi.etl.core.WorkflowDefinition` instances."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Generic, Self, TypeVar

from typing_extensions import override

from sghi.etl.core import Processor, Sink, Source, WorkflowDefinition
from sghi.exceptions import SGHIError
from sghi.utils import (
    ensure_callable,
    ensure_instance_of,
    ensure_not_none_nor_empty,
    ensure_optional_instance_of,
)

from .processors import NOOPProcessor, ScatterGatherProcessor
from .sinks import NullSink, ScatterSink
from .sources import GatherSource
from .workflow_definitions import SimpleWorkflowDefinition

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Type variable representing the data type after processing."""

_RDT = TypeVar("_RDT")
"""Type variable representing the raw data type."""

_T = TypeVar("_T")

_T1 = TypeVar("_T1")

_T2 = TypeVar("_T2")

_CompositeProcessorFactory = Callable[
    [Sequence[Processor[Any, Any]]],
    Processor[Any, Any],
]

_CompositeSourceFactory = Callable[[Sequence[Source[Any]]], Source[Any]]

_CompositeSinkFactory = Callable[[Sequence[Sink[Any]]], Sink[Any]]

_ProcessorFactory = Callable[[], Processor[_T1, _T2]]

_SinkFactory = Callable[[], Sink[_T2]]

_SourceFactory = Callable[[], Source[_T1]]

# =============================================================================
# EXCEPTIONS
# =============================================================================


class NoSourceProvidedError(SGHIError):
    """A :class:`Source` wasn't provided to a :class:`WorkflowBuilder`.

    At least one ``Source`` instance or one factory function that suppliers
    ``Source`` instances ought to be provided to a ``WorkflowBuilder``
    instance. This error is raised by the
    :meth:`~sghi.etl.commons.workflow_builder.WorkflowBuilder.build`
    method of a ``WorkflowBuilder`` instance where this invariant is broken.
    """


class SoleValueAlreadyRetrievedError(SGHIError):
    """Raised when attempting to retrieve a value that should only be accessed
    once.

    This is raised by the factory methods that are created internally by the
    :class:`WorkflowBuilder` class to wrap instances of types :class:`Source`,
    :class:`Processor` and :class:`Sink`. These factory methods are only meant
    to be used once. Subsequent invocations of those factory methods will lead
    to this error being raised.
    """  # noqa: D205


# =============================================================================
# WORKFLOW BUILDER
# =============================================================================


class WorkflowBuilder(Generic[_RDT, _PDT]):
    """A builder class for constructing
    :class:`workflow definitions<sghi.etl.core.WorkflowDefinition>` in a
    structured manner.

    This class helps in assembling SGHI ETL Workflows by configuring
    :class:`sources<sghi.etl.core.Source>`,
    :class:`processors<sghi.etl.core.Processor>` and
    :class:`sinks<sghi.etl.core.Sink>`. This builder class offers a convenient
    way to construct workflows by providing methods to register sources,
    processors, and sinks, either individually or using factories.
    """  # noqa: D205

    __slots__ = (
        "_composite_processor_factory",
        "_composite_sink_factory",
        "_composite_source_factory",
        "_default_processor_factory",
        "_default_sink_factory",
        "_description",
        "_id",
        "_name",
        "_processor_factories",
        "_sink_factories",
        "_source_factories",
    )

    def __init__(  # noqa: PLR0913
        self,
        id: str,  # noqa: A002
        name: str,
        description: str | None = None,
        source_factories: Sequence[_SourceFactory[Any]] | None = None,
        processor_factories: Sequence[_ProcessorFactory[Any, Any]]
        | None = None,
        sink_factories: Sequence[_SinkFactory[Any]] | None = None,
        default_processor_factory: _ProcessorFactory[
            _RDT, _PDT
        ] = NOOPProcessor,
        default_sink_factory: _SinkFactory[_PDT] = NullSink,
        composite_source_factory: _CompositeSourceFactory = GatherSource,
        composite_processor_factory: _CompositeProcessorFactory = ScatterGatherProcessor,  # noqa: E501
        composite_sink_factory: _CompositeSinkFactory = ScatterSink,
    ) -> None:
        r"""Create a ``WorkflowBuilder`` of the following properties.

        :param id: A unique identifier to assign to the assembled workflow(s).
            This MUST be a non-empty string.
        :param name: A name to assign to the assembled workflow(s). This MUST
            be a non-empty string.
        :param description: An optional textual description of the assembled
            workflow(s). This MUST be a string when NOT ``None``. Defaults to
            ``None`` when not provided.
        :param source_factories: An optional ``Sequence`` of ``Source``
            factories that will be used to create the ``Source``\ s of the
            assembled workflow(s). This MUST be a ``collections.abc.Sequence``
            instance when not ``None``. Defaults to ``None`` when not provided.
        :param processor_factories: An optional ``Sequence`` of ``Processor``
            factories that will be used to create the ``Processor``\ s of the
            assembled workflow(s). This MUST be a ``collections.abc.Sequence``
            when not ``None``. Defaults to ``None`` when not provided.
        :param sink_factories: An optional ``Sequence`` of ``Sink`` factories
            that will be used to create ``Sink``\ s of the assembled
            workflow(s). This MUST be a ``collections.abc.Sequence`` when not
            ``None``. Defaults to ``None`` when not provided.
        :param default_processor_factory: An optional factory function that
            will be used to create a ``Processor`` for the assembled
            workflow(s) when no explicit ``Processor`` is specified. This MUST
            be a valid callable object. Defaults to the ``NOOPProcessor``
            class, which does nothing.
        :param default_sink_factory: An optional factory function that will be
            used to create a ``Sink`` for the assembled workflow(s) when no
            explicit ``Sink`` is specified. This MUST be a valid callable
            object. Defaults to the ``NullSink`` class, whose instances discard
            all data drained to them.
        :param composite_source_factory: An optional factory function that will
            be used to combine multiple ``Source``\ s into a single ``Source``.
            This factory function is only used when more than one ``Source`` is
            provided. This MUST be a valid callable object that accepts a
            ``Sequence`` of ``Source`` instances. Defaults to the
            ``GatherSource`` class, which yields data from all its embedded
            ``Source``\ s concurrently.
        :param composite_processor_factory: An optional factory function that
            will be used to combine multiple ``Processor``\ s into a single
            ``Processor``. This factory function is only used when more than
            one ``Processor`` is provided. This MUST be a valid callable object
            that accepts a ``Sequence`` of ``Processor`` instances. Defaults to
            the ``ScatterGatherProcessor`` class which distributes processing
            to all its embedded ``Processor``\ s concurrently before gathering
            and returning their results.
        :param composite_sink_factory: An optional factory function that will
            be used to combine multiple ``Sink``\ s into a single ``Sink``.
            This factory function is only used when more than one ``Sink`` is
            provided. This MUST be a valid callable object that accepts a
            ``Sequence`` of ``Sink`` instances. Defaults to the ``ScatterSink``
            class, which drains data to all its embedded ``Sink``\ s
            concurrently.

        :raise TypeError: If ``id`` or ``name`` are NOT of type string. If
            ``description`` is NOT a string when NOT ``None``. If
            ``source_factories``, ``processor_factories`` or ``sink_factories``
            are NOT of type ``collections.abc.Sequence`` when NOT ``None``.
        :raise ValueError: If ``id`` or ``name`` are empty strings. If
            ``default_processor_factory``, ``default_sink_factory``,
            ``composite_source_factory``, ``composite_processor_factory`` or
            ``composite_sink_factory`` are NOT valid callable objects.
        """
        super().__init__()
        self._id: str = ensure_not_none_nor_empty(
            value=ensure_instance_of(
                value=id,
                klass=str,
                message="'id' MUST be a string.",
            ),
            message="'id' MUST NOT be an empty string.",
        )
        self._name: str = ensure_not_none_nor_empty(
            value=ensure_instance_of(
                value=name,
                klass=str,
                message="'name' MUST be a string.",
            ),
            message="'name' MUST NOT be an empty string.",
        )
        self._description: str | None = ensure_optional_instance_of(
            value=description,
            klass=str,
            message="'description' MUST be a string when NOT None.",
        )
        ensure_optional_instance_of(
            value=source_factories,
            klass=Sequence,
            message=(
                "'source_factories' MUST be a collections.abc.Sequence when "
                "not None."
            ),
        )
        self._source_factories: list[_SourceFactory[_RDT]] = []
        self._source_factories.extend(source_factories or ())
        ensure_optional_instance_of(
            value=processor_factories,
            klass=Sequence,
            message=(
                "'processor_factories' MUST be a collections.abc.Sequence "
                "when not None."
            ),
        )
        self._processor_factories: list[_ProcessorFactory[_RDT, _PDT]] = []
        self._processor_factories.extend(processor_factories or ())
        ensure_optional_instance_of(
            value=sink_factories,
            klass=Sequence,
            message=(
                "'sink_factories' MUST be a collections.abc.Sequence when "
                "not None."
            ),
        )
        self._sink_factories: list[_SinkFactory[_PDT]] = []
        self._sink_factories.extend(sink_factories or ())
        self._default_processor_factory: _ProcessorFactory[_RDT, _PDT]
        self._default_processor_factory = ensure_callable(
            value=default_processor_factory,
            message="'default_processor_factory' MUST be a callable object.",
        )
        self._default_sink_factory: _SinkFactory[_PDT] = ensure_callable(
            value=default_sink_factory,
            message="'default_sink_factory' MUST be a callable object.",
        )
        self._composite_source_factory: _CompositeSourceFactory
        self._composite_source_factory = ensure_callable(
            value=composite_source_factory,
            message="'composite_source_factory' MUST be a callable object.",
        )
        self._composite_processor_factory: _CompositeProcessorFactory
        self._composite_processor_factory = ensure_callable(
            value=composite_processor_factory,
            message="'composite_processor_factory' MUST be a callable object.",
        )
        self._composite_sink_factory: _CompositeSinkFactory
        self._composite_sink_factory = ensure_callable(
            value=composite_sink_factory,
            message="'composite_sink_factory' MUST be a callable object.",
        )

    def __call__(self) -> WorkflowDefinition[_RDT, _PDT]:
        """Build and return a :class:`~sghi.etl.core.WorkflowDefinition`.

        This method finalizes the workflow construction and returns a
        ``WorkflowDefinition`` instance that encapsulates the workflow's
        configuration.

        This method delegates the actual call to :meth:`build`.

        :return: The build ``WorkflowDefinition`` instance.

        :raise NoSourceProvidedError: If neither a ``Source`` nor a factory
            function that supplies ``Source`` instances has been provided.
            At least one of these is required.

        .. seealso:: :meth:`build`.
        """
        return self.build()

    @override
    def __repr__(self) -> str:
        """Return a sting representation of this ``WorkflowBuilder``.

        This method provides a human-readable representation of this
        ``WorkflowBuilder`` which is composed of its ID, name, and description.

        :return: A string representation of this ``WorkflowBuilder``.
        """
        return self.__str__()

    @override
    def __str__(self) -> str:
        """Return a sting representation of this ``WorkflowBuilder``.

        This method provides a human-readable representation of this
        ``WorkflowBuilder`` which is composed of its ID, name, and description.

        :return: A string representation of this ``WorkflowBuilder``.
        """
        return (
            f"WorkflowBuilder(id={self._id}, name={self._name}, "
            f"description={self._description})"
        )

    # PROPERTIES
    # -------------------------------------------------------------------------
    @property
    def composite_processor_factory(self) -> _CompositeProcessorFactory:
        r"""Get the factory function used to combine multiple ``Processor``\ s.

        This property retrieves the factory function currently configured for
        combining ``Processor``\ s when creating new ``WorkflowDefinition``\ s.

        .. note::

            This is only used if more than one ``Processor`` is provided.
        """
        return self._composite_processor_factory

    @composite_processor_factory.setter
    def composite_processor_factory(
        self,
        __composite_processor_factory: _CompositeProcessorFactory,
        /,
    ) -> None:
        r"""Set the factory function used to combine multiple ``Processor``\ s.

        :param __composite_processor_factory: A factory function that accepts a
            ``Sequence`` of ``Processor`` instances and returns a single
            ``Processor`` that combines all the given ``Processor``\ s. This
            MUST be a valid callable object.

        :raise ValueError: If the provided value *IS NOT* a valid callable
            object.
        """
        self._composite_processor_factory = ensure_callable(
            value=__composite_processor_factory,
            message="'composite_processor_factory' MUST be a callable object.",
        )

    @property
    def composite_sink_factory(self) -> _CompositeSinkFactory:
        r"""Get the factory function used to combine multiple ``Sink``\ s.

        This property retrieves the factory function currently configured for
        combining ``Sink``\ s when creating new ``WorkflowDefinition``\ s.

        .. note::

            This is only used if more than one ``Sink`` is provided.
        """
        return self._composite_sink_factory

    @composite_sink_factory.setter
    def composite_sink_factory(
        self,
        __composite_sink_factory: _CompositeSinkFactory,
        /,
    ) -> None:
        r"""Set the factory function used to combine multiple ``Sink``\ s.

        :param __composite_sink_factory: A factory function that accepts a
            ``Sequence`` of ``Sink`` instances and returns a single ``Sink``
            that combines all the given ``Sink``\ s. This MUST be a valid
            callable object.

        :raise ValueError: If the provided value *IS NOT* a valid callable
            object.
        """
        self._composite_sink_factory = ensure_callable(
            value=__composite_sink_factory,
            message="'composite_sink_factory' MUST be a callable object.",
        )

    @property
    def composite_source_factory(self) -> _CompositeSourceFactory:
        r"""Get the factory function used to combine multiple ``Source``\ s.

        This property retrieves the factory function currently configured for
        combining ``Source``\ s when creating new ``WorkflowDefinition``\ s.

        .. note::

            This is only used if more than one ``Source`` is provided.
        """
        return self._composite_source_factory

    @composite_source_factory.setter
    def composite_source_factory(
        self,
        __composite_source_factory: _CompositeSourceFactory,
        /,
    ) -> None:
        r"""Set the factory function used to combine multiple ``Source``\ s.

        :param __composite_source_factory: A factory function that accepts a
            ``Sequence`` of ``Source`` instances and returns a single
            ``Source`` that combines all the given ``Source``\ s. This MUST be
            a valid callable object.

        :raise ValueError: If the provided value *IS NOT* a valid callable
            object.
        """
        self._composite_source_factory = ensure_callable(
            value=__composite_source_factory,
            message="'composite_source_factory' MUST be a callable object.",
        )

    @property
    def default_processor_factory(self) -> _ProcessorFactory[_RDT, _PDT]:
        r"""Get the factory function used to create default ``Processor``\ s.

        This property retrieves the factory function currently configured to
        create ``Processor``\ s for the assembled ``WorkflowDefinition``\ (s)
        when no explicit ``Processor`` is provided to the builder.

        The returned factory function is invoked each time :meth:`build` is
        invoked and no ``Processor`` is provided to the builder.
        """
        return self._default_processor_factory

    @default_processor_factory.setter
    def default_processor_factory(
        self,
        __default_processor_factory: _ProcessorFactory[_RDT, _PDT],
        /,
    ) -> None:
        r"""Set the factory function used to create default ``Processor``\ s.

        :param __default_processor_factory: A factory function that supplies
            ``Processor`` instances to be used as default ``Processor``\ (s)
            for the assembled ``WorkflowDefinition``\ (s) when no explicit
            ``Processor`` is provided to the builder. This MUST be a valid
            callable object.

        :raise ValueError: If the provided value *IS NOT* a valid callable
            object.
        """
        self._default_processor_factory = ensure_callable(
            value=__default_processor_factory,
            message="'default_processor_factory' MUST be a callable object.",
        )

    @property
    def default_sink_factory(self) -> _SinkFactory[_PDT]:
        r"""Get the factory function used to create default ``Sink``\ s.

        This property retrieves the factory function currently configured to
        create ``Sink``\ s for the assembled ``WorkflowDefinition``\ (s) when
        no explicit ``Sink`` is provided to the builder.

        The returned factory function is invoked each time :meth:`build` is
        invoked and no ``Sink`` is provided to the builder.
        """
        return self._default_sink_factory

    @default_sink_factory.setter
    def default_sink_factory(
        self,
        __default_sink_factory: _SinkFactory[_PDT],
        /,
    ) -> None:
        r"""Set the factory function used to create default ``Sink``\ s.

        :param __default_sink_factory: A factory function that supplies
            ``Sink`` instances to be used as the default ``Sink``\ (s) for
            the assembled ``WorkflowDefinition``\ (s) when no explicit
            ``Sink`` is provided to the builder. This MUST be a valid
            callable object.

        :raise ValueError: If the provided value *IS NOT* a valid callable
            object.
        """
        self._default_sink_factory: _SinkFactory[_PDT] = ensure_callable(
            value=__default_sink_factory,
            message="'default_sink_factory' MUST be a callable object.",
        )

    @property
    def description(self) -> str | None:
        """An optional textual description of the assembled workflow(s)."""
        return self._description

    @description.setter
    def description(self, __description: str | None, /) -> None:
        r"""Set the description of the assembled ``WorkflowDefinition``\ (s).

        :param __description: An optional textual description of the assembled
            ``WorkflowDefinition``\ (s). This MUST be a string when NOT
            ``None``.

        :raise TypeError: If the given value is NEITHER a string nor ``None``.
        """
        self._description: str | None = ensure_optional_instance_of(
            value=__description,
            klass=str,
            message="'description' MUST be a string when NOT None.",
        )

    @property
    def id(self) -> str:
        """The identifier to assign to the assembled workflow(s)."""
        return self._id

    @id.setter
    def id(self, __id: str, /) -> None:
        r"""Set an identifier for the assembled ``WorkflowDefinition``\ (s).

        :param __id: A unique identifier to assign to the assembled
            ``WorkflowDefinition``\ (s). This MUST be a non-empty string.

        :raise ValueError: If the provided value is an empty string.
        :raise TypeError: If the provided value is NOT of type string.
        """
        self._id = ensure_not_none_nor_empty(
            value=ensure_instance_of(
                value=__id,
                klass=str,
                message="'id' MUST be a string.",
            ),
            message="'id' MUST NOT be an empty string.",
        )

    @property
    def name(self) -> str:
        """The name to assign to the assembled workflow(s)."""
        return self._name

    @name.setter
    def name(self, __name: str, /) -> None:
        r"""Set the name of the assembled ``WorkflowDefinition``\ (s).

        :param __name: A name to assign to the created
            ``WorkflowDefinition``\ (s). This MUST be a non-empty string.

        :raise ValueError: If the provided value is an empty string.
        :raise TypeError: If the provided value is NOT of type string.
        """
        self._name: str = ensure_not_none_nor_empty(
            value=ensure_instance_of(
                value=__name,
                klass=str,
                message="'name' MUST be a string.",
            ),
            message="'name' MUST NOT be an empty string.",
        )

    @property
    def processor_factories(self) -> Sequence[_ProcessorFactory[Any, Any]]:
        r"""Get the ``Processor`` factories registered to create
        ``Processor``\ s for the assembled ``WorkflowDefinition``\ (s).

        This property retrieves a ``Sequence`` of factory functions currently
        configured to create ``Processor``\ s for the assembled
        ``WorkflowDefinition``\ (s). These are the factory functions explicitly
        provided by clients of this class to this builder.

        .. tip::

            If the ``Sequence`` returned by this property is empty, then when
            :meth:`build` is invoked, the factory function returned by the
            :attr:`default_processor_factory` property will be used to create
            a default ``Processor`` for the assembled ``WorkflowDefinition``.
        """  # noqa: D205
        return tuple(self._processor_factories)

    @processor_factories.setter
    def processor_factories(
        self,
        __processor_factories: Sequence[_ProcessorFactory[Any, Any]],
        /,
    ) -> None:
        r"""Set the ``Processor`` factories to use when creating
        ``Processor``\ s for the assembled ``WorkflowDefinition``\ (s).

        :param __processor_factories: A ``Sequence`` of factory functions that
            supply ``Processor`` instances for the assembled
            ``WorkflowDefinition``\ (s). This MUST be an instance of
            ``collections.abc.Sequence``. An empty ``Sequence`` is a valid
            value.

        :raise TypeError: If the provided value is NOT an instance of
            ``collections.abc.Sequence``.
        """  # noqa: D205
        ensure_instance_of(
            value=__processor_factories,
            klass=Sequence,
            message="'processor_factories' MUST be a collections.abc.Sequence.",  # noqa: E501
        )
        self._processor_factories = list(__processor_factories)

    @property
    def sink_factories(self) -> Sequence[_SinkFactory[Any]]:
        r"""Get the ``Sink`` factories registered to create ``Sink``\ s for the
        assembled ``WorkflowDefinition``\ (s).

        This property retrieves a ``Sequence`` of factory functions currently
        configured to create ``Sink``\ s for the assembled
        ``WorkflowDefinition``\ (s). These are the factory functions explicitly
        provided by clients of this class to this builder.

        .. tip::

            If the ``Sequence`` returned by this property is empty, then when
            :meth:`build` is invoked, the factory function returned by the
            :attr:`default_sink_factory` property will be used to create a
            default ``Sink`` for the assembled ``WorkflowDefinition``.
        """  # noqa: D205
        return tuple(self._sink_factories)

    @sink_factories.setter
    def sink_factories(
        self,
        __sink_factories: Sequence[_SinkFactory[Any]],
        /,
    ) -> None:
        r"""Set the ``Sink`` factories to use when creating ``Sink``\ s for the
        assembled ``WorkflowDefinition``\ (s).

        :param __sink_factories: A ``Sequence`` of factory functions that
            supply ``Sink`` instances for the assembled
            ``WorkflowDefinition``\ (s). This MUST be an instance of
            ``collections.abc.Sequence``. An empty ``Sequence`` is a valid
            value.

        :raise TypeError: If the provided value is NOT an instance of
            ``collections.abc.Sequence``.
        """  # noqa: D205
        ensure_instance_of(
            value=__sink_factories,
            klass=Sequence,
            message="'sink_factories' MUST be a collections.abc.Sequence.",
        )
        self._sink_factories = list(__sink_factories)

    @property
    def source_factories(self) -> Sequence[_SourceFactory[Any]]:
        r"""Get the ``Source`` factories registered to create ``Source``\ s for
        the assembled ``WorkflowDefinition``\ (s).

        This property retrieves a ``Sequence`` of factory functions currently
        configured to create ``Source``\ s for the assembled
        ``WorkflowDefinition``\ (s). These are the factory functions explicitly
        provided by clients of this class to this builder.

        .. caution::

            If the ``Sequence`` returned by this property is empty, then when
            :meth:`build` is invoked, an exception will be raised. At least one
            ``Source`` MUST be explicitly provided to a builder before a
            ``WorkflowDefinition`` can be built.
        """  # noqa: D205
        return tuple(self._source_factories)

    @source_factories.setter
    def source_factories(
        self,
        __source_factories: Sequence[_SourceFactory[Any]],
        /,
    ) -> None:
        r"""Set the ``Source`` factories to use when creating ``Source``\ s
        for the assembled ``WorkflowDefinition``\ (s).

        :param __source_factories: A ``Sequence`` of factory functions that
            supply ``Source`` instances for the assembled
            ``WorkflowDefinition``\ (s). This MUST be an instance of
            ``collections.abc.Sequence``. An empty ``Sequence`` is a valid
            value.

        :raise TypeError: If the provided value is NOT an instance of
            ``collections.abc.Sequence``.
        """  # noqa: D205
        ensure_instance_of(
            value=__source_factories,
            klass=Sequence,
            message="'source_factories' MUST be a collections.abc.Sequence.",
        )
        self._source_factories = list(__source_factories)

    # BUILD
    # -------------------------------------------------------------------------
    def build(self) -> WorkflowDefinition[_RDT, _PDT]:
        """Build and return a :class:`~sghi.etl.core.WorkflowDefinition`.

        This method finalizes the workflow construction and returns a
        ``WorkflowDefinition`` instance that encapsulates the workflow's
        configuration.

        :return: The build ``WorkflowDefinition`` instance.

        :raise NoSourceProvidedError: If neither a ``Source`` nor a factory
            function that supplies ``Source`` instances has been provided.
            At least one of these is required.
        """
        return SimpleWorkflowDefinition(
            id=self.id,
            name=self._name,
            description=self._description,
            source_factory=self._build_source_factory(),
            processor_factory=self._build_processor_factory(),
            sink_factory=self._build_sink_factory(),
        )

    # DECORATORS
    # -------------------------------------------------------------------------
    def applies_processor(
        self,
        processor: Processor[Any, Any],
    ) -> Processor[Any, Any]:
        r"""Register a ``Processor`` to be used in the assembled
        ``WorkflowDefinition``.

        This method accepts a ``Processor`` instance and is meant to be
        used as a decorator. It delegates the actual call to the
        :meth:`~sghi.etl.commons.workflow_builder.WorkflowBuilder.apply_processor`
        method and as such, the same constraints hold.

        .. important::

            The given ``Processor`` instance is internally converted into a
            factory function that only supplies the ``Processor`` instance
            once. Attempting to get a value from the factory function again
            (e.g. by invoking
            :meth:`~sghi.etl.commons.workflow_builder.WorkflowBuilder.build`
            again) will result in a :exc:`SoleValueAlreadyRetrievedError` being
            raised. As a result, the builder SHOULD NOT be used to construct
            multiple ``WorkflowDefinition``\ s. Doing so will result in failure
            when the :meth:`build` method is invoked multiple times on the same
            builder. Clients of this class should be aware of this limitation.

        :param processor: A ``Processor`` instance to be included in the
            assembled ``WorkflowDefinition``. This MUST BE a ``Processor``
            instance.

        :return: The given ``Processor`` instance.

        :raise TypeError: If ``processor`` is NOT a ``Processor`` instance.

        .. seealso:: :meth:`apply_processor`.
        """  # noqa: D205
        ensure_instance_of(
            value=processor,
            klass=Processor,
            message=(
                "'processor' MUST be an 'sghi.etl.core.Processor' instance."
            ),
        )
        self.apply_processor(processor)
        return processor

    def drains_to(self, sink: Sink[Any]) -> Sink[Any]:
        r"""Register a ``Sink`` to be used in the assembled
        ``WorkflowDefinition``.

        This method accepts a ``Sink`` instance and is meant to be used as a
        decorator. It delegates the actual call to the
        :meth:`~sghi.etl.commons.workflow_builder.WorkflowBuilder.drain_to`
        method and as such, the same constraints hold.

        .. important::

            The given ``Sink`` instance is internally converted into a factory
            function that only supplies the ``Sink`` instance once. Attempting
            to get a value from the factory function again (e.g. by invoking
            :meth:`~sghi.etl.commons.workflow_builder.WorkflowBuilder.build`
            again) will result in a :exc:`SoleValueAlreadyRetrievedError` being
            raised. As a result, the builder SHOULD NOT be used to construct
            multiple ``WorkflowDefinition``\ s. Doing so will result in failure
            when the :meth:`build` method is invoked multiple times on the same
            builder. Clients of this class should be aware of this limitation.

        :param sink: A ``Sink`` instance to be included in the assembled
            ``WorkflowDefinition``. This MUST BE a ``Sink`` instance.

        :return: The given ``Sink`` instance.

        :raise TypeError: If ``sink`` is NOT a ``Sink`` instance.

        .. seealso:: :meth:`drain_to`.
        """  # noqa: D205
        ensure_instance_of(
            value=sink,
            klass=Sink,
            message="'sink' MUST be an 'sghi.etl.core.Sink' instance.",
        )
        self.drain_to(sink)
        return sink

    def draws_from(self, source: Source[Any]) -> Source[Any]:
        r"""Register a ``Source`` to be used in the assembled
        ``WorkflowDefinition``.

        This method accepts a ``Source`` instance and is meant to be used as a
        decorator. It delegates the actual call to the
        :meth:`~sghi.etl.commons.workflow_builder.WorkflowBuilder.draw_from`
        method and as such, the same constraints hold.

        .. important::

            The given ``Source`` instance is internally converted into a
            factory function that only supplies the ``Source`` instance
            once. Attempting to get a value from the factory function again
            (e.g. by invoking
            :meth:`~sghi.etl.commons.workflow_builder.WorkflowBuilder.build`
            again) will result in a :exc:`SoleValueAlreadyRetrievedError` being
            raised. As a result, the builder SHOULD NOT be used to construct
            multiple ``WorkflowDefinition``\ s. Doing so will result in failure
            when the :meth:`build` method is invoked multiple times on the same
            builder. Clients of this class should be aware of this limitation.

        :param source: A ``Source`` instance to be included in the assembled
            ``WorkflowDefinition``. This MUST BE a ``Source`` instance.

        :return: The given ``Source`` instance.

        :raise TypeError: If ``processor`` is NOT a ``Source`` instance.

        .. seealso:: :meth:`draw_from`.
        """  # noqa: D205
        ensure_instance_of(
            value=source,
            klass=Source,
            message="'source' MUST be an 'sghi.etl.core.Source' instance.",
        )
        self.draw_from(source)
        return source

    # FLOW API
    # -------------------------------------------------------------------------
    def apply_processor(
        self,
        processor: Processor[Any, Any] | _ProcessorFactory[Any, Any],
    ) -> Self:
        r"""Register a ``Processor`` to be used in the assembled
        ``WorkflowDefinition``\ (s).

        This method accepts a ``Processor`` instance or factory
        function that supplies ``Processor`` instances to be included
        in the assembled ``WorkflowDefinition``\ (s).

        .. important::

            When a ``Processor`` instance is provided instead of a factory
            function, it is internally converted into a factory function that
            only supplies the ``Processor`` instance once. Attempting to get a
            value from the factory function again will result in a
            :exc:`SoleValueAlreadyRetrievedError` being raised. As a result,
            the builder SHOULD NOT be used to construct multiple
            ``WorkflowDefinition``\ s. Doing so will result in failure when the
            :meth:`build` method is invoked multiple times on the same builder.
            Clients of this class should be aware of this limitation.

        :param processor: A ``Processor`` instance or factory function that
            suppliers ``Processor`` instances to be included in the assembled
            ``WorkflowDefinition``\ (s). This MUST EITHER be a ``Processor``
            instance or a valid callable object that suppliers ``Processor``
            instances.

        :return: This builder, i.e. ``self``.

        :raise ValueError: If ``processor`` is NEITHER a ``Processor``
            instance NOR a callable object.
        """  # noqa: D205
        match processor:
            case Processor():
                self._processor_factories.append(
                    self._create_factory(processor)
                )
            case _ if callable(processor):
                # noinspection PyTypeChecker
                self._processor_factories.append(processor)
            case _:
                _err_msg: str = (
                    "'processor' MUST be an 'sghi.etl.core.Processor' "
                    "instance or a factory function that returns an instance "
                    "of the same type."
                )
                raise ValueError(_err_msg)

        return self

    def clear_processor_factories(self) -> Self:
        r"""Remove all the registered ``Processor``\ s from the builder.

        After this method returns, the :attr:`processor_factories` property
        returns an empty ``Sequence``.
        """
        self._processor_factories.clear()
        return self

    def clear_sink_factories(self) -> Self:
        r"""Remove all the registered ``Sink``\ s from the builder.

        After this method returns, the :attr:`sink_factories` property returns
        an empty ``Sequence``.
        """
        self._sink_factories.clear()
        return self

    def clear_source_factories(self) -> Self:
        r"""Remove all the registered ``Source``\ s from the builder.

        After this method returns, the :attr:`source_factories` property
        returns an empty ``Sequence``.
        """
        self._source_factories.clear()
        return self

    def drain_to(self, sink: Sink[Any] | _SinkFactory[Any]) -> Self:
        r"""Register a ``Sink`` to be used in the assembled
        ``WorkflowDefinition``\ (s).

        This method accepts a ``Sink`` instance or factory function that
        supplies ``Sink`` instances to be included in the assembled
        ``WorkflowDefinition``\ (s).

        .. important::

            When a ``Sink`` instance is provided instead of a factory function,
            it is internally converted into a factory function that only
            supplies the ``Sink`` instance once. Attempting to get a
            value from the factory function again will result in a
            :exc:`SoleValueAlreadyRetrievedError` being raised. As a result,
            the builder SHOULD NOT be used to construct multiple
            ``WorkflowDefinition``\ s. Doing so will result in failure when the
            :meth:`build` method is invoked multiple times on the same builder.
            Clients of this class should be aware of this limitation.

        :param sink: A ``Sink`` instance or factory function that suppliers
            ``Sink`` instances to be included in the assembled
            ``WorkflowDefinition``\ (s). This MUST EITHER be a ``Sink``
            instance or a valid callable object that suppliers ``Sink``
            instances.

        :return: This builder, i.e. ``self``.

        :raise ValueError: If ``sink`` is NEITHER a ``Sink`` instance NOR a
            callable object.
        """  # noqa: D205
        match sink:
            case Sink():
                self._sink_factories.append(self._create_factory(sink))
            case _ if callable(sink):
                # noinspection PyTypeChecker
                self._sink_factories.append(sink)
            case _:
                _err_msg: str = (
                    "'sink' MUST be an 'sghi.etl.core.Sink' instance or a "
                    "factory function that returns an instance of the same "
                    "type."
                )
                raise ValueError(_err_msg)

        return self

    def draw_from(self, source: Source[Any] | _SourceFactory[Any]) -> Self:
        r"""Register a ``Source`` to be used in the assembled
        ``WorkflowDefinition``\ (s).

        This method accepts a ``Source`` instance or factory function that
        supplies ``Source`` instances to be included in the assembled
        ``WorkflowDefinition``\ (s).

        .. important::

            When a ``Source`` instance is provided instead of a factory
            function, it is internally converted into a factory function that
            only supplies the ``Source`` instance once. Attempting to get a
            value from the factory function again will result in a
            :exc:`SoleValueAlreadyRetrievedError` being raised. As a result,
            the builder SHOULD NOT be used to construct multiple
            ``WorkflowDefinition``\ s. Doing so will result in failure when the
            :meth:`build` method is invoked multiple times on the same builder.
            Clients of this class should be aware of this limitation.

        :param source: A ``Source`` instance or factory function that suppliers
            ``Source`` instances to be included in the assembled
            ``WorkflowDefinition``\ (s). This MUST EITHER be a ``Source``
            instance or a valid callable object that suppliers ``Source``
            instances.

        :return: This builder, i.e. ``self``.

        :raise ValueError: If ``source`` is NEITHER a ``Source`` instance NOR a
            callable object.
        """  # noqa: D205
        match source:
            case Source():
                self._source_factories.append(self._create_factory(source))
            case _ if callable(source):
                # noinspection PyTypeChecker
                self._source_factories.append(source)
            case _:
                _err_msg: str = (
                    "'source' MUST be an 'sghi.etl.core.Source' instance or a "
                    "factory function that returns an instance of the same "
                    "type."
                )
                raise ValueError(_err_msg)

        return self

    # HELPERS
    # -------------------------------------------------------------------------
    def _build_processor_factory(self) -> _ProcessorFactory[_RDT, _PDT]:
        match self._processor_factories:
            case (_, _, *_):

                def _factory() -> Processor[_RDT, _PDT]:  # pragma: no cover
                    return self._composite_processor_factory(
                        [_pf() for _pf in self._processor_factories]
                    )

                return _factory
            case (entry, *_):
                return entry
            case _:
                return self._default_processor_factory

    def _build_sink_factory(self) -> _SinkFactory[_PDT]:
        match self._sink_factories:
            case (_, _, *_):

                def _factory() -> Sink[_PDT]:  # pragma: no cover
                    return self._composite_sink_factory(
                        [_sf() for _sf in self._sink_factories]
                    )

                return _factory
            case (entry, *_):
                return entry
            case _:
                return self._default_sink_factory

    def _build_source_factory(self) -> _SourceFactory[_RDT]:
        match self._source_factories:
            case (_, _, *_):

                def _factory() -> Source[_RDT]:  # pragma: no cover
                    return self._composite_source_factory(
                        [_sf() for _sf in self._source_factories]
                    )

                return _factory
            case (entry, *_):
                return entry
            case _:
                _err_msg: str = (
                    "No sources available. At least once 'Source' or one "
                    "factory function that supplies 'Source' instances MUST "
                    "be provided."
                )
                raise NoSourceProvidedError(_err_msg)

    @staticmethod
    def _create_factory(val: _T) -> Callable[[], _T]:
        has_been_retrieved: bool = False

        def _return_once_then_raise_factory() -> _T:
            nonlocal has_been_retrieved
            if has_been_retrieved:
                _err_msg: str = (
                    "This factory's sole value has already been retrieved."
                )
                raise SoleValueAlreadyRetrievedError(_err_msg)

            has_been_retrieved = True
            return val

        return _return_once_then_raise_factory


# =============================================================================
# MODULE EXPORTS
# =============================================================================


__all__ = [
    "NoSourceProvidedError",
    "SoleValueAlreadyRetrievedError",
    "WorkflowBuilder",
]
