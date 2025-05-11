# from types import coroutine # Not used
from typing import Awaitable, Callable, Any, List, Type


class PipelineException(Exception):
    """
    Custom exception for pipeline-related errors.
    """
    pass


class Processor:
    """
    Represents a data processing stage.
    It wraps a callable processor function and includes type checking.
    """

    def __init__(self, processor_callable: Callable[[Any], Any], input_type: Type = Any, output_type: Type = Any):
        """
        Initializes a Processor.

        Args:
            processor_callable: The callable function or method that performs the processing.
            input_type: The expected input type for the processor. Defaults to Any.
            output_type: The expected output type from the processor. Defaults to Any.

        Raises:
            ValueError: If processor_callable is not callable.
        """
        if not callable(processor_callable):
            raise ValueError("Processor must be a callable object.")

        self._input_type = input_type
        self._output_type = output_type
        self._callable = processor_callable

        # Attempt to get a meaningful name for the processor for debugging
        if hasattr(processor_callable, '__name__'):
            self._processor_name = processor_callable.__name__
        elif hasattr(processor_callable, '__class__') and hasattr(processor_callable.__class__, '__name__'):
            # For callable objects (instances of classes with __call__)
            self._processor_name = processor_callable.__class__.__name__
        else:
            self._processor_name = str(processor_callable)

    @property
    def name(self) -> str:
        """The name of the wrapped processor callable."""
        return self._processor_name

    @property
    def input_type(self) -> Type:
        """The expected input type."""
        return self._input_type

    @property
    def output_type(self) -> Type:
        """The expected output type."""
        return self._output_type

    async def __call__(self, value: Any) -> Any:
        """
        Executes the wrapped processor callable on the given value,
        performing type checks if specified.

        Args:
            value: The input value to process.

        Returns:
            The processed value.

        Raises:
            ValueError: If input or output types do not match expectations (and are not Any).
        """
        # Check input type if it's not Any
        if self._input_type is not Any and not isinstance(value, self._input_type):
            raise ValueError(
                f"Input value for processor '{self.name}' must be of type "
                f"{self._input_type.__name__}, got {type(value).__name__}"
            )

        # Execute the actual processor callable
        output = self._callable(value)

        if isinstance(output, Awaitable):
            output = await output

        # Check output type if it's not Any
        if self._output_type is not Any and not isinstance(output, self._output_type):
            raise ValueError(
                f"Output value from processor '{self.name}' must be of type "
                f"{self._output_type.__name__}, got {type(output).__name__}"
            )
        return output

    def __repr__(self) -> str:
        """
        Provides a developer-friendly string representation of the Processor.
        """
        return (f"<Processor name='{self.name}' "
                f"input_type={self._input_type.__name__} "
                f"output_type={self._output_type.__name__}>")


class Pipeline:
    """
    Represents a data processing pipeline.
    Processes an item by passing it sequentially through a list of Processor instances.
    """

    def __init__(self, processors: List[Processor] = None):
        """
        Initializes the pipeline.

        Args:
            processors: An optional list of Processor instances to initialize the pipeline with.
        """
        self._processors: List[Processor] = []
        if processors:
            for p in processors:
                if not isinstance(p, Processor):
                    raise ValueError("All items in the initial processors list must be Processor instances.")
                self._processors.append(p)

    def add_processor(self, processor_callable: Callable[[Any], Any], input_type: Type = Any,
                      output_type: Type = Any) -> None:
        """
        Adds a new processing stage to the pipeline.

        Args:
            processor_callable: The callable function or method to execute for this stage.
            input_type: The expected input type for this processor. Defaults to Any.
            output_type: The expected output type from this processor. Defaults to Any.
        """
        # The Processor class itself will validate if processor_callable is callable
        new_processor = Processor(processor_callable=processor_callable, input_type=input_type, output_type=output_type)
        self._processors.append(new_processor)

    async def process(self, item: Any) -> Any:
        """
        Processes an item by passing it sequentially through all added processors.
        Handles both synchronous and asynchronous processors.

        Args:
            item: The data item to process.

        Returns:
            The item after being processed by all processors.

        Raises:
            PipelineException: If any processor in the pipeline raises an exception,
                               or if a processor returns None.
        """
        processed_item = item
        for processor_instance in self._processors:
            try:
                # Pass the item to the current processor instance
                # The Processor.__call__ method handles the actual execution
                processed_item = processor_instance(processed_item)

                # Handle awaitables if a processor is async
                if isinstance(processed_item, Awaitable):
                    processed_item = await processed_item

                # Ensure processors don't return None, as it can break the chain
                if processed_item is None:
                    raise PipelineException(
                        f"Processor {processor_instance!r} returned None, which is not allowed."
                    )

            except Exception as e:
                # Catch any exception from the processor (including type errors from Processor.__call__)
                # and wrap it in PipelineException for consistent error reporting.
                # The {e} part will include the original error message, like the TypeError.
                # The {processor_instance!r} will now use the improved __repr__ of the Processor.
                raise PipelineException(f"Error processing item with {processor_instance!r}: {e}")

        return processed_item
