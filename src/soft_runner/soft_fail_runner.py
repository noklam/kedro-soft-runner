"""``SequentialRunner`` is an ``AbstractRunner`` implementation. It can be
used to run the ``Pipeline`` in a sequential manner using a topological sort
of provided nodes.
"""

from collections import Counter
from itertools import chain

from pluggy import PluginManager

from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from kedro.pipeline.pipeline import _strip_transcoding
from kedro.runner.runner import run_node
from kedro.runner import SequentialRunner
from typing import Dict, Set, Any
from kedro.io.core import DataSetError
import logging

logger = logging.getLogger(__name__)


def node_dependencies_reversed(self) -> Dict[Node, Set[Node]]:
    """All dependencies of nodes where the first Node has a direct dependency on
    the second Node.

    Returns:
        Dictionary where keys are nodes and values are sets made up of
        their parent nodes. Independent nodes have this as empty sets.
    """
    dependencies = {node: set() for node in self._nodes}
    for parent in self._nodes:
        for output in parent.outputs:
            for child in self._nodes_by_input[_strip_transcoding(output)]:
                dependencies[parent].add(child)

    return dependencies


Pipeline.node_dependencies_reversed = property(node_dependencies_reversed)


def find_skip_nodes(node, node_dependencies, skip_nodes=None) -> Set[Node]:
    """Traverse the DAG with Breath-First-Search (BFS) to find all descendent nodes.
    `skip_nodes` is used to eliminate unnecessary search path, the `skip_nodes` will be
    updated during the search.

    Returns:
        The set of nodes that need to be skipped.
    """
    """_summary_

    Returns:
        _type_: _description_
    """
    queue = [node]
    node_set = set()
    while queue:
        parent_node = queue.pop()
        if parent_node in skip_nodes:
            continue
        for child in node_dependencies[parent_node]:
            node_set.add(child)
            if child not in skip_nodes:
                skip_nodes.add(child)
            queue.append(child)
    return node_set


class SoftFailRunner(SequentialRunner):
    """``SoftFailRunner`` is an ``AbstractRunner`` implementation that runs a
    ``Pipeline`` sequentially using a topological sort of provided nodes.
    Unlike the SequentialRunner, this runner will not terminate the pipeline
    immediately upon encountering node failure. Instead, it will continue to run on
    remaining nodes as long as their dependencies are fulfilled.
    """

    def _run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager,
        session_id: str = None,
    ) -> None:
        """The method implementing sequential pipeline running.

        Args:
            pipeline: The ``Pipeline`` to run.
            catalog: The ``DataCatalog`` from which to fetch data.
            hook_manager: The ``PluginManager`` to activate hooks.
            session_id: The id of the session.

        Raises:
            Exception: in case of any downstream node failure.
        """
        nodes = pipeline.nodes
        done_nodes = set()
        skip_nodes = set()
        node_dependencies = pipeline.node_dependencies_reversed

        load_counts = Counter(chain.from_iterable(n.inputs for n in nodes))
        logger.warning("Using Custom Runner")
        for exec_index, node in enumerate(nodes):
            try:
                if node in skip_nodes:
                    logger.warning(f"Skipping nodes {node}")
                    continue
                run_node(node, catalog, hook_manager, self._is_async, session_id)
                done_nodes.add(node)
            except Exception:
                new_nodes = find_skip_nodes(node, node_dependencies, skip_nodes)
                logger.warning(f"Skipped node: {str(new_nodes)}")

            # decrement load counts and release any data sets we've finished with
            for data_set in node.inputs:
                load_counts[data_set] -= 1
                if load_counts[data_set] < 1 and data_set not in pipeline.inputs():
                    catalog.release(data_set)
                    # try:
                    #     catalog.release(data_set)
                    # except DataSetError:
                    #     continue  # Temporary ignore the GC issue
            for data_set in node.outputs:
                if load_counts[data_set] < 1 and data_set not in pipeline.outputs():
                    catalog.release(data_set)
                    # try:
                    #     catalog.release(data_set)
                    # except DataSetError:
                    #     continue

            logger.info(
                "Completed %d out of %d tasks",
                exec_index + 1 - len(skip_nodes),
                len(nodes),
            )
        logger.warn(
            "%d node(s) failed during the execution",
            len(skip_nodes),
        )
        if skip_nodes:
            self._suggest_resume_scenario(pipeline, done_nodes)

    def run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager = None,
        session_id: str = None,
    ) -> Dict[str, Any]:
        """Run the ``Pipeline`` using the datasets provided by ``catalog``
        and save results back to the same objects.

        Args:
            pipeline: The ``Pipeline`` to run.
            catalog: The ``DataCatalog`` from which to fetch data.
            hook_manager: The ``PluginManager`` to activate hooks.
            session_id: The id of the session.

        Raises:
            ValueError: Raised when ``Pipeline`` inputs cannot be satisfied.

        Returns:
            Any node outputs that cannot be processed by the ``DataCatalog``.
            These are returned in a dictionary, where the keys are defined
            by the node outputs.

        """

        hook_manager = hook_manager or _NullPluginManager()
        catalog = catalog.shallow_copy()

        unsatisfied = pipeline.inputs() - set(catalog.list())
        if unsatisfied:
            raise ValueError(
                f"Pipeline input(s) {unsatisfied} not found in the DataCatalog"
            )

        # free_outputs = pipeline.outputs() - set(catalog.list())
        unregistered_ds = pipeline.data_sets() - set(catalog.list())
        for ds_name in unregistered_ds:
            catalog.add(ds_name, self.create_default_data_set(ds_name))

        if self._is_async:
            self._logger.info(
                "Asynchronous mode is enabled for loading and saving data"
            )
        self._run(pipeline, catalog, hook_manager, session_id)

        # self._logger.warn("Pipeline execution completed successfully.")

        # Override runner temporarily - need to handle the GC properly, not important for now
        # return {ds_name: catalog.load(ds_name) for ds_name in free_outputs}