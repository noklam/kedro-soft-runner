"""``SequentialRunner`` is an ``AbstractRunner`` implementation. It can be
used to run the ``Pipeline`` in a sequential manner using a topological sort
of provided nodes.
"""

from collections import Counter
from itertools import chain

from pluggy import PluginManager

from kedro.io import AbstractDataSet, DataCatalog, MemoryDataSet
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from kedro.pipeline.pipeline import _strip_transcoding
from kedro.runner.runner import AbstractRunner, run_node
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


def find_descendent_nodes(node, node_dependencies, skip_nodes):
    """Traverse the DAG with BFS to find all descendent nodes and merge them to the skip_nodes"""
    queue = [node]
    node_set = set()
    while queue:
        parent_node = queue.pop()
        if parent_node in skip_nodes:
            continue
        print("!!!!", node_dependencies)
        print(parent_node, parent_node.__repr__(), type(parent_node))
        print(node_dependencies[parent_node])
        for child in node_dependencies[parent_node]:
            node_set.add(child)
            if child not in skip_nodes:
                skip_nodes.add(child)
                logger.warn(
                f"Queue: {queue}, Child: {child}, Parent_node:{node_dependencies[parent_node]}"
            )
            queue.append(child)
    return node_set


class DeferRunner(AbstractRunner):
    """``SequentialRunner`` is an ``AbstractRunner`` implementation. It can
    be used to run the ``Pipeline`` in a sequential manner using a
    topological sort of provided nodes.
    """

    def __init__(self, is_async: bool = False):
        """Instantiates the runner classs.

        Args:
            is_async: If True, the node inputs and outputs are loaded and saved
                asynchronously with threads. Defaults to False.

        """
        super().__init__(is_async=is_async)

    def create_default_data_set(self, ds_name: str) -> AbstractDataSet:
        """Factory method for creating the default data set for the runner.

        Args:
            ds_name: Name of the missing data set

        Returns:
            An instance of an implementation of AbstractDataSet to be used
            for all unregistered data sets.

        """
        return MemoryDataSet()

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
                new_nodes = find_descendent_nodes(node, node_dependencies, skip_nodes)
                logger.warning(f"Adding new nodes to skip {new_nodes}")
                self._suggest_resume_scenario(pipeline, done_nodes, catalog)

            # decrement load counts and release any data sets we've finished with
            for data_set in node.inputs:
                load_counts[data_set] -= 1
                if load_counts[data_set] < 1 and data_set not in pipeline.inputs():
                    catalog.release(data_set)

            for data_set in node.outputs:
                if load_counts[data_set] < 1 and data_set not in pipeline.outputs():
                    catalog.release(data_set)

            logger.info(
                "Completed %d out of %d tasks", exec_index + 1 - len(skip_nodes), len(nodes)
            )
            logger.info(
                f"Skipped nodes {skip_nodes}"
            )


