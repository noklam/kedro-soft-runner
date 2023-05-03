"""Project settings. There is no need to edit this file unless you want to change values
from the Kedro defaults. For further information, including these default values, see
https://kedro.readthedocs.io/en/stable/kedro_project_setup/settings.html."""

# Instantiated project hooks.
# from soft_runner.hooks import ProjectHooks

from kedro.framework.hooks import hook_impl


class SimulateNodeErrorHook:
    @hook_impl
    def before_pipeline_run(self, run_params, pipeline, catalog):
        params = run_params["extra_params"]
        simulate_error_node = params.get("error", "")
        simulate_error_node = simulate_error_node.split(
            "-"
        )  # If it is a hyphen-separated list
        print("Simulate nodes with error", simulate_error_node)
        self.error_nodes = simulate_error_node

    @hook_impl
    def before_node_run(self, node):
        if node.name in self.error_nodes:
            raise ValueError(
                f"Node with error {node.name}, List of error node {self.error_nodes}"
            )


HOOKS = (SimulateNodeErrorHook(),)

# Installed plugins for which to disable hook auto-registration.
# DISABLE_HOOKS_FOR_PLUGINS = ("kedro-viz",)

# Class that manages storing KedroSession data.
# from kedro.framework.session.shelvestore import ShelveStore
# SESSION_STORE_CLASS = ShelveStore
# Keyword arguments to pass to the `SESSION_STORE_CLASS` constructor.
# SESSION_STORE_ARGS = {
#     "path": "./sessions"
# }

# Class that manages Kedro's library components.
# from kedro.framework.context import KedroContext
# CONTEXT_CLASS = KedroContext

# Directory that holds configuration.
# CONF_SOURCE = "conf"

# Class that manages how configuration is loaded.
# from kedro.config import TemplatedConfigLoader
# CONFIG_LOADER_CLASS = TemplatedConfigLoader
# Keyword arguments to pass to the `CONFIG_LOADER_CLASS` constructor.
# CONFIG_LOADER_ARGS = {
#     "globals_pattern": "*globals.yml",
# }

# Class that manages the Data Catalog.
# from kedro.io import DataCatalog
# DATA_CATALOG_CLASS = DataCatalog
