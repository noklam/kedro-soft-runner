"""Project pipelines."""
from typing import Dict

# from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from .pipelines.disjoint.pipeline import create_pipeline
from .pipelines.disjoint.pipeline import disjoint
disjoint_pipeline = create_pipeline()
def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = {}
    pipelines["__default__"] = disjoint
    return pipelines
