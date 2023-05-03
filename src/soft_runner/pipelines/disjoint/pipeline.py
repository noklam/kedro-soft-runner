from kedro.pipeline import Pipeline, node, pipeline


def one_one(x):
    return x


def fork():
    return ("dummy1", "dummy2")


def combine(x, y):
    return "dummy"


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(fork, inputs=None, outputs=["B", "E"], name="A"),
            node(one_one, inputs="B", outputs="C", name="B"),
            node(one_one, inputs="C", outputs="D", name="C"),
            node(one_one, inputs="E", outputs="F", name="F"),
            node(one_one, inputs="F", outputs="G", name="G"),
        ]
    )
