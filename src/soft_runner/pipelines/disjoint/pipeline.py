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
def create_pipeline2(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(fork, inputs=None, outputs=["A1", "A2"], name="A_1"),
            node(one_one, inputs="A1", outputs="A3", name="A_2"),
            node(fork, inputs=None, outputs=["B1", "B2"], name="B_1"),

        ]
    )
disjoint = create_pipeline2()
