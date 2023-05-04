# How to run?


## Custom Hook for inject failing nodes
`kedro run --params error:A-B`, `-` is used as delimeter, while `A` and `B` is the node name

## How to run the custom Runner?
`kedro run --runner=soft_runner.defer_runner.SoftFailRunner`


# Development notes
To test the result filtered node
```python
kedro ipython
p = pipelines["__default__"]
p = pipeline.filter(from_nodes=["B","F"])

from kedro.runner import SequentialRunner
runner = SequentialRunner()
runner.run(p, catalog)
```