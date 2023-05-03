# How to run?


## Custom Hook for inject failing nodes
`kedro run --params error:A-B`, `-` is used as delimeter, while `A` and `B` is the node name

## How to run the custom Runner?
`kedro run --runner=soft_runner.defer_runner.DeferRunner`