from kedro.pipeline import Pipeline, node, pipeline

country_names = [
    "Chile",
    "Finland",
    "Germany",
    "Greece",
    "Ireland",
    "Israel",
    "Italy",
    "Norway",
    "Peru",
    "Philippines",
    "Poland",
    "Portugal",
    "Romania",
    "Singapore",
]


def process_data(x):
    if x["id"].isin(["error"]).any():
        raise ValueError("Invalid Value!")
    return x


def fork():
    return ("dummy1", "dummy2")


def combine(x, y):
    return "dummy"


def many_to_one(*args):
    return "something"


def generate_country_pipeline(country):
    return pipeline(
        [
            node(
                process_data,
                inputs=country,
                outputs=country + "_feature",
                name=country + "_feature_engineering",
            ),
            node(
                process_data,
                country + "_feature",
                country + "_processed",
                name="process_" + country,
            ),
        ]
    )


def create_pipeline(**kwargs) -> Pipeline:
    countries_pipeline = sum(
        [generate_country_pipeline(country) for country in country_names]
    )
    combined_inputs = [country + "_processed" for country in country_names]
    combine_node = node(
        many_to_one,
        inputs=combined_inputs,
        outputs="final_output",
        name="combine_result",
    )
    return countries_pipeline + pipeline([combine_node])

if __name__ == "__main__":
    # Generate the input file need
    import pandas as pd

    raw_csv = pd.read_csv("src/soft_runner/pipelines/disjoint/raw.csv")
    for country in country_names:
        csv_name = country + ".csv"
        raw_csv.to_csv(csv_name, index=False)
