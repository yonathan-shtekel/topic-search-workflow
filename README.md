# KafkaSearchWorkflow

## Overview

KafkaSearchWorkflow is a sample project showcasing the use of Temporal for building reliable distributed workflows. It utilizes the Temporal framework to orchestrate the execution of two activities: `get_partitions` and `topic_search`.

## Prerequisites

Before running the project, make sure you have the following dependencies installed:

- [Temporal Server](https://docs.temporal.io/docs/server/install)
- [Python 3.7+](https://www.python.org/downloads/)
- [Poetry](https://python-poetry.org/docs/#installation)

## Getting Started

1. Clone the repository:

    ```bash
    git clone https://github.com/your-username/KafkaSearchWorkflow.git
    cd KafkaSearchWorkflow
    ```

2. Install dependencies using Poetry:

    ```bash
    poetry install
    ```

3. Run the Temporal Server:

    ```bash
    temporal start
    ```

4. Run the example workflow:

    ```bash
    poetry run python search_worker.py
    ```

## Project Structure

- `activities/`: Contains the activity implementations (`get_partitions`, `topic_search`).
- `search_workflow.py`: Defines the `KafkaSearchWorkflow` workflow.
- `main.py`: Script to run the example workflow.
- `pyproject.toml`: Poetry project file.

## Workflow Execution

The `KafkaSearchWorkflow` orchestrates the execution of two activities:

1. `get_partitions`: Fetches partition information.
2. `topic_search`: Performs a topic search using the partition information obtained.

## Configuration

Adjust configuration parameters in `main.py` as needed, such as the Temporal server address and task queue name.

## Contributions

Feel free to contribute to the project by submitting issues or pull requests.

## License

This project is licensed under the [MIT License](LICENSE).

Adjust the README file further based on the specifics of your project, such as adding additional configuration details, usage examples, or any other relevant information.
