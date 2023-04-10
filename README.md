# DuckingIt
A framework to leverage the endless capabilities of serverless computing powered by DuckDB.

Please note that the framework currently supports only AWS Lambda functions. To use the framework, you must first create a Lambda layer of DuckDB that can be used within a Lambda function. Additionally, you must create a Lambda Executor function that can execute the actual DuckDB SQL. Once you've completed these setup steps, you can leverage the power of serverless functions through the SDK written in Python to perform analytics on a Data Lake.

While Apache Spark can perform similar (and more advanced) functions, the cost of running Spark clusters can be prohibitively expensive. As a result, a much more affordable alternative is to use a cluster of serverless functions, such as Lambda functions, to perform the same actions as Spark, without the need to turn them off manually.

## Installation
To install the Python SDK from PyPI execute the command below. Nonetheless, it's recommended that you first review the [setup](/README.md#setup) section in order to properly utilize the package.

```bash
pip install duckingit
```

## Setup
Please ensure that you have both Docker and Terraform installed before setting up the infrastructure.

The SDK acts as a gateway to the serverless function cluster, so the entire infrastructure must be set up before executing any commands on the DuckDB instances.

AWS Lambda employs layers to handle pre-installed packages, and DuckDB is no exception. To make the installed binaries work on AWS Lambda, Docker must be installed because the layer is built on the same architecture that runs on AWS Lambda.

The first step is to create the DuckDB layer. Running the following command will produce a duckdb-layer.zip file in the image/release/ folder:
```bash
make release-image
```

To proceed with setting up the infrastructure on AWS, you need to have Terraform installed. Follow these commands:
```bash
make tf-init
make tf-apply
```

After waiting for a minute or two, the process should be complete. You can now check for the presence of a Lambda function called DuckExecutor and a lambda layer called duckdb under Lambda layers.

Once you have verified the above components, the infrastructure should be set up and fully operational.

## Usage
... Coming up

## Contribution
Thank you for taking an interest in my project on GitHub. I am always looking for new contributors to help me improve and evolve my codebase. If you're interested in contributing, feel free to fork the repository and submit a pull request with your changes.

I welcome all kinds of contributions, from bug fixes to feature additions and documentation improvements. If you're not sure where to start, take a look at the issues tab or reach out to us for guidance.

Let's collaborate and make our project better together!


___________________________________
Ducking it ~ Killing it
