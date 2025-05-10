
# ILS Middleware
Minimal viable product (MVP) for using [Apache Airflow][AF] to manage Blue Core workflows
that interact with institutional integrated library systems (ILS) and/or
library services platform (LSP). Currently there are Directed Acyclic Graphs (DAG)
for Stanford and Cornell Sinopia-to-ILS/LSP workflows. Alma users with the Linked Data API enabled can use the Alma DAG for connecting Sinopia to Alma.

## 🐳 Running Locally with Docker
Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).

> 📝 **NOTE** Make sure there is enough RAM available locally for the
> docker daemon, we recommend at least 5GB.

1. Clone repository: `git clone https://github.com/blue-core-lod/bluecore-workflows`
2. If it's commented out, uncomment the line `- ./dags:/opt/airflow/dags` in docker-compose.yaml (under `volumes`, under `x-airflow-common`).
3. Run `docker compose up airflow-init` to initialize the Airflow
4. Bring up airflow, `docker compose up` to run the containers in the foreground,
   add `docker compose up -d` to run as a daemon.
5. Access Airflow locally at http://localhost:8080

### 📨 Setup the SQS queue in localstack for local development

```bash 
   AWS_ACCESS_KEY_ID=999999 AWS_SECRET_ACCESS_KEY=1231 aws sqs \
       --endpoint-url=http://localhost:4566 create-queue \
       --region us-west-2 \
       --queue-name all-institutions
```

### 🔗  Setup the local AWS connection for SQS

1. From the `Admin > Connections` menu
2. Click the "+"
3. Add an Amazon Web Services connection with the following settings:

    * Connection Id: aws_sqs_connection
    * Login: 999999
    * Password: 1231
    * Extra: `{"host": "http://localstack:4566", "region_name": "us-west-2"}`

### 📤 Send a message to the SQS queue

In order to test a dag locally, a message must be sent to the above queue:
```bash
   AWS_ACCESS_KEY_ID=999999 AWS_SECRET_ACCESS_KEY=1231 aws sqs \
       send-message \
       --endpoint-url=http://localhost:4566 \
       --queue-url https://localhost:4566/000000000000/all-institutions \
       --message-body file://tests/fixtures/sqs/test-message.json
```

📝 Note: the test message content in `tests/fixtures/sqs/test-message.json` contains an email address that you can update to your own.

## ✏️ Editing existing DAGs
The `dags/stanford.py` contains Stanford's Symphony and FOLIO workflows from
Sinopia editor user initiated process. The `dags/cornell.py` DAG is for Cornell's
FOLIO workflow. Editing either of these code files will change the DAG.

## 🆕 Adding a new DAG
In the `dags` subdirectory, add a python module for the DAG. Running Airflow
locally through Docker (see above), the DAG should appear in the list of DAGs
or display any errors in the DAG.

To add any new DAGs to `blue-core-lod/bluecore-workflows:latest` image, you can either
* build the image locally with `docker build -t blue-core-lod/bluecore-workflows:latest .` or,
* if commented out, uncomment the `build: .` line (under `x-airflow-common`) in `docker-compose.yaml`
while commenting out the previous line `image: ${AIRFLOW_IMAGE_NAME:-blue-core-lod/bluecore-workflows:latest}`.

## 📦 Dependency Management and Packaging
We are using [uv][UV] to manage dependency updates.\
Once you have uv installed, you can install the other project dependencies by running:
```bash
    # 📝 Note: This does not work with Mac OS (Apple Silicone) due to dependency errors compiling with C++
    uv sync
```
📝 **Note**: In local development to avoid dependency errors using Mac OS (Apple Silicone), you can run: 
```bash
  uv pip install ".[dev]"
```

## 🧪 Automated Tests
The [pytest][PYTEST] framework is used to run the tests.  Tests can be invoked manually by calling `uv run pytest` (which will save an xml formatted [coverage report][PYTESTCOV], as well as printing the coverage report to the terminal).

## 🛠️ Building Python Package
If you plan on building a local Docker image, be sure to build the Python
installation wheel first by running `uv build`.

## 🔍 Typechecking
The [mypy][MYPY] static type checker is used to find type errors (parameter passing, assignment, return, etc of incompatible value types).  CI runs `uv run mypy --ignore-missing-imports .` (as not all imports have type info available).

## 🔎 Linting
The [flake8][FLK8] Python code linter can be manually run by invoking `uv run flake8` from
the command-line. Configuration options are in the `setup.cfg` file, under the flake8 section.

## 🧹 Code formatting
Code can be auto-formatted using [ruff][RUFF].
To have Black apply formatting: `uv run ruff format .`
⚠️ Note: in local development use `ruff format .` to avoid dependency errors

[AF]: https://airflow.apache.org/
[BLACK]: https://black.readthedocs.io/
[FLK8]: https://flake8.pycqa.org/en/latest/
[POET]: https://python-poetry.org/
[PYTEST]: https://docs.pytest.org/
[PYTESTCOV]: https://github.com/pytest-dev/pytest-cov
[MYPY]: https://mypy.readthedocs.io/en/stable/
[UV]: https://docs.astral.sh/uv/
[RUFF]: https://docs.astral.sh/ruff/
