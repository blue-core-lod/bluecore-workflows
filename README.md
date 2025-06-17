
# ILS Middleware

Minimal viable product (MVP) for using [Apache Airflow][AF] to manage Blue Core workflows
that interact with institutional integrated library systems (ILS) and/or
library services platform (LSP). Currently there are Directed Acyclic Graphs (DAG)
for Stanford and Cornell Sinopia-to-ILS/LSP workflows. Alma users with the Linked Data API enabled can use the Alma DAG for connecting Sinopia to Alma.

---

## 🐳 Running Locally with Docker
Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).

>⚠️ **NOTE**: Make sure there is enough RAM available locally for the
> docker daemon, we recommend at least 5GB.

1. Clone repository: `git clone https://github.com/blue-core-lod/bluecore-workflows`
2. If it's commented out, uncomment the line `- ./dags:/opt/airflow/dags` in docker-compose.yaml (under `volumes`, under `x-airflow-common`).
3. Run `docker compose up` to start development docker environment
4. Access Airflow locally at http://localhost:8080
5. Access Keycloak locally at http://localhost:8081
---

## 🔐 Keycloak local development and credentials
Keycloak will automatically import realm config located at: `keycloak-export/bluecore-realm.json` \
when the Keycloak container starts. 
### 🔑 Logging into Airflow using Keycloak with developer credentials
This realm config contains the following:
> - Realm: `bluecore`
> - Client: `bluecore_api`
> - Username: `developer` #admin account
> - password: `123456`
> 
>⚠️ **Note**: Other account names that can be used are: `dev_op`,`dev_public`,`dev_user`, and `dev_viewer` with the same password. 
> These accounts reflect the roles associated in their name.
### 🔑 Logging into Keycloak master realm
You can also create a new realm and client in Keycloak by going to:
> - http://localhost:8081 
> - username: `admin` 
> - password: `admin`

###  💾 Exporting Keycloak realm configs
To export any changes to the bluecore realm config, you can use the following command:
```bash
   ./scripts/export-keycloak-realm.sh
````
This will export the realm config to the `keycloak-export/bluecore-realm.json` file.

---

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
---

## 📦 Dependency Management and Packaging
We are using [uv][UV] to manage dependency updates.\
Once you have uv installed, you can install the other project dependencies by running:
```bash
    # 📝 Note: This does not work initially with MacOS (Apple silicon) due to dependency errors compiling with C++
    uv sync
```
>⚠️ **Note**: In local development to avoid dependency errors using MacOS (Apple silicon), follow the: **[📝 Apple Silicon Dependency Install Guide](https://github.com/blue-core-lod/bluecore_info/wiki/bluecore-workflows-apple-silicon-install)**
> 
---

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

[AF]: https://airflow.apache.org/
[BLACK]: https://black.readthedocs.io/
[FLK8]: https://flake8.pycqa.org/en/latest/
[POET]: https://python-poetry.org/
[PYTEST]: https://docs.pytest.org/
[PYTESTCOV]: https://github.com/pytest-dev/pytest-cov
[MYPY]: https://mypy.readthedocs.io/en/stable/
[UV]: https://docs.astral.sh/uv/
[RUFF]: https://docs.astral.sh/ruff/
