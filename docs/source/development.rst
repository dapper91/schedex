Development
-----------

Initialize the development environment installing dev dependencies:

.. code-block:: console

    uv sync --all-extras --group dev --group linters


Code style
__________

After any code changes made make sure that the code style is followed.
To control that automatically install pre-commit hooks:

.. code-block:: console

    pre-commit install

They will be checking your changes for the coding conventions used in the project before any commit.


Testing
_______

- Prepare environment:

.. code-block:: console

    DOCKER_COMPOSE_PROJECT_PATH=tests/integration
    export `cat "$DOCKER_COMPOSE_PROJECT_PATH/.env" | xargs`

- Run unit tests

.. code-block:: console

    pytest -m unit

- Run postgres integration tests:

.. code-block:: console

    docker compose --project-directory $DOCKER_COMPOSE_PROJECT_PATH up -d postgres --wait
    pytest --cov=schedex -m "postgres or psycopg" --postgres-url "postgresql://$DB_USER:$DB_PASSWORD@localhost:5432/$DB_NAME"
    docker compose --project-directory "$DOCKER_COMPOSE_PROJECT_PATH" down

- Run mysql integration tests:

.. code-block:: console

    docker compose --project-directory "$DOCKER_COMPOSE_PROJECT_PATH" up -d mariadb --wait
    pytest --cov=schedex -m "mysql" --mysql-url "mysql://$DB_USER:$DB_PASSWORD@localhost:3306/$DB_NAME"
    docker compose --project-directory "$DOCKER_COMPOSE_PROJECT_PATH" down

- Run mongodb integration tests

.. code-block:: console

    docker compose --project-directory "$DOCKER_COMPOSE_PROJECT_PATH" up -d mongodb --wait
    pytest --cov=schedex -m "mongodb" --mongodb-url "mongodb://$DB_USER:$DB_PASSWORD@localhost:27017/?replicaSet=rs0"
    docker compose --project-directory "$DOCKER_COMPOSE_PROJECT_PATH" down


Documentation
_____________

If you've made any changes to the documentation, make sure it builds successfully.
To build the documentation follow the instructions:

- Install documentation generation dependencies:

.. code-block:: console

    uv sync --all-extras --group dev --group docs

- Build the documentation:

.. code-block:: console

    sphinx-build docs/source docs/build
