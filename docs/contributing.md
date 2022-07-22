# Contibuting

All contributions to *databases* are welcome!

## Issues

To make it as simple as possible for us to help you, please include the following:

* OS 
* python version
* databases version
* database backend (mysql, sqlite or postgresql)
* database driver (aiopg, aiomysql etc.)

Please try to always include the above unless you're unable to install *databases* or **know** it's not relevant
to your question or feature request.

## Pull Requests

It should be quite straight forward to get started and create a Pull Request.

!!! note
    Unless your change is trivial (typo, docs tweak etc.), please create an issue to discuss the change before
    creating a pull request.

To make contributing as easy and fast as possible, you'll want to run tests and linting locally. 

You'll need to have **python >= 3.6 (recommended 3.7+)** and **git** installed.

## Getting started

1. Clone your fork and cd into the repo directory
```bash
git clone git@github.com:<your username>/databases.git
cd databases
```

2. Create and activate virtual env
```bash
virtualenv env
source env/bin/activate
```

3. Install databases, dependencies and test dependencies
```bash
pip install -r requirements.txt
```

4. Checkout a new branch and make your changes
```bash
git checkout -b my-new-feature-branch
```

## Make your changes...

## Contribute

1. Formatting and linting - databases uses black for formatting, autoflake for linting and mypy for type hints check
run all of those with lint script
```bash
./scripts/lint
```

2. Prepare tests (basic)
   1. Set-up `TEST_DATABASE_URLS` env variable where you can comma separate urls for several backends
   2. The simples one is for sqlite alone: `sqlite:///test.db`

3. Prepare tests (all backends)
   1. In order to run all backends you need either a docker installation on your system or all supported backends servers installed on your local machine.
   2. A sample docker configuration that reflects the CI/CD workflow of databases might be:
   
   ```dockerfile
    version: '2.1'
    services:
      postgres:
        image: postgres:10.8
        environment:
          POSTGRES_USER: username
          POSTGRES_PASSWORD: password
          POSTGRES_DB: testsuite
        ports:
          - 5432:5432
    
      mysql:
        image: mysql:5.7
        environment:
          MYSQL_USER: username
          MYSQL_PASSWORD: password
          MYSQL_ROOT_PASSWORD: password
          MYSQL_DATABASE: testsuite
        ports:
          - 3306:3306
   ```
   3. To test all backends, the test urls need to consist of all possible drivers too, so a sample might look like following:
   ```text
    sqlite:///test.db,
    sqlite+aiosqlite:///test.db,
    mysql+aiomysql://username:password@localhost:3306/testsuite,
    mysql+asyncmy://username:password@localhost:3306/testsuite,
    postgresql+aiopg://username:password@127.0.0.1:5432/testsuite,
    postgresql+asyncpg://username:password@localhost:5432/testsuite
   ```

4. Run tests
```bash
./scripts/test
```

5. Build documentation
   1. If you have changed the documentation make sure it runs successfully. 
   You can preview the live documentation by running the following command:
```bash
./scripts/docs
```

6. Commit, push, and create your pull request
