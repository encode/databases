import os

import pytest
import sqlalchemy

from databases import DatabaseURL

assert "TEST_DATABASE_URLS" in os.environ, "TEST_DATABASE_URLS is not set."

DATABASE_URLS = [url.strip() for url in os.environ["TEST_DATABASE_URLS"].split(",")]


# @pytest.fixture(autouse=True, scope="module")
# def metadata():
#     yield sqlalchemy.MetaData()



# @pytest.fixture(autouse=True, scope="module")
# def create_test_database():
#     # Create test databases
#     import pdb; pdb.set_trace()
#     for url in DATABASE_URLS:
#         database_url = DatabaseURL(url)
#         if database_url.dialect == "mysql":
#             url = str(database_url.replace(driver="pymysql"))
#         engine = sqlalchemy.create_engine(url)
#         metadata.create_all(engine)

#     # Run the test suite
#     yield

#     # Drop test databases
#     for url in DATABASE_URLS:
#         database_url = DatabaseURL(url)
#         if database_url.dialect == "mysql":
#             url = str(database_url.replace(driver="pymysql"))
#         engine = sqlalchemy.create_engine(url)
#         metadata.drop_all(engine)
