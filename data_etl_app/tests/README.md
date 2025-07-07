# Test Suite Documentation

This directory contains the test suite for the application. It is organized into several subdirectories, each serving a specific purpose:

## Directory Structure

- **conftest.py**: Contains fixtures that can be shared across multiple test files, such as a test client or mock Redis instances.
  
- **test_routes/**: This directory contains tests related to the application's routes. It is marked as a package and can be used to organize route-related tests.

- **test_services/**: This directory contains unit tests related to the application's data_etl_app.services. It is also marked as a package for better organization.

- **test_utils/**: This directory is reserved for utility tests. It is marked as a package and can be used to organize any utility-related tests that may be added in the future.

## Running the Tests

To run the tests, you can use the following command:

```bash
pytest
```

Make sure you have all the necessary dependencies installed. You can install them using:

```bash
pip install -r requirements.txt
```

## Additional Information

- Ensure that your environment variables are set up correctly before running the tests.
- For any new tests, please follow the existing structure and naming conventions to maintain consistency.