# Testing

For Pyspark testing we can use the library **chispa** , which provides lot of helper functions

```bash
├── README.md
├── __init__.py                     # package file is must
├── conftest.py                     # to setup the variables which can be used across the test methods 
└── utils
    ├── test_sparkutilfunctions.py  # declaring all the test case methods
    └── test_utilfunctions.py       # declaring all the test case methods
```

To run one test method

```bash
pytest -vv -s --disable-pytest-warnings test/test_sparkutilfunctions.py -k 'test__get_corresponding_datatype'
```

To run test coverage also

```bash
pytest -vv -s --disable-pytest-warnings  --doctest-modules --junitxml=junit/test-results.xml --cov --cov-report=xml --cov-report=html
```
