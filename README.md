.
├── README.md
│   ├── _SUCCESS
│   └── part-00000-77207a66-077c-464f-ab2f-e8b7417ed477-c000.snappy.parquet
├── requirements.txt
├── src
│   ├── __init__.py
│   ├── __pycache__
│   │   └── __init__.cpython-310.pyc
│   ├── main
│   │   ├── __init__.py
│   │   ├── __pycache__
│   │   └── topXitem.py
│   └── test
│       ├── __init__.py
│       ├── __pycache__
│       └── test.py

1) create a new environemnt and install the required dependencies using pip install -r requirements.txt

2) run PYTHONPATH=src python -m unittest discover src/test