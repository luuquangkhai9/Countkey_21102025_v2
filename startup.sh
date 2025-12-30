#!/bin/bash
echo "Starting Python script run.py..."
python run.py &

echo "Starting API server..."
uvicorn api_keyword:app --host 0.0.0.0 --port 55555 --reload


