#!/bin/bash
# Move to the project directory
cd "$(dirname "$0")"

# Activate the virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run the application
python3 app.py
