#!/bin/bash

# Script to convert SQLite ? placeholders to PostgreSQL $1, $2, ... placeholders
# This will process files in the crates/db/db/src directory

find crates/db/db/src -name "*.rs" -type f -exec sed -i 's/?/$PLACEHOLDER/g' {} \;

echo "Converted ? placeholders to temporary PLACEHOLDER markers"
echo "Now manually review and convert to $1, $2, ... based on parameter order"