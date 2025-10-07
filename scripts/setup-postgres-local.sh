#!/bin/bash

# Alternative script to start PostgreSQL without Docker
# This will install and run PostgreSQL locally

echo "Setting up PostgreSQL for Torii indexer..."

# Check if PostgreSQL is already installed
if command -v psql &> /dev/null; then
    echo "PostgreSQL is already installed."
else
    echo "PostgreSQL not found. Please install it first:"
    echo "  sudo apt update"
    echo "  sudo apt install postgresql postgresql-contrib"
    echo ""
    echo "Then run this script again."
    exit 1
fi

# Check if PostgreSQL service is running
if ! sudo systemctl is-active --quiet postgresql; then
    echo "Starting PostgreSQL service..."
    sudo systemctl start postgresql
    sudo systemctl enable postgresql
fi

# Create torii user and database
echo "Setting up Torii database and user..."

sudo -u postgres psql << EOF
-- Drop existing database and user if they exist
DROP DATABASE IF EXISTS torii;
DROP USER IF EXISTS torii;

-- Create user and database
CREATE USER torii WITH PASSWORD 'torii';
CREATE DATABASE torii OWNER torii;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE torii TO torii;
GRANT ALL PRIVILEGES ON SCHEMA public TO torii;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO torii;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO torii;

-- Create extensions
\c torii
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

\q
EOF

echo ""
echo "✅ PostgreSQL setup complete!"
echo ""
echo "🐘 PostgreSQL Database Information:"
echo "Host: localhost"
echo "Port: 5432"
echo "Database: torii"
echo "Username: torii"
echo "Password: torii"
echo ""
echo "Connection URL: postgresql://torii:torii@localhost:5432/torii"
echo ""
echo "To connect manually:"
echo "  psql -h localhost -U torii -d torii"
echo ""
echo "To test the connection:"
echo "  psql postgresql://torii:torii@localhost:5432/torii -c 'SELECT version();'"