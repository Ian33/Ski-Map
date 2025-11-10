# Render PostgreSQL Viewer

A Dash app that connects to a PostgreSQL database on Render and displays table contents in an interactive grid.

## Features

- Lists available tables in a dropdown
- Displays up to 500 rows from selected table using Dash AG Grid
- Automatically loads data from PostgreSQL using SQLAlchemy

## Deployment

1. Fork this repo
2. Add a PostgreSQL database to Render
3. Create a new **Web Service** on Render and connect this repo
4. Add your database URL as an environment variable:
   - `DATABASE_URL=postgresql://username:password@host:port/dbname`

