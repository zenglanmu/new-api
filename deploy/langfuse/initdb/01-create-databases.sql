-- Create the Langfuse database on the shared PostgreSQL server.
--
-- The postgres entrypoint runs this file only when its data directory is empty
-- (first boot), but the statement is guarded so running it manually later is
-- harmless. The database is owned by POSTGRES_USER, which Langfuse then uses
-- through DATABASE_URL.
\set ON_ERROR_STOP on

SELECT 'CREATE DATABASE "langfuse"'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'langfuse')
\gexec
