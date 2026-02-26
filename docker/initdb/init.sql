-- cria role e banco na primeira inicialização do volume
DO
$$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bankpy_user') THEN
      CREATE ROLE bankpy_user WITH LOGIN PASSWORD 'Engenharia10';
   END IF;
END
$$;

DO
$$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'bankpy_db') THEN
      CREATE DATABASE bankpy_db OWNER postgres;
   END IF;
END
$$;

\connect bankpy_db

GRANT CONNECT ON DATABASE bankpy_db TO bankpy_user;
GRANT USAGE ON SCHEMA public TO bankpy_user;
GRANT CREATE ON SCHEMA public TO bankpy_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO bankpy_user;

