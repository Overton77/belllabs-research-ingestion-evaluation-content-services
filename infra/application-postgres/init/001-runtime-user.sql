DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'belllabs_app') THEN
        CREATE ROLE belllabs_app LOGIN PASSWORD 'belllabs-app-local'
            NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOBYPASSRLS;
    END IF;
END
$$;
