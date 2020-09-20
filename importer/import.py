#!/usr/bin/env python

from os import environ
from datetime import timedelta
import psycopg2
import sys
import ijson.backends.yajl2_cffi as ijson
import rapidjson
from rapidjson import NM_DECIMAL, NM_NATIVE
import fire
from retrying import retry
import time
import gzip
from multiprocessing import cpu_count, Process, get_context
from multiprocessing.pool import Pool

def retry_if_db_error(exception):
    return isinstance(exception, psycopg2.OperationalError)

# we wait a bit for postgres to be ready since if used with docker, so docker might take a while to start
@retry(stop_max_delay=10000, retry_on_exception=retry_if_db_error)
def _pg_connect():
    host = environ.get("POSTGRES_HOST")
    dbname = environ.get("POSTGRES_DB")
    user = environ.get("POSTGRES_USER")
    password = environ.get("POSTGRES_PASSWORD")
    return psycopg2.connect(
        f"host={host} dbname={dbname} user={user} password={password}"
    )


def _pg_execute(sql, params=None):
    if params is None:
        params = {}
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


SINGLE_INSERT = """
    INSERT INTO import.zones
    VALUES (
        %(id)s, %(parent)s, %(name)s,
        %(admin_level)s, %(zone_type)s,
        %(osm_id)s, %(wikidata)s,
        ST_MakeValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%(geometry)s), 4326), 3857))
    )
"""

SINGLE_INSERT_OR_UPDATE = """
    DELETE FROM import.zones WHERE id=%(id)s;

    INSERT INTO import.zones
    VALUES (
        %(id)s, %(parent)s, %(name)s,
        %(admin_level)s, %(zone_type)s,
        %(osm_id)s, %(wikidata)s,
        ST_MakeValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%(geometry)s), 4326), 3857))
    );
"""

def import_zone(z_line):
    global pg_cur
    if isinstance(z_line, (str, bytes)):
        z = rapidjson.loads(z_line)
    else:
        z = z_line
    z["geometry"] = rapidjson.dumps(
        z.pop("geometry"),
        number_mode=NM_DECIMAL|NM_NATIVE
    )
    try:
        pg_cur.execute(SINGLE_INSERT, z)
    except IntegrityError:
        pass

def import_zone_with_update(z_line):
    global pg_cur
    if isinstance(z_line, (str, bytes)):
        z = rapidjson.loads(z_line)
    else:
        z = z_line
    z["geometry"] = rapidjson.dumps(
        z.pop("geometry"),
        number_mode=NM_DECIMAL|NM_NATIVE
    )
    pg_cur.execute(SINGLE_INSERT_OR_UPDATE, z)

def _import_cosmogony_to_pg(cosmogony_path, partial_import):
    _pg_execute("CREATE SCHEMA IF NOT EXISTS import;")

    if not partial_import:
        _pg_execute("DROP TABLE IF EXISTS import.zones;")

    _pg_execute(
        """
            CREATE TABLE IF NOT EXISTS import.zones(
                id bigint NOT NULL,
                parent bigint,
                name varchar,
                admin_level int,
                zone_type varchar,
                osm_id varchar,
                wikidata varchar,
                geometry geometry,
                PRIMARY KEY (id)
            )
            WITH (OIDS=FALSE);

            CREATE INDEX IF NOT EXISTS zones_geometry_idx ON import.zones USING gist(geometry);

            CREATE INDEX IF NOT EXISTS zones_parent_idx ON import.zones (parent);
        """
        )

    mp_context = get_context()
    class SafeProcess(mp_context.Process):
        def run(self):
            global pg_cur
            with _pg_connect() as pg_conn:
                with pg_conn.cursor() as pg_cur:
                    return super().run()
    mp_context.Process = SafeProcess

    def import_zones(zones_iterator, partial_import):
        print("Importing cosmogony zones to pg...")
        start = time.clock()
        def print_timer():
            print(
                f"{nb_zones} zones imported in "
                f"{timedelta(seconds=(time.clock()-start))}"
            )

        nb_workers = min(8, cpu_count())
        with Pool(nb_workers, context=mp_context) as pool:
            if partial_import:
                res = pool.imap_unordered(import_zone, zones_iterator, chunksize=10)
                # res = pool.imap_unordered(import_zone_with_update, zones_iterator, chunksize=10)
            else:
                res = pool.imap_unordered(import_zone, zones_iterator, chunksize=10)
            pool.close()
            nb_zones = 0
            for _ in res:
                nb_zones += 1
                if nb_zones % 10000 == 0:
                    print_timer()
            pool.join()

        print_timer()

    if cosmogony_path.endswith('.json'):
        with open(cosmogony_path, "rb") as f:
            zones = ijson.items(f, "zones.item")
            import_zones(zones, partial_import)
    elif cosmogony_path.endswith('.jsonl.gz'):
        with gzip.open(cosmogony_path) as f:
            zones = (line for line in f)
            import_zones(zones, partial_import)
    else:
        raise Exception("Unknown file extension in '{}'", cosmogony_path)

    print("Import done.")


def import_data(cosmogony_path, partial_import=False):
    """
    import the cosmogony data into pg

    The data are imported in an 'import' schema.

    The `publish` method needs to be called to make the data available
    """
    return _import_cosmogony_to_pg(cosmogony_path, partial_import)


def publish():
    """
    make the imported data available.

    atomic operation that move the `import` schema to `public`.
    """
    _pg_execute(
        """
        DROP TABLE if exists public.zones;

        ALTER TABLE import.zones SET SCHEMA public;
    """
    )


if __name__ == "__main__":
    fire.Fire()
