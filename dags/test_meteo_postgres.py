
from datetime import datetime, timezone
from airflow.sdk import dag, task


@dag(
    dag_id="test_meteo_postgres",
    schedule=None,
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["postgresql", "test"],
)
def test_postgresql():

    @task
    def verifier_connexion():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="meteo_postgres")

        resultat = hook.get_first(
            "SELECT current_database(), current_user"
        )

        print(f"Base : {resultat[0]}")
        print(f"Utilisateur : {resultat[1]}")
        print("Connexion PostgreSQL réussie !")

    verifier_connexion()


test_postgresql()
