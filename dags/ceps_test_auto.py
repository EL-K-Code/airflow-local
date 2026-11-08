from datetime import datetime, timezone
from airflow.sdk import dag, task


@dag(
    dag_id="ceps_test_auto",
    schedule="*/2 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["formation", "ceps"],
)
def pipeline_ceps():

    @task
    def extraire():
        print("Extraction de données fictives")
        return 100

    @task
    def transformer(valeur):
        resultat = valeur * 2
        print(f"Transformation : {valeur} × 2 = {resultat}")
        return resultat

    @task
    def afficher(resultat):
        print(f"Résultat final : {resultat}")

    valeur = extraire()
    resultat = transformer(valeur)
    afficher(resultat)


pipeline_ceps()