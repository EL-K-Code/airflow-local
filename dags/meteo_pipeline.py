
import csv
import io
import math

from datetime import datetime, timezone, timedelta
from pathlib import Path

from airflow.sdk import dag, task


# ==========================================================
# CONFIGURATION
# ==========================================================

VILLES = {
    "Paris": (48.8566, 2.3522),
    "Dakar": (14.7167, -17.4677),
    "Lome": (6.1375, 1.2123),
}

DOSSIER_CSV = Path("/opt/airflow/data/meteo")

BUCKET_S3 = "meteo-brut"
CONNEXION_S3 = "minio_s3"
CONNEXION_POSTGRES = "meteo_postgres"

API_METEO = (
    "https://api.met.no/weatherapi/"
    "locationforecast/2.0/compact"
)

# Remplacer par une vraie adresse de contact
USER_AGENT = (
    "AirflowMeteoDemo/1.0 "
    "(contact: airflow-demo@example.com)"
)


# ==========================================================
# DEFINITION DU DAG
# ==========================================================

@dag(
    dag_id="meteo_pipeline",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["meteo", "etl", "data-quality", "s3", "postgresql"],
)
def pipeline_meteo():

    # ------------------------------------------------------
    # TACHE 1 : EXTRACTION METEO
    # ------------------------------------------------------

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=20),
    )
    def extraire():
        import requests

        observations = []

        for ville, (latitude, longitude) in VILLES.items():

            reponse = requests.get(
                API_METEO,
                params={
                    "lat": latitude,
                    "lon": longitude,
                },
                headers={
                    "User-Agent": USER_AGENT
                },
                timeout=30,
            )

            reponse.raise_for_status()

            donnees = reponse.json()

            premiere_prevision = (
                donnees["properties"]["timeseries"][0]
            )

            details = (
                premiere_prevision["data"]["instant"]["details"]
            )

            observations.append({
                "ville": ville,
                "observation_utc": premiere_prevision["time"],
                "temperature": details["air_temperature"],
                "humidite": details["relative_humidity"],
                "vent": round(
                    details["wind_speed"] * 3.6,
                    2,
                ),
            })

        print(
            f"EXTRACTION OK : "
            f"{len(observations)} observations"
        )

        return observations

    # ------------------------------------------------------
    # TACHE 2 : CONTROLE QUALITE DES DONNEES
    # ------------------------------------------------------

    @task
    def controler_qualite(observations):

        erreurs = []

        villes_attendues = set(VILLES.keys())
        villes_observees = set()

        colonnes_obligatoires = [
            "ville",
            "observation_utc",
            "temperature",
            "humidite",
            "vent",
        ]

        # Vérifier le nombre d'observations
        if len(observations) != len(VILLES):
            erreurs.append(
                f"Nombre incorrect d'observations : "
                f"{len(observations)} au lieu de {len(VILLES)}"
            )

        # Vérifier chaque observation
        for numero, obs in enumerate(observations, start=1):

            # 1. Colonnes obligatoires
            manquantes = [
                colonne
                for colonne in colonnes_obligatoires
                if colonne not in obs
                or obs[colonne] is None
                or obs[colonne] == ""
            ]

            if manquantes:
                erreurs.append(
                    f"Ligne {numero} : "
                    f"valeurs manquantes {manquantes}"
                )
                continue

            ville = obs["ville"]

            # 2. Vérification des villes
            if ville not in villes_attendues:
                erreurs.append(
                    f"Ville inconnue : {ville}"
                )

            if ville in villes_observees:
                erreurs.append(
                    f"Ville présente plusieurs fois : {ville}"
                )

            villes_observees.add(ville)

            # 3. Vérification de la date
            try:
                date_obs = datetime.fromisoformat(
                    obs["observation_utc"].replace(
                        "Z", "+00:00"
                    )
                )

                if date_obs.tzinfo is None:
                    raise ValueError(
                        "La date doit inclure un fuseau horaire"
                    )

            except (ValueError, TypeError, AttributeError):
                erreurs.append(
                    f"{ville} : date invalide"
                )

            # 4. Vérification des variables numériques
            limites = {
                "temperature": (-100, 65),
                "humidite": (0, 100),
                "vent": (0, 500),
            }

            for variable, (minimum, maximum) in limites.items():

                valeur = obs[variable]

                if (
                    not isinstance(valeur, (int, float))
                    or isinstance(valeur, bool)
                    or not math.isfinite(valeur)
                ):
                    erreurs.append(
                        f"{ville} : {variable} "
                        f"n'est pas un nombre valide"
                    )
                    continue

                if not minimum <= valeur <= maximum:
                    erreurs.append(
                        f"{ville} : {variable}={valeur} "
                        f"hors limites [{minimum}, {maximum}]"
                    )

        # Vérifier les villes manquantes
        villes_manquantes = (
            villes_attendues - villes_observees
        )

        if villes_manquantes:
            erreurs.append(
                f"Villes manquantes : "
                f"{sorted(villes_manquantes)}"
            )

        # Résultat du contrôle
        if erreurs:

            print("CONTROLE QUALITE : ECHEC")

            for erreur in erreurs:
                print(f"ERREUR : {erreur}")

            raise ValueError(
                "Qualité des données non conforme : "
                + " | ".join(erreurs)
            )

        print("CONTROLE QUALITE : SUCCESS")
        print(f"Nombre d'observations : {len(observations)}")
        print(f"Villes : {sorted(villes_observees)}")
        print("Valeurs numériques et dates : conformes")

        return observations

    # ------------------------------------------------------
    # TACHE 3 : ENREGISTREMENT CSV
    # ------------------------------------------------------

    @task
    def enregistrer_csv(observations):

        DOSSIER_CSV.mkdir(
            parents=True,
            exist_ok=True,
        )

        horodatage = datetime.now(
            timezone.utc
        ).strftime("%Y%m%d_%H%M%S_%f")

        chemin = (
            DOSSIER_CSV / f"meteo_{horodatage}.csv"
        )

        colonnes = [
            "ville",
            "observation_utc",
            "temperature",
            "humidite",
            "vent",
        ]

        with chemin.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as fichier:

            writer = csv.DictWriter(
                fichier,
                fieldnames=colonnes,
            )

            writer.writeheader()
            writer.writerows(observations)

        print(f"CSV créé : {chemin}")

        return str(chemin)

    # ------------------------------------------------------
    # TACHE 4 : ENVOI VERS MINIO / S3
    # ------------------------------------------------------

    @task(retries=2)
    def envoyer_s3(chemin_csv):

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(
            aws_conn_id=CONNEXION_S3
        )

        nom_fichier = Path(chemin_csv).name
        cle_s3 = f"observations/{nom_fichier}"

        hook.load_file(
            filename=chemin_csv,
            key=cle_s3,
            bucket_name=BUCKET_S3,
            replace=True,
        )

        print(
            f"Fichier envoyé : "
            f"s3://{BUCKET_S3}/{cle_s3}"
        )

        return {
            "bucket": BUCKET_S3,
            "key": cle_s3,
        }

    # ------------------------------------------------------
    # TACHE 5 : CHARGEMENT POSTGRESQL DEPUIS S3
    # ------------------------------------------------------

    @task(retries=2)
    def charger_postgresql(archive_s3):

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # Lire les données directement depuis MinIO

        s3 = S3Hook(
            aws_conn_id=CONNEXION_S3
        )

        contenu = s3.read_key(
            key=archive_s3["key"],
            bucket_name=archive_s3["bucket"],
        )

        if not contenu:
            raise ValueError(
                "Fichier CSV S3 vide ou introuvable"
            )

        lignes = list(
            csv.DictReader(io.StringIO(contenu))
        )

        if not lignes:
            raise ValueError(
                "Aucune observation dans le CSV"
            )

        # Connexion PostgreSQL

        postgres = PostgresHook(
            postgres_conn_id=CONNEXION_POSTGRES
        )

        creation_table = """
            CREATE TABLE IF NOT EXISTS observations_meteo (
                ville TEXT NOT NULL,
                observation_utc TIMESTAMPTZ NOT NULL,
                temperature DOUBLE PRECISION,
                humidite DOUBLE PRECISION,
                vent DOUBLE PRECISION,
                date_ingestion TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (ville, observation_utc)
            )
        """

        insertion = """
            INSERT INTO observations_meteo (
                ville,
                observation_utc,
                temperature,
                humidite,
                vent
            )
            VALUES (%s, %s, %s, %s, %s)

            ON CONFLICT (ville, observation_utc)
            DO UPDATE SET
                temperature = EXCLUDED.temperature,
                humidite = EXCLUDED.humidite,
                vent = EXCLUDED.vent,
                date_ingestion = NOW()
        """

        conn = postgres.get_conn()

        try:
            with conn:
                with conn.cursor() as cur:

                    cur.execute(creation_table)

                    for ligne in lignes:

                        cur.execute(
                            insertion,
                            (
                                ligne["ville"],
                                ligne["observation_utc"],
                                ligne["temperature"],
                                ligne["humidite"],
                                ligne["vent"],
                            ),
                        )

            print(
                f"{len(lignes)} observations "
                "chargées dans PostgreSQL"
            )

        finally:
            conn.close()

    # ------------------------------------------------------
    # ORCHESTRATION DES TACHES
    # ------------------------------------------------------

    donnees = extraire()

    donnees_valides = controler_qualite(donnees)

    fichier_csv = enregistrer_csv(donnees_valides)

    archive_s3 = envoyer_s3(fichier_csv)

    charger_postgresql(archive_s3)


pipeline_meteo()
