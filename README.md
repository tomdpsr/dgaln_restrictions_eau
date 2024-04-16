# DGALN - Restrictions eau

Ce dépôt met en place une pipeline de données permettant de récupérer les [données des restrictions liées à la sécheresse](https://www.data.gouv.fr/fr/datasets/donnee-secheresse-propluvia/), de les traiter et de les analyser via superset


## Initialisation
Lancer le script d'initialisation permettant de créer les conteneurs dockers
```bash
./run_app.sh
```
## Airflow
Pour lancer la pipeline de données :
* Se connecter sur l'UI : http://localhost:8080/home (user/mdp: airflow/airflow)
* Lancer le dag <b> dag_restrictions_eau </b>

## Superset
Pour visualiser le dashboard :
* Se connecter sur l'UI : http://localhost:8088/ (user/mdp: admin/admin)
* Se rendre dans Dashboards > Import dashboard
* Importer l'archive <i> ./resources/dashboard_evolution_restrictions_eau.zip </i> (mdp: admin)
