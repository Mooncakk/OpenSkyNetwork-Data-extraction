# OpenSkyNetwork-Data-extraction
Extrait, et charges des données de vols via l'api d'OpenSkyNetwork sur une base de donnée DuckDB

![img_1.png](img_1.png)

### Mise en place environnement

#### 1. Créer la connection DUCK_DB dans Airflow

Se rendre sur l'interface Airflow à l'adresse 0.0.0.0:8080 :

Aller dans Admin > Connections
- Conn Id : DUCK_DB	
- Conn Type : duckdb

**Vous pouvez maintenant lancer les DAGs**

