# OpenSkyNetwork-Data-extraction
Extrait, et charges des données de vols via l'api d'OpenSkyNetwork sur une base de donnée DuckDB

![img.png](img.png)

### Mise en place environnement

#### 1. Générer un token
Exécuter le script get_access_token pour pouvoir requêter sur l'api
```shell
sh get_access_token.sh
```
:warning: **Le token expire au bout de 30 minutes**


#### 2. Créer la connection DUCK_DB dans Airflow

Se rendre sur l'interface Airflow à l'adresse 0.0.0.0:8080 :

Aller dans Admin > Connections
- Conn Id : DUCK_DB	
- Conn Type : duckdb

**Vous pouvez maintenant lancer les DAGs**

