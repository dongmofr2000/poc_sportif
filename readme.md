POC Avantages Sportifs : Calcul et Impact Financier
Ce projet est une Preuve de Concept (POC) visant à valider la faisabilité technique et à calculer l'impact financier de la mise en place de deux avantages pour les salariés, basés sur leur activité physique : la Prime Sportive et les 5 Jours Bien-être.

1. 🎯 Objectifs du POC
Le POC a atteint les objectifs suivants :

Faisabilité Technique : Mise en place d'un pipeline ETL/ELT stable (Python/Pandas + PostgreSQL) capable de nettoyer, joindre et transformer des données hétérogènes (RH et Activités).

Calcul des Avantages : Application réussie de la logique métier pour déterminer l'éligibilité aux deux avantages.

Impact Financier : Génération d'un jeu de données final prêt pour le reporting dans Power BI, permettant de calculer le coût total de la Prime Sportive.
2. 🧱 Architecture et OutilsLe pipeline est construit autour d'une architecture simplifiée (mode Batch) pour ce POC, mais il est conçu pour être transposable à une architecture événementielle future (mentionnée dans la Note de Cadrage).ComposantRôlePython / PandasLangage de développement et bibliothèque pour l'analyse, le nettoyage, la jointure et la transformation des données (ETL/ELT).PostgreSQLBase de données de destination sécurisée pour stocker les résultats et servir de source unique pour le reporting.SQLAlchemy / Psycopg2Connecteurs Python utilisés pour interagir avec PostgreSQL.Fichiers CSVSources de données simulées (RH et Activités Strava-like).Slack (via Requests)Outil de Monitoring Opérationnel pour confirmer le succès du pipeline et envoyer des statistiques clés.3. ⚙️ Prérequis et ConfigurationPour exécuter le pipeline, les outils suivants doivent être installés et configurés.A. Prérequis SystèmePython 3.xPostgreSQL (version 9.5 ou ultérieure) installé et en cours d'exécution.

B. Configuration de la Base de Données
Assurez-vous que votre instance PostgreSQL est accessible.

Créez la base de données cible :

SQL

CREATE DATABASE sport_projet;
Vérifiez que les identifiants de connexion correspondent à la section 0. CONFIGURATION & CONNEXION du script (DB_USER, DB_PASS, etc.).

C. Installation des Dépendances Python
Créez et activez un environnement virtuel, puis installez les bibliothèques nécessaires :

Bash

# Crée un environnement virtuel
python -m venv venv 
# Active l'environnement (sur Windows)
.\venv\Scripts\activate 
# Installe les dépendances
pip install pandas numpy sqlalchemy psycopg2-binary requests

D. Fichiers SourcesLes deux fichiers suivants doivent être placés à la racine du répertoire du projet :donnees_rh.csv (Séparateur ;, Encodage latin-1)activites_simulees.csv (Séparateur ,, Encodage utf-8)4. 📝 Logique Métier Appliquée (Règles d'Éligibilité)Le script applique la logique suivante aux données :Règle 1 : Prime Sportive (5% du Salaire Brut)Éligibilité si et seulement si :Le mode de déplacement déclaré est considéré comme sportif (vélo, trottinette, marche/running, autres).La distance domicile-travail est inférieure aux plafonds :Marche/Running : Distance $\leq 15 \text{ km}$.Vélo, Trottinette, Autres : Distance $\leq 25 \text{ km}$.Règle 2 : 5 Jours Bien-êtreÉligibilité si et seulement si :Le salarié a enregistré au minimum 15 activités physiques dans l'année (total_activites \geq 15).5. ▶️ Exécution du Pipeline (Mode d'Emploi)Une fois les prérequis installés, lancez le script Python depuis votre terminal :
python pipeline_sportif.py
📋 Sortie Attendue
En cas de succès, la sortie console se termine comme suit :

...
⏳ Étape T (Transformation) : Nettoyage et calculs...
⚠️ Avertissement: Colonne 'distance_domicile_travail_km' non trouvée dans RH. Ajout d'une distance fictive (5 km) pour le test.
   -> Transformation terminée. 161 lignes prêtes à être chargées.
⏳ Étape L (Chargement) : Transfert vers PostgreSQL...
✅ Chargement terminé. Les données sont dans la table 'salaires_primes' de la base 'sport_projet'.
⚠️ Avertissement : URL Slack par défaut. Notification non envoyée.
🎉 PIPELINE TERMINÉ AVEC SUCCÈS.
6. 📈 Reporting et Monitoring
A. Résultat des Données
Le résultat final est chargé dans la table salaires_primes de la base de données sport_projet. Cette table est la source unique pour l'analyse et contient les colonnes clés :

collaborateur_id

salaire

montant_prime (Impact financier)

eligibilite_prime (Résultat de la Règle 1)

total_activites

eligibilite_jours_bien_etre (Résultat de la Règle 2)
B. Monitoring (Base)
Le script intègre un Monitoring Opérationnel basique via :

Gestion des Exceptions : Le pipeline s'arrête et affiche une erreur claire en cas de problème critique (KeyError, erreur de connexion, etc.).

Notification de Succès : La fonction send_slack_notification confirme la réussite du cycle complet E-T-L et inclut les statistiques clés du coût total.










