# Statistques d'exécution de Map/Reduce

### Statistiques importantes lors de l'exécution d'un job Hadoop :

- **Map input records** : Le nombre de couples clé-valeur traitées avec appels à la fonction `map`.
- **Map output records** : Le nombre de couples clé-valeur produites par des appels à la fonction `map`.
- **Reduce input records** : Le nombre de couples clé-valeur traitées avec appels à la fonction `reduce`. Typiquement égal à `Map output records`, mais plus petit si certaines optimisations sont activées.
- **Reduce input groups** : Le nombre de clés distinctes traitées avec appels à la fonction `reduce`.
- **Reduce output records** : Le nombre de couples clé-valeur produites par des appels à la fonction `reduce`.

### Exemple HadoopPoem

- **Input** : Texte sur 17 lignes composé de 72 mots dont 59 distincts.
- **Map input records** : 17 (une par ligne).
- **Map output records** : 72 (une par mot).
- **Reduce input records** : 72.
- **Reduce input groups** : 59 (mots distincts).
- **Reduce output records** : 59 (calcul du nombre d’occurrences de chaque mot).


# Plus en détail

Voici les statistiques les plus importantes (pour démarrer) visualisées lors de l'exécution d'un job Hadoop :

- **Map input records**  
  - Le nombre de couples clé-valeur traités avec appels à la fonction map.

- **Map output records**  
  - Le nombre de couples clé-valeur produits par des appels à la fonction map.

- **Reduce input records**  
  - Le nombre de couples clé-valeur traités avec appels à la fonction reduce. Typiquement égal à Map output records, mais plus petit si certaines optimisations sont activées.

- **Reduce input groups**  
  - Le nombre de clés distinctes traitées avec appels à la fonction reduce.

- **Reduce output records**  
  - Le nombre de couples clé-valeur produits par des appels à la fonction reduce.

---

L'exemple hadoopPoem contient un texte sur **17** lignes composé de **72** mots dont **59** sont distincts.  
Un appel à la fonction map est réalisé pour chaque ligne du fichier. Nous avons donc :  
- **Map input records = 17**  
Une paire (clé, valeur) est générée par un mapper pour chaque mot présent dans la ligne. Nous avons donc :  
- **Map output records = 72**  
Toutes ces paires sont traitées par les reducers. Nous avons donc :  
- **Reduce input records = 72**  
Étant donné que le texte contient seulement **59** mots distincts, nous avons :  
- **Reduce input groups = 59**  
Enfin, le reduce calcule le nombre d'occurrences de chaque mot, et nous avons :  
- **Reduce output records = 59**

### Autres statistiques importantes :

- **Spilled Records**  
  - Le nombre de couples clé-valeur écrits sur disque par tous les appels aux fonctions map et reduce.

- **Map output materialized bytes**  
  - Les couples clé-valeur générés par les fonctions map qui ont été écrits sur disque (en octets).

- **SPLIT_RAW_BYTES**  
  - La taille des métadonnées des splits (portions de données) lus par le map.

- **Map output bytes**  
  - La taille des résultats des appels à la fonction map (en octets).

- **Reduce shuffle bytes**  
  - Le résultat des fonctions map copié par le shuffle vers les reducers (en octets).

- **Combine output records**  
  - Le résultat des fonctions "combine" (en octets) - si définies.

- **Combine input records**  
  - Le nombre d'entrées des fonctions "combine" - si définies.

- **Total committed heap usage (bytes)**  
  - Taille de la mémoire Java disponible.

### File Input Format Counters

- **Bytes Read**  
  - Taille des fichiers en entrée.

### FileSystemCounter

- **FILE_BYTES_WRITTEN = 67312**  
  - Taille des données (en octets) écrites sur HDFS par les appels aux fonctions map et reduce.

- **FILE_BYTES_READ = 2151**  
  - Taille des données (en octets) lues dans HDFS par les appels aux fonctions map et reduce.

### File Output Format Counters

- **Bytes Written**  
  - Taille des fichiers en sortie.
