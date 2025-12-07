## MapReduce avec Eclipse

Voici les étapes pour configurer Eclipse afin de pouvoir utiliser Map-Reduce :
1. Dans le menu file, importer un nouveau Maven projet en utilisant le `pom.xml` du projet.

Maintenant, vous pouvez tester le programme `src/WordCount.java` à partir d’Eclipse (voir le menu "Run", ou faire `CTRL+F11` pour exécuter le fichier source courant, il est possible de lancer le debugger avec `F11`).

Après chaque exécution, les résultats sont enregistrés dans des sous-dossiers `output/wordCount-xxxx`. En cliquant sur « Refresh » (ou `F5` dans le Package Explorer) vous pouvez voir les fichiers de résultats dans Eclipse. Vous pouvez aussi les voir en allant explorer directement le dossier via l'explorateur de fichiers du système.

## MapReduce avec Eclipse - (Alternative 1)

Télécharger l'archive [TP_HMIN122M-hadoop.zip](https://www.dropbox.com/s/d68l5r2dntyr6h9/TP_HMIN122M-hadoop.zip?dl=0)

Voici les étapes pour configurer Eclipse afin de pouvoir utiliser Map-Reduce :

1. Décompressez l'archive en conservant le dossier racine `TP_HMIN122M-hadoop/`.
2. Dans le menu file, créez un nouveau « java projet » (dans java).
3. Décochez la case "Use default location".
4. Dans le champ "location" sélectionnez le chemin vers votre dossier `TP_HMIN122M-hadoop/`.
5. Cliquez sur "next".
6. Puis cliquez sur l'onglet « Libraries », et assurez-vous qu'Eclipse détecte bien 7 librairies en `.jar` dans "Classpath".  
   - Si ce n'est pas le cas, cliquez sur « Add external JARs » et naviguez pour vous rendre dans le dossier `TP_HMIN122M-hadoop/lib-dev`, puis ajoutez-y tous les `.jar` qui y sont présents.
7. Cliquez sur « finish » pour créer le projet.

## Installation (Alternative 2)

En cas de difficulté, voici une autre alternative.

1. Il est possible de directement charger le projet à partir de l'archive.
2. Dans le menu file, cliquez sur "Open projects from File System".
3. Cliquez sur "Archive...".
4. Sélectionnez `TP_HMIN122M-hadoop.zip`, puis cliquez sur "OK".
5. Dans la liste "Folder" qui s'affiche : désélectionnez la première ligne : `TP_HMIN122M-hadoop.zip_expanded`.
6. Cliquez sur "Finish".

Le projet `TP_HMIN122M-hadoop` s'ajoutera alors dans la fenêtre "Project Explorer". Les fichiers sur le disque seront localisés à la racine de votre workspace, vous pouvez les retrouver en faisant : clic droit sur votre projet → "show in" → "System Explorer".

## Problème sur les nouveaux postes informatique

L'exception levée sur les nouveaux postes informatiques est due au nouveau mécanisme d'authentification de l'UM, où votre nom utilisateur correspond à votre adresse e-mail. Kerberos, le protocole d'authentification utilisé par Hadoop, utilise des règles différentes pour les adresses email.

La solution est de modifier votre nom utilisateur lors de l'exécution du programme. Pour ce faire, il suffit d'ajouter la variable d'environnement `HADOOP_USER_NAME` avant l'exécution du programme.  
Le plus simple est de l'ajouter directement via Eclipse où l'on va modifier la configuration à l'exécution :

1. Faites un clic droit sur la racine de votre projet, situé dans l'onglet à droite.
2. Suivez "Run as" → "Run Configurations".
3. Allez à l'onglet "Environment".
4. Cliquez sur "New...".
   - Name : `HADOOP_USER_NAME`
   - Value : `user`
5. Exécutez le programme WordCount et vérifiez que tout fonctionne.


## Problèmes de securité. Forcer l'utilisation de Java 11.

- Si vous utiliséz IntelliJ, vous devez vous assurer d'utiliser Java 11. Pour ce faire configurer l'option `Build and Run` avec valeur `Bundle` ; cela permet d'utiliser la version spécifée dans le `pom.xml`.
![Configuration Exécution](IntelliJ-SetJavaBundle.png)

## Bug de Hadoop sous Windows

(critique, on déconseille l'utilisation de Windows)

L'utilisation de Hadoop sous Windows entraîne la levée d'une exception de type IOException (Failed to set permissions of path: `\tmp\hadoop-user\mapred\staging\user722309568\.staging to 0700`). C'est un bug connu de Hadoop ([cf. HADOOP-7682](https://issues.apache.org/jira/browse/HADOOP-7682)), qu'il est possible de résoudre de la manière suivante :

1. Télécharger le jar suivant [patch-hadoop_7682-1.0.x-win/downloads](https://github.com/congainc/patch-hadoop_7682-1.0.x-win/downloads).
2. Déplacer le jar dans `TP_HMIN122M-hadoop/lib-dev`.
3. Ajouter le `.jar` au build path :
   - Dans le Package Explorer, clic droit sur le projet → "Build Path" → "Configure Build Path" → "Libraries" → "Add external jars".
   - Ajouter le `.jar` précédemment téléchargé.
4. Changer une valeur de la configuration du job avec la méthode suivante :
   ```java
   conf.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");


Où `conf` est l'objet `Configuration` envoyé au job lors de sa création.

5. Exécuter le programme et vérifier son bon fonctionnement.