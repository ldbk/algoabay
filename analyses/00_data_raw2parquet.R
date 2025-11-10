# raw AIS data to parquet

library(arrow) #deal with parquet
library(here)  # for path

#set the root of the project directory
here::i_am("/Users/moi/ifremer/projet/algaabay")

# 1. convert csv to zip file
# list zip file if anyrecent
csvfile<-dir(path="../data/AIS_03092024/",patt=".csv",full=T,rec=T)
	fctzip<-function(uu){
		zip(sub(".csv",".zip",uu),uu)
		unlink(uu)
	}
	# fctzip(alltmpfile[1])
	mclapply(csvfile,fctzip)

# pour 2024
# Définir le chemin du dossier où se trouvent les fichiers xlsx
dossier <- "Q:/data/obsmer/FREE2_OBSMER_serie"

# Lister tous les fichiers csv dans le dossier
fichiers_csv <- list.files(path = dossier, pattern = "\\.csv$", full.names = TRUE)

# Boucle pour lire chaque fichier csv et l'enregistrer en fichier parquet
for (fichier in fichiers_csv) {
  # Lire le fichier csv
  data <- read_delim(fichier,
                     delim = ";", escape_double = FALSE, locale = locale(encoding = "WINDOWS-1252"),
                     trim_ws = TRUE
  )  
  
  # Créer le nom du fichier parquet en ajoutant "_parquet"
  nom_fichier_parquet <- sub("\\.csv$", ".parquet", fichier)  
  # Sauvegarder le fichier en format parquet
  write_parquet(data, nom_fichier_parquet)
  
  cat("Fichier converti :", nom_fichier_parquet, "\n")
  
}
