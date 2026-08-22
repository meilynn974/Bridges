# SCRIPT COMPLET : DIAGRAMMES DATES + RADAR

library(readxl)
library(dplyr)
library(ggplot2)

cat("Chargement des donnees...\n")

# Charger les donnees
df <- as.data.frame(read_excel("C:/Users/meily/Documents/Base de donnée_sheets.xlsx", sheet = 1, col_types = "text"))

# Nettoyer
df[] <- lapply(df, function(x) trimws(as.character(x)))
df <- df[df$Type_Conflit_Principal != "" & df$Type_Conflit_Principal != "NA", ]

# PARTIE 1 : DIAGRAMME DATES vs NOMBRE DE CONFLITS

cat("Preparation diagramme dates...\n")

df_dates <- df
df_dates$Date_Conflit <- suppressWarnings(as.numeric(df_dates$Date_Conflit))
df_dates <- df_dates[!is.na(df_dates$Date_Conflit) & df_dates$Date_Conflit > 0, ]
df_dates <- df_dates[df_dates$Date_Conflit <= 2026, ]

# Creer les periodes groupees
df_dates$Periode <- cut(df_dates$Date_Conflit,
                        breaks = c(1969, 1980, 1990, 2000, 2010, 2015, 2020, 2026),
                        labels = c("1970-1979", "1980-1989", "1990-1999", "2000-2009", "2010-2014", "2015-2019", "2021-2026"),
                        right = FALSE)

# Compter les conflits par periode
conflits_par_periode <- df_dates %>%
  group_by(Periode) %>%
  summarise(n_conflits = n(), .groups = 'drop') %>%
  filter(!is.na(Periode))

cat("Resumes par periode :\n")
print(conflits_par_periode)

# Diagramme dates
cat("Creation diagramme dates...\n")
png("Diagramme_dates_conflits.png", width = 1000, height = 600)
ggplot(conflits_par_periode, aes(x = Periode, y = n_conflits, fill = Periode)) +
  geom_bar(stat = "identity", color = "black", size = 0.7) +
  labs(title = "Nombre de conflits de peche par periode",
       x = "Periode", y = "Nombre de conflits") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 11),
        axis.text.y = element_text(size = 11),
        text = element_text(size = 12, face = "bold"),
        legend.position = "none",
        plot.title = element_text(face = "bold", size = 14)) +
  geom_text(aes(label = n_conflits), vjust = -0.5, size = 4, fontface = "bold")
dev.off()
cat("OK : Diagramme_dates_conflits.png cree\n")

# PARTIE 2 : DIAGRAMME RADAR (CONFLITS PRINCIPAUX VS SECONDAIRES)

cat("\nPreparation diagramme radar...\n")

# Garder la premiere valeur si plusieurs conflits secondaires
df$Type_Conflit_Secondaire <- sapply(df$Type_Conflit_Secondaire, function(x) {
  if (is.na(x) || x == "") return("NS")
  trimws(strsplit(x, ",")[[1]][1])
})

# Creer une table de contingence
contingence <- table(df$Type_Conflit_Principal, df$Type_Conflit_Secondaire)

cat("Table de contingence :\n")
print(contingence)

# Preparer les donnees pour le radar
conflits_principaux <- c("Allocation_externe", "Allocation_interne", "Gestion", "Juridique")
conflits_secondaires_freq <- sort(colSums(contingence), decreasing = TRUE)[1:12]
conflits_secondaires <- names(conflits_secondaires_freq)

# Creer une sous-table
sub_contingence <- contingence[conflits_principaux, conflits_secondaires]
radar_data <- t(sub_contingence)

# Preparer pour ggplot2
radar_long <- data.frame()

for (i in 1:nrow(radar_data)) {
  for (j in 1:ncol(radar_data)) {
    radar_long <- rbind(radar_long, data.frame(
      Conflit_Secondaire = rownames(radar_data)[i],
      Conflit_Principal = colnames(radar_data)[j],
      Nombre = radar_data[i, j],
      stringsAsFactors = FALSE
    ))
  }
}

# Couleurs
couleurs <- c("#FFD60A", "#FF6FB5", "#4CAF50", "#9B5DE5")

# Creer le graphique radar avec ggplot2
cat("Creation diagramme radar...\n")
png("Radar_conflits_principaux_secondaires.png", width = 1400, height = 1000, res = 120)

ggplot(radar_long, aes(x = Conflit_Secondaire, y = Nombre, color = Conflit_Principal, group = Conflit_Principal)) +
  geom_point(size = 3) +
  geom_line(size = 1.2) +
  scale_color_manual(values = couleurs) +
  coord_polar(start = 0, direction = 1) +
  labs(title = "Profil des conflits principaux selon les conflits secondaires",
       color = "Conflit Principal",
       x = "", y = "Nombre de cas") +
  theme_minimal() +
  theme(axis.text.x = element_text(size = 10),
        axis.text.y = element_text(size = 9),
        text = element_text(size = 11, face = "bold"),
        plot.title = element_text(face = "bold", size = 14, hjust = 0.5),
        legend.position = "right",
        panel.grid.major = element_line(color = "grey80", size = 0.3))

dev.off()
cat("OK : Radar_conflits_principaux_secondaires.png cree\n")

# RESUME FINAL

cat("\n=== RESUME DES CONFLITS PRINCIPAUX ===\n")
for (cp in conflits_principaux) {
  total <- sum(contingence[cp, ])
  cat("\n", cp, ":", total, "cas\n")
  
  secondaires <- sort(contingence[cp, ], decreasing = TRUE)
  for (i in 1:min(5, length(secondaires))) {
    if (secondaires[i] > 0) {
      cat("  -", names(secondaires)[i], ":", secondaires[i], "\n")
    }
  }
}

cat("\n=== FICHIERS GENERES ===\n")
cat("OK : Diagramme_dates_conflits.png\n")
cat("OK : Radar_conflits_principaux_secondaires.png\n")
cat("\nScript termine !\n")