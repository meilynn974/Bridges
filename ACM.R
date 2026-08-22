# ============================================================
# ANALYSE DES CORRESPONDANCES MULTIPLES - CONFLITS DE PÊCHE
# VERSION CORRIGÉE
# ============================================================

# 1. CHARGER LES LIBRAIRIES
library(readxl)
library(dplyr)
library(FactoMineR)
library(factoextra)
library(gridExtra)
library(ggcorrplot)
library(ggplot2)

# 2. CHARGER LES DONNÉES
# Adapter le chemin selon votre système
df <- as.data.frame(read_excel("Base_de_donnée_sheets.xlsx", sheet = 1, col_types = "text"))

# Nettoyer les noms de colonnes
names(df) <- trimws(names(df))
df[] <- lapply(df, function(x) trimws(as.character(x)))

# Supprimer les lignes sans conflit principal
df <- df[df$Type_Conflit_Principal != "" & df$Type_Conflit_Principal != "NA", ]

# 3. VARIABLES À ANALYSER
variables_actives <- c("Echelle_Conflit", "Zones_Maritimes", "Type_Conflit_Principal",
                       "Type_Conflit_Secondaire", "Type_acteurs_A", "Type_acteurs_B",
                       "Type_litige_juridique", "Intensité_conflit", "Statut_conflit", "Mode_résolution")

# Variables avec plusieurs valeurs séparées par des virgules
multi_valuees <- c("Zones_Maritimes", "Type_Conflit_Secondaire", "Type_acteurs_B", "Mode_résolution")

# 4. NETTOYER LES VARIABLES MULTI-VALUÉES
# Garder seulement la première valeur si plusieurs valeurs séparées par des virgules
for (v in multi_valuees) {
  df[[v]] <- sapply(df[[v]], function(x) {
    if (is.na(x) || x == "") return("NS")
    trimws(strsplit(as.character(x), ",")[[1]][1])
  })
}

# Remplacer les valeurs manquantes par "NS"
for (v in variables_actives) {
  df[[v]] <- as.character(df[[v]])
  df[[v]][df[[v]] == "" | df[[v]] == "NA" | is.na(df[[v]])] <- "NS"
}

# 5. PRÉPARER LES DONNÉES POUR L'ANALYSE
grid_var <- df[, variables_actives]
grid_var[] <- lapply(grid_var, as.factor)

cat("Données prêtes pour l'analyse :\n")
cat("Nombre de lignes :", nrow(grid_var), "\n")
cat("Nombre de colonnes :", ncol(grid_var), "\n\n")

# ============================================================
# ACM - ANALYSE DES CORRESPONDANCES MULTIPLES
# ============================================================

# 6. CALCULER L'ACM
res.mca <- MCA(grid_var, ncp = 5, graph = FALSE)

cat("ACM calculée avec succès !\n")
cat("Variance expliquée par les 2 premiers axes :", 
    round(res.mca$eig[1,2] + res.mca$eig[2,2], 2), "%\n\n")

# 7. GRAPHIQUE DES VARIABLES
png("ACM_variables.png", width = 1200, height = 800)
fviz_mca_var(res.mca,
             col.var = "contrib",
             gradient.cols = c("#2E9FDF", "#FFD60A", "#FF6FB5"),
             repel = TRUE,
             labelsize = 3,
             axes = c(1, 2)) +
  theme(text = element_text(size = 10))
dev.off()
cat("✓ Graphique des variables sauvegardé : ACM_variables.png\n")

# 8. GRAPHIQUE DES INDIVIDUS
png("ACM_individus.png", width = 1200, height = 800)
fviz_mca_ind(res.mca,
             col.ind = "cos2",
             geom = "point",
             gradient.cols = c("#2E9FDF", "#FFD60A", "#FF6FB5"),
             repel = TRUE,
             axes = c(1, 2)) +
  theme(text = element_text(size = 8))
dev.off()
cat("✓ Graphique des individus sauvegardé : ACM_individus.png\n")

# 9. VARIANCE EXPLIQUÉE (SCREEPLOT)
png("ACM_screeplot.png", width = 1000, height = 600)
fviz_screeplot(res.mca, addlabels = TRUE, ylim = c(0, 50)) +
  theme(text = element_text(size = 10))
dev.off()
cat("✓ Screeplot sauvegardé : ACM_screeplot.png\n")

# 10. CONTRIBUTIONS AUX AXES
png("ACM_contributions.png", width = 1200, height = 1000)
contrib_axe1 <- fviz_contrib(res.mca, choice = "var", axes = 1, top = 15) +
  labs(title = "Contributions à l'Axe 1") +
  theme(text = element_text(size = 10), axis.text.x = element_text(angle = 45, hjust = 1))

contrib_axe2 <- fviz_contrib(res.mca, choice = "var", axes = 2, top = 15) +
  labs(title = "Contributions à l'Axe 2") +
  theme(text = element_text(size = 10), axis.text.x = element_text(angle = 45, hjust = 1))

grid.arrange(contrib_axe1, contrib_axe2, ncol = 1)
dev.off()
cat("✓ Contributions sauvegardées : ACM_contributions.png\n")

# ============================================================
# CLUSTERING K-MEANS
# ============================================================

# 11. K-MEANS
kcluster <- scale(res.mca$ind$coord)
k_optimal <- 6
set.seed(123)
res.km <- kmeans(kcluster, centers = k_optimal, nstart = 25)

# Ajouter les clusters aux données
df$cluster <- as.factor(res.km$cluster)

# Graphique des clusters
png("Clusters_KMeans.png", width = 1200, height = 800)
fviz_cluster(res.km, data = kcluster,
             ellipse.type = "convex",
             geom = "point",
             palette = c("#2E9FDF", "#FFD60A", "#FF6FB5", "#FF8C42", "#4CAF50", "#9B5DE5"),
             repel = TRUE,
             axes = c(1, 2)) +
  labs(title = "Classification des conflits de pêche (K-means, k=6)") +
  theme(text = element_text(size = 10))
dev.off()
cat("✓ Clusters sauvegardés : Clusters_KMeans.png\n")

# Résumé des clusters
cat("\n=== RÉSUMÉ DES CLUSTERS ===\n")
for (i in 1:k_optimal) {
  count <- sum(df$cluster == i)
  cat("Cluster", i, ":", count, "cas\n")
}

# ============================================================
# DIAGRAMMES TEMPORELS ET SPATIAUX
# ============================================================

# 12. PRÉPARER LES DONNÉES TEMPORELLES
df$Date_num <- suppressWarnings(as.numeric(df$Date_Conflit))
df$Periode <- cut(df$Date_num,
                  breaks = c(1969, 1990, 2000, 2010, 2020, 2026),
                  labels = c("Avant 1990", "1990-1999", "2000-2009", "2010-2019", "2020-2026"))

# 13. TIMELINE PAR ZONE MARITIME
timeline_zone <- df %>%
  group_by(Periode, Zones_Maritimes) %>%
  summarise(n_conflits = n(), .groups = 'drop') %>%
  filter(Zones_Maritimes != "NS" & !is.na(Periode))

png("Timeline_zones_maritimes.png", width = 1200, height = 700)
ggplot(timeline_zone, aes(x = Periode, y = n_conflits, fill = Zones_Maritimes)) +
  geom_bar(stat = "identity", position = "stack") +
  labs(title = "Distribution temporelle des conflits par zone maritime",
       x = "Période", y = "Nombre de conflits", fill = "Zone maritime") +
  scale_fill_manual(values = c("#2E9FDF", "#FFD60A", "#FF6FB5", "#FF8C42", "#4CAF50")) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        text = element_text(size = 11),
        plot.title = element_text(face = "bold"))
dev.off()
cat("✓ Timeline zones maritimes sauvegardée : Timeline_zones_maritimes.png\n")

# 14. TIMELINE PAR PAYS (TOP 8)
df$Pays_opposant[df$Pays_opposant == ""] <- "NS"

timeline_pays <- df %>%
  group_by(Periode, Pays_opposant) %>%
  summarise(n_conflits = n(), .groups = 'drop') %>%
  filter(Pays_opposant != "NS" & !is.na(Periode)) %>%
  arrange(desc(n_conflits))

# Identifier les top 8 pays
top_pays <- df %>%
  filter(Pays_opposant != "NS") %>%
  group_by(Pays_opposant) %>%
  summarise(total = n(), .groups = 'drop') %>%
  arrange(desc(total)) %>%
  head(8) %>%
  pull(Pays_opposant)

timeline_pays_top <- timeline_pays %>%
  filter(Pays_opposant %in% top_pays)

png("Timeline_pays.png", width = 1300, height = 750)
ggplot(timeline_pays_top, aes(x = Periode, y = n_conflits, color = Pays_opposant, group = Pays_opposant)) +
  geom_line(size = 1.2) +
  geom_point(size = 3) +
  labs(title = "Évolution temporelle des conflits : Top 8 pays impliqués",
       x = "Période", y = "Nombre de conflits", color = "Pays") +
  scale_color_manual(values = c("#2E9FDF", "#FFD60A", "#FF6FB5", "#FF8C42", "#4CAF50", "#9B5DE5", "#FFA500", "#00CED1")) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        text = element_text(size = 11),
        plot.title = element_text(face = "bold"),
        legend.position = "right")
dev.off()
cat("✓ Timeline pays sauvegardée : Timeline_pays.png\n")

# 15. TIMELINE PAR TYPE DE CONFLIT PRINCIPAL
timeline_conflit <- df %>%
  group_by(Periode, Type_Conflit_Principal) %>%
  summarise(n_conflits = n(), .groups = 'drop') %>%
  filter(Type_Conflit_Principal != "NS" & !is.na(Periode))

png("Timeline_type_conflits.png", width = 1200, height = 700)
ggplot(timeline_conflit, aes(x = Periode, y = n_conflits, fill = Type_Conflit_Principal)) +
  geom_bar(stat = "identity", position = "stack") +
  labs(title = "Distribution temporelle par type de conflit principal",
       x = "Période", y = "Nombre de conflits", fill = "Type de conflit") +
  scale_fill_manual(values = c("#2E9FDF", "#FFD60A", "#FF6FB5", "#FF8C42")) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        text = element_text(size = 11),
        plot.title = element_text(face = "bold"))
dev.off()
cat("✓ Timeline types de conflits sauvegardée : Timeline_type_conflits.png\n")

cat("\n=== TOUS LES GRAPHIQUES ONT ÉTÉ GÉNÉRÉS ===\n")
cat("Fichiers PNG créés :\n")
cat("  - ACM_variables.png\n")
cat("  - ACM_individus.png\n")
cat("  - ACM_screeplot.png\n")
cat("  - ACM_contributions.png\n")
cat("  - Clusters_KMeans.png\n")
cat("  - Timeline_zones_maritimes.png\n")
cat("  - Timeline_pays.png\n")
cat("  - Timeline_type_conflits.png\n")