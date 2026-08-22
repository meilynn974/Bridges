# ============================================================
#  LANCER LE SCRIPT PYTHON DEPUIS R
#  Place ce fichier dans le même dossier que collecte_swio_v2.py
# ============================================================

Sys.unsetenv("VIRTUAL_ENV")
library(reticulate)

# ── 1. Active le virtualenv dédié ──
use_virtualenv("swio_env", required = TRUE)

# ── 2. Installe requests si nécessaire ──
if (!py_module_available("requests")) {
  cat("Installation de 'requests'...\n")
  virtualenv_install("swio_env", "requests")
}

# ── 3. Récupère le chemin du Python du venv ──
py_exe <- py_config()$python
cat("Python utilisé :", py_exe, "\n")

# ── 4. Lance le script depuis son propre dossier ──
script_path <- normalizePath("collecte_swio_v2.py", mustWork = FALSE)
script_dir  <- dirname(script_path)

cat("Lancement de la collecte...\n")
old_dir <- setwd(script_dir)
on.exit(setwd(old_dir))

code_retour <- system2(py_exe, args = script_path)

if (code_retour == 0) {
  cat("\n✅ Collecte terminée.\n")
  cat("Fichier généré : articles_filtres_SWIO.ris\n")
  cat("→ Importe dans Zotero : Fichier → Importer → sélectionne le .ris\n")
} else {
  cat("\n❌ Erreur lors de l'exécution. Vérifie les messages ci-dessus.\n")
}
