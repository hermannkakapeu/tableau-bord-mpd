import os
import time
import shutil
import logging
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from extract import parse_sdmx_to_dataframe, sauvegarder_tableau
from config import DOSSIER_HISTORIQUE, DOSSIER_LOGS, HEADLESS, MAPPING, NOM_FICHIER_DATA









# scraper.py — modifier init_driver()
def init_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)
    
    
    
def setup_logger(nom_lien, horodatage):
    """Crée un logger dédié à chaque exécution."""
    os.makedirs(DOSSIER_LOGS, exist_ok=True)
    log_path = os.path.join(DOSSIER_LOGS, f"log_{nom_lien}_{horodatage}.log")

    logger = logging.getLogger(f"{nom_lien}_{horodatage}")
    logger.setLevel(logging.DEBUG)

    # Handler fichier
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)

    # Handler console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger, log_path


def traiter_lien(lien_config):
    """Traite un lien SDMX et sauvegarde les résultats horodatés."""
    nom        = lien_config["nom"]
    url        = lien_config["url"]
    horodatage = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logger, log_path = setup_logger(nom, horodatage)

    logger.info(f"═══════════════════════════════════════")
    logger.info(f"Démarrage traitement : {nom}")
    logger.info(f"URL : {url}")

    # ── Créer le dossier historique pour ce lien ──────────────
    dossier_lien = os.path.join(DOSSIER_HISTORIQUE, nom)
    os.makedirs(dossier_lien, exist_ok=True)

    # ── Chemin de base horodaté ───────────────────────────────
    chemin_base = os.path.join(dossier_lien, f"{nom}_{horodatage}")
    chemin_txt_raw = chemin_base + "_raw.txt"

    #driver = init_driver(headless=HEADLESS)
    driver = init_driver(headless=HEADLESS)
    wait   = WebDriverWait(driver, 15)

    try:
        driver.get(url)
        logger.info(f"Page chargée — Titre : {driver.title}")

        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        contenu_page = driver.find_element(By.TAG_NAME, "body").text

        if not contenu_page.strip():
            logger.warning("Contenu texte vide, récupération du source HTML...")
            contenu_page = driver.page_source

        # ── Sauvegarder le brut ───────────────────────────────
        with open(chemin_txt_raw, "w", encoding="utf-8") as f:
            f.write(f"URL : {driver.current_url}\n")
            f.write(f"Titre : {driver.title}\n")
            f.write("=" * 60 + "\n")
            f.write(contenu_page)

        logger.info(f"Brut sauvegardé : {chemin_txt_raw} ({len(contenu_page)} caractères)")

        # ── Parser ───────────────────────────────────────────
        df = parse_sdmx_to_dataframe(chemin_txt_raw)
        logger.info(f"Parsing OK — {len(df)} observations")

        # ── Validation des colonnes critiques ────────────────
        if df.empty:
            raise ValueError("DataFrame vide après parsing — aucune observation extraite.")
        if df['Indicateur'].isna().all():
            raise ValueError("Colonne 'Indicateur' entièrement vide — données inutilisables, lien ignoré.")
        if df['Valeur'].isna().all():
            raise ValueError("Colonne 'Valeur' entièrement vide — données inutilisables, lien ignoré.")

        logger.info(f"Période : {df['Période'].min()} → {df['Période'].max()}")
        logger.info(f"Valeur min={df['Valeur'].min():.2f} | max={df['Valeur'].max():.2f} | moy={df['Valeur'].mean():.2f}")

        # ── Sauvegarder (archive horodatée) ──────────────────────
        xlsx_path, df_traite = sauvegarder_tableau(df, chemin_base)
        nom_fichier_base = NOM_FICHIER_DATA.get(nom, f"data_{nom}")

        logger.info(f"✅ Excel (archive) : {xlsx_path}")
        logger.info(f"✅ LOG             : {log_path}")

        # ── Copier dans data/ sous nom générique (écrasé à chaque run) ──
        if "Secteur" in df_traite.columns:
            # ── PIB : deux dossiers selon le secteur ─────────────
            # Unicode escape sur "Agrégat" pour éviter tout problème
            # d'encodage de fichier source sur Windows.
            _AGREGAT = "Agr\u00e9gat"
            df_data    = df_traite[df_traite["Secteur"] != _AGREGAT]
            df_agregat = df_traite[df_traite["Secteur"] == _AGREGAT]

            dossier_data = os.path.join(dossier_lien, "data")
            os.makedirs(dossier_data, exist_ok=True)
            chemin_data_xlsx = os.path.join(dossier_data, nom_fichier_base + ".xlsx")
            df_data.to_excel(chemin_data_xlsx, index=False, sheet_name="Données")
            logger.info(f"✅ DATA            : {chemin_data_xlsx}  ({len(df_data)} lignes, hors Agr\u00e9gat)")

            dossier_agreg = os.path.join(dossier_lien, "data-agreg")
            os.makedirs(dossier_agreg, exist_ok=True)
            chemin_agreg_xlsx = os.path.join(dossier_agreg, nom_fichier_base + "-agregat.xlsx")
            df_agregat.to_excel(chemin_agreg_xlsx, index=False, sheet_name="Données")
            logger.info(f"✅ DATA-AGREG      : {chemin_agreg_xlsx}  ({len(df_agregat)} lignes, Agr\u00e9gat seul)")
        else:
            # ── Autres sources : copie directe ────────────────────
            dossier_data = os.path.join(dossier_lien, "data")
            os.makedirs(dossier_data, exist_ok=True)
            chemin_data_xlsx = os.path.join(dossier_data, nom_fichier_base + ".xlsx")
            shutil.copy2(xlsx_path, chemin_data_xlsx)
            logger.info(f"✅ DATA            : {chemin_data_xlsx}")

        logger.info(f"Traitement '{nom}' terminé avec succès.")

    except Exception as e:
        logger.error(f"❌ Erreur lors du traitement de '{nom}' : {e}")
        screenshot_path = os.path.join(dossier_lien, f"erreur_{horodatage}.png")
        driver.save_screenshot(screenshot_path)
        logger.error(f"Screenshot : {screenshot_path}")

    finally:
        driver.quit()
        logger.info("Navigateur fermé.")
        # Nettoyer les handlers pour éviter les doublons
        logger.handlers.clear()


def run_tous_les_liens(liens):
    """Lance le traitement pour tous les liens de la config."""
    print(f"\n🚀 Lancement — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   {len(liens)} lien(s) à traiter\n")

    for lien in liens:
        traiter_lien(lien)
        time.sleep(2)  # Petite pause entre chaque lien

    print(f"\n✅ Tous les liens traités — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
