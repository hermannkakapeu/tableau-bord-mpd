import xml.etree.ElementTree as ET
import pandas as pd
from config import MAPPING

MOIS_FR = {
    1: "Janvier", 2: "Février",  3: "Mars",      4: "Avril",
    5: "Mai",     6: "Juin",     7: "Juillet",    8: "Août",
    9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre"
}


def transposer_indicateurs(df: pd.DataFrame, col_valeur: str = "Valeur") -> pd.DataFrame:

    df = df.copy()

    # ── 1. Parser la colonne Période
    df["Période"] = pd.to_datetime(df["Période"], dayfirst=True, errors="coerce")

    # ── 2. Renommer Période → Date
    df = df.rename(columns={"Période": "Date"})

    # ── 3. Pivot (index = Date)
    df_pivot = df.pivot_table(
        index="Date",
        columns="Indicateur",
        values=col_valeur,
        aggfunc="first"
    ).reset_index()

    df_pivot.columns.name = None

    # ── 4. Ajouter les colonnes temporelles
    df_pivot.insert(1, "Mois_n", df_pivot["Date"].dt.strftime("%m/%Y"))
    df_pivot.insert(2, "Mois",   df_pivot["Date"].dt.month.map(MOIS_FR))
    df_pivot.insert(3, "Année",  df_pivot["Date"].dt.year)

    # ── 5. Trier
    df_pivot = df_pivot.sort_values("Date").reset_index(drop=True)

    return df_pivot


# ══════════════════════════════════════════════════════════════
# DÉTECTION AUTOMATIQUE PAR MOTS-CLÉS (fallback)
# ══════════════════════════════════════════════════════════════

def detecter_secteur_type(nom: str) -> tuple:
    """Détecte (Secteur, Type PIB) — recherche exacte puis fallback mots-clés."""

    nom_strip  = nom.strip()
    nom_upper  = nom_strip.upper()

    # ── Recherche exacte (insensible à la casse) ──────────────
    for cle, valeur in MAPPING.items():
        if cle.upper() == nom_upper:
            return valeur

    # ── Fallback par mots-clés ────────────────────────────────
    mots_primaire = [
        "AGRICULT", "ELEVAGE", "PECHE", "SYLVICULT",
        "FORET", "FORÊT", "CHASSE", "EGRENAGE", "PRIMAIRE"
    ]
    mots_secondaire = [
        "INDUSTRI", "PETROLI", "MANUFACTUR", "EXTRACTI",
        "ELECTRICITE", "ENERGIE", "BTP", "BATIMENT",
        "TRAVAUX", "SECONDAIRE", "ASSAINISSEMENT"
    ]
    mots_non_marchand = [
        "ADMINISTRATION", "NON MARCHAND", "NON MARCHNAD",
        "ENSEIGNEMENT", "SANTE", "ISBLM", "APU"
    ]
    mots_tertiaire = [
        "COMMERCE", "TRANSPORT", "TELECOM", "BANQUE",
        "ASSURANCE", "IMMOBIL", "TERTIAIRE", "SERVICE",
        "FINANCIER", "INFORMATION", "COMMUNICATION", "REPARATION"
    ]
    mots_agregat = [
        "PIB", "TAXE", "IMPOT", "SUBVENTION",
        "DROIT", "COUT DES FACTEURS"
    ]

    for mot in mots_primaire:
        if mot in nom_upper:
            return ("Primaire", "Marchand")

    for mot in mots_secondaire:
        if mot in nom_upper:
            return ("Secondaire", "Marchand")

    for mot in mots_non_marchand:
        if mot in nom_upper:
            return ("Tertiaire", "Non Marchand")

    for mot in mots_tertiaire:
        if mot in nom_upper:
            return ("Tertiaire", "Marchand")

    for mot in mots_agregat:
        if mot in nom_upper:
            return ("Agrégat", "Marchand")

    return ("Autre", "Inconnu")


# ══════════════════════════════════════════════════════════════
# FONCTION PRINCIPALE
# ══════════════════════════════════════════════════════════════

def enrichir_pib(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrichit un DataFrame PIB brut avec les colonnes :
    Secteur | Nom | Type PIB | Valeur | Année | Trimestre

    Args:
        df : DataFrame avec colonnes 'Période', 'Valeur', 'Indicateur'

    Returns:
        DataFrame enrichi et restructuré
    """

    df = df.copy()

    # ── 1. Parser la Période ──────────────────────────────────
    df["Période"] = pd.to_datetime(df["Période"], errors="coerce")

    # ── 2. Extraire les colonnes temporelles ─────────────────
    df["Date"]      = df["Période"]
    df["Mois_n"]    = df["Période"].dt.strftime("%m/%Y")
    df["Mois"]      = df["Période"].dt.month.map(MOIS_FR)
    df["Année"]     = df["Période"].dt.year
    df["Trimestre"] = df["Période"].dt.quarter.map(
        {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
    )

    # ── 3. Nom = Indicateur nettoyé ───────────────────────────
    df["Nom"] = df["Indicateur"].str.strip()

    # ── 4. Détecter Secteur et Type PIB ──────────────────────
    resultats      = df["Nom"].apply(detecter_secteur_type)
    df["Secteur"]  = resultats.apply(lambda x: x[0])
    df["Type PIB"] = resultats.apply(lambda x: x[1])

    # ── 5. Restructurer ───────────────────────────────────────
    df_final = df[[
        "Secteur",
        "Nom",
        "Type PIB",
        "Valeur",
        "Date",
        "Mois_n",
        "Mois",
        "Année",
        "Trimestre"
    ]].copy()

    # ── 6. Trier ──────────────────────────────────────────────
    ordre_secteur = {
        "Primaire": 0, "Secondaire": 1,
        "Tertiaire": 2, "Agrégat": 3, "Autre": 4
    }
    df_final["_ordre"] = df_final["Secteur"].map(ordre_secteur).fillna(9)
    df_final = (
        df_final
        .sort_values(["Année", "Trimestre", "_ordre"])
        .drop(columns="_ordre")
        .reset_index(drop=True)
    )

    return df_final


def parse_sdmx_to_dataframe(fichier_xml):
    with open(fichier_xml, "r", encoding="utf-8") as f:
        contenu = f.read()

    lignes = contenu.split("\n")
    xml_lines = []
    capture = False

    for ligne in lignes:
        if ligne.strip().startswith("<StructureSpecificData"):
            capture = True
        if capture:
            xml_lines.append(ligne)

    xml_brut = "\n".join(xml_lines)
    root = ET.fromstring(xml_brut)
    dataset = root.find("DataSet")
    donnees = []

    for series in dataset.findall("Series"):
        indicateur = series.get("NOM_INDICATOR", "N/A")
        if indicateur == "N/A":
            indicateur = series.get("NOMFR_INDICATOR", "N/A")
        code_sdmx     = series.get("SDMX-CODE_INDICATOR", "N/A")
        frequence     = series.get("FREQ", "N/A")
        id_indicateur = series.get("ID_INDICATOR", "N/A")
        description   = series.get("SDMX-DESCRIPTOR_INDICATOR", "N/A")

        for obs in series.findall("Obs"):
            periode = obs.get("TIME_PERIOD")
            valeur  = obs.get("OBS_VALUE")
            donnees.append({
                "Période"       : periode,
                "Valeur"        : float(valeur) if valeur else None,
                "Indicateur"    : indicateur,
                "Code SDMX"     : code_sdmx,
                "Fréquence"     : frequence,
                "ID Indicateur" : id_indicateur,
                "Description"   : description
            })

    df = pd.DataFrame(donnees)

    try:
        df["Période"] = pd.to_datetime(df["Période"], format="%Y-%m")
    except Exception:
        df["Période"] = pd.to_datetime(df["Période"], errors="coerce")

    df["Valeur"] = pd.to_numeric(df["Valeur"], errors="coerce")
    df = df.sort_values("Période").reset_index(drop=True)
    return df


def sauvegarder_tableau(df, chemin_base):
    """
    Sauvegarde en CSV, Excel et TXT avec le chemin horodaté fourni.
    chemin_base : ex. 'historique/vigmepg/vigmepg_2024-01-15_14-30-00'
    """
    data_sans_T = ["pib-courant", ""]
    chemin_csv   = chemin_base + ".csv"
    chemin_xlsx  = chemin_base + ".xlsx"
    chemin_txt   = chemin_base + ".txt"
    print(f"Chemins de sauvegarde :\n  CSV : {chemin_csv}\n  Excel : {chemin_xlsx}\n  TXT : {chemin_txt}")
    
    if 'pib' not in chemin_xlsx :
        print("Transposition des indicateurs...")
        df = transposer_indicateurs(df)
        print("Transposition terminée.")
        df.to_csv(chemin_csv, index=False, encoding="utf-8-sig", sep=";")
        df.to_excel(chemin_xlsx, index=False, sheet_name="Données")
        df.to_string(open(chemin_txt, "w", encoding="utf-8"))
    else:
        print("Pas de transposition pour les données de PIB courant.")
        df = enrichir_pib(df)
        print("Enrichissement du PIB terminé.")
        df.to_csv(chemin_csv, index=False, encoding="utf-8-sig", sep=";")
        df.to_excel(chemin_xlsx, index=False, sheet_name="Données")
        df.to_string(open(chemin_txt, "w", encoding="utf-8"))


    return chemin_csv, chemin_xlsx, chemin_txt


    