import re
import xml.etree.ElementTree as ET
import pandas as pd
from config import MAPPING

MOIS_FR = {
    1: "Janvier", 2: "Février",  3: "Mars",      4: "Avril",
    5: "Mai",     6: "Juin",     7: "Juillet",    8: "Août",
    9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre"
}

# ── Attributs à exclure du suffixe composite ──────────────────────────────────
# Ces NOM_* sont soit le nom de l'indicateur (déjà capturé), soit de la
# métadonnée de base de référence (inutile pour distinguer les séries).
_EXCLUS_DIMS = {
    "NOM_INDICATOR",
    "NOMFR_INDICATOR",
    "LIBELLE_FRANCAIS_INDICATOR",
    "NOM_BASE_PER",
}


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS INTERNES
# ══════════════════════════════════════════════════════════════════════════════

def _nom_indicateur(attrs: dict) -> str:
    """
    Retourne le nom de l'indicateur en testant plusieurs variantes d'attributs
    (selon la source Knoema/ODS, l'attribut diffère) :
      - NOM_INDICATOR       (icac, icai, ihpc, expimp, population, txpauvrete, pib-courant)
      - NOMFR_INDICATOR     (ihpi)
      - LIBELLE_FRANCAIS_INDICATOR  (ippi)
    Fallback sur ID_INDICATOR si aucun nom textuel n'est trouvé.
    """
    return (
        attrs.get("NOM_INDICATOR") or
        attrs.get("NOMFR_INDICATOR") or
        attrs.get("LIBELLE_FRANCAIS_INDICATOR") or
        attrs.get("ID_INDICATOR") or
        "N/A"
    )


def _dims_serie(attrs: dict) -> list:
    """
    Retourne la liste des valeurs de dimensions supplémentaires présentes sur
    la balise <Series>, qui permettent de distinguer des séries ayant le même
    nom d'indicateur de base.

    Exemples :
      population  → ["DISTRICT AUTONOME D'ABIDJAN", "Masculin"]
      pib-courant → ["Valeur Ajoutée à prix courant", "PIB Approche Production"]
      ihpc, icac  → []   (aucune dimension supplémentaire)
    """
    return [
        v for k, v in attrs.items()
        if k.startswith("NOM_") and k not in _EXCLUS_DIMS and v
    ]


def _parser_periode(p: str) -> pd.Timestamp:
    """
    Convertit un TIME_PERIOD SDMX en Timestamp pandas.
    Formats gérés :
      - Mensuel   : "2023-01"
      - Trimestriel : "2015-Q1"
      - Annuel    : "1985"
    """
    if not isinstance(p, str) or not p:
        return pd.NaT
    # Trimestriel : "2015-Q1"
    m = re.match(r'^(\d{4})-Q(\d)$', p)
    if m:
        year, q = int(m.group(1)), int(m.group(2))
        return pd.Timestamp(year, (q - 1) * 3 + 1, 1)
    # Annuel : "1985" ou "2024"
    if re.match(r'^\d{4}$', p):
        return pd.Timestamp(int(p), 1, 1)
    # Mensuel : "2023-01"
    try:
        return pd.to_datetime(p, format="%Y-%m")
    except Exception:
        return pd.to_datetime(p, errors="coerce")


# ══════════════════════════════════════════════════════════════════════════════
# FONCTION PRINCIPALE DE PARSING
# ══════════════════════════════════════════════════════════════════════════════

def parse_sdmx_to_dataframe(fichier_xml: str) -> pd.DataFrame:
    """
    Lit un fichier SDMX StructureSpecificData (Knoema/ODS) et retourne
    un DataFrame avec les colonnes :

        Période | Valeur | Indicateur | Code SDMX | Fréquence | ID Indicateur | Description

    Corrections apportées par rapport à la version initiale :
    ─────────────────────────────────────────────────────────
    1. Nom d'indicateur : cascade correcte sur NOM_INDICATOR → NOMFR_INDICATOR
       → LIBELLE_FRANCAIS_INDICATOR (l'ancien elif était mort → ippi retournait "N/A").
    2. Parsing de date multi-format : mensuel ("2023-01"), trimestriel ("2015-Q1"),
       annuel ("1985") — le format forcé "%Y-%m" cassait les données pib-courant
       et txpauvrete/population.
    3. Séries multi-dimensions : les attributs NOM_REGION, NOM_SEXE (population),
       NOM_TYPE, NOM_NATURE (pib-courant) sont concaténés dans le nom d'indicateur
       (séparateur " — ") afin que chaque série reste unique après transposition.
    """
    with open(fichier_xml, "r", encoding="utf-8") as f:
        contenu = f.read()

    # ── Isoler le XML SDMX (ignorer l'entête robot : URL, Titre, ===) ────────
    lignes    = contenu.split("\n")
    xml_lines = []
    capture   = False
    for ligne in lignes:
        if ligne.strip().startswith("<StructureSpecificData"):
            capture = True
        if capture:
            xml_lines.append(ligne)

    xml_brut = "\n".join(xml_lines)
    root     = ET.fromstring(xml_brut)
    dataset  = root.find("DataSet")
    donnees  = []

    for series in dataset.findall("Series"):
        attrs = series.attrib

        # ── Nom de base ────────────────────────────────────────────────
        nom_base = _nom_indicateur(attrs)

        # ── Dimensions complémentaires → indicateur composite ──────────
        dims       = _dims_serie(attrs)
        indicateur = " — ".join([nom_base] + dims) if dims else nom_base

        code_sdmx     = attrs.get("SDMX-CODE_INDICATOR", "")
        frequence     = attrs.get("FREQ", "N/A")
        id_indicateur = attrs.get("ID_INDICATOR", "N/A")
        description   = attrs.get("SDMX-DESCRIPTOR_INDICATOR", "")

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
                "Description"   : description,
            })

    df = pd.DataFrame(donnees)
    if df.empty:
        return df

    df["Période"] = df["Période"].apply(_parser_periode)
    df["Valeur"]  = pd.to_numeric(df["Valeur"], errors="coerce")
    df = df.sort_values("Période").reset_index(drop=True)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# TRANSPOSITION (données mensuelles / annuelles hors PIB)
# ══════════════════════════════════════════════════════════════════════════════

def transposer_indicateurs(df: pd.DataFrame, col_valeur: str = "Valeur") -> pd.DataFrame:

    df = df.copy()

    # ── 1. S'assurer que Période est bien un Timestamp ─────────────
    df["Période"] = pd.to_datetime(df["Période"], errors="coerce")

    # ── 2. Renommer Période → Date ─────────────────────────────────
    df = df.rename(columns={"Période": "Date"})

    # ── 3. Pivot (index = Date, colonnes = Indicateur) ─────────────
    df_pivot = df.pivot_table(
        index="Date",
        columns="Indicateur",
        values=col_valeur,
        aggfunc="first"
    ).reset_index()

    df_pivot.columns.name = None

    # ── 4. Colonnes temporelles ────────────────────────────────────
    df_pivot.insert(1, "Mois_n", df_pivot["Date"].dt.strftime("%m/%Y"))
    df_pivot.insert(2, "Mois",   df_pivot["Date"].dt.month.map(MOIS_FR))
    df_pivot.insert(3, "Année",  df_pivot["Date"].dt.year)

    # ── 5. Trier ───────────────────────────────────────────────────
    df_pivot = df_pivot.sort_values("Date").reset_index(drop=True)

    return df_pivot


# ══════════════════════════════════════════════════════════════════════════════
# DÉTECTION AUTOMATIQUE PAR MOTS-CLÉS (fallback MAPPING)
# ══════════════════════════════════════════════════════════════════════════════

def detecter_secteur_type(nom: str) -> tuple:
    """
    Détecte (Secteur, Type PIB) à partir du nom d'indicateur.
    Recherche exacte dans MAPPING d'abord, puis fallback mots-clés.
    Le nom passé doit être le NOM DE BASE (avant le premier " — ").
    """
    nom_strip = nom.strip()
    nom_upper = nom_strip.upper()

    # ── Recherche exacte (insensible à la casse) ───────────────────
    for cle, valeur in MAPPING.items():
        if cle.upper() == nom_upper:
            return valeur

    # ── Fallback par mots-clés ─────────────────────────────────────
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


# ══════════════════════════════════════════════════════════════════════════════
# ENRICHISSEMENT PIB
# ══════════════════════════════════════════════════════════════════════════════

def enrichir_pib(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrichit un DataFrame PIB brut avec les colonnes :
        Secteur | Nom | Type PIB | Type Valeur | Valeur | Date | Mois_n | Mois | Année | Trimestre

    Args:
        df : DataFrame issu de parse_sdmx_to_dataframe
             (colonnes : Période, Valeur, Indicateur, …)

    Returns:
        DataFrame enrichi et restructuré.

    Note : pour les séries composites (ex. "PRIMAIRE — Valeur Ajoutée à prix
    courant — PIB Approche Production"), le MAPPING est appliqué sur le nom
    de BASE uniquement (partie avant le premier " — ") afin que la détection
    de secteur reste correcte.
    """
    df = df.copy()

    # ── 1. Parser la Période ───────────────────────────────────────
    df["Période"] = pd.to_datetime(df["Période"], errors="coerce")

    # ── 2. Colonnes temporelles ────────────────────────────────────
    df["Date"]      = df["Période"]
    df["Mois_n"]    = df["Période"].dt.strftime("%m/%Y")
    df["Mois"]      = df["Période"].dt.month.map(MOIS_FR)
    df["Année"]     = df["Période"].dt.year
    df["Trimestre"] = df["Période"].dt.quarter.map(
        {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
    )

    # ── 3. Nom de base (avant le premier " — ") ────────────────────
    # Nécessaire pour que l'exact-match du MAPPING fonctionne même
    # lorsque l'indicateur est composite (pib-courant avec NOM_TYPE/NOM_NATURE).
    parties       = df["Indicateur"].str.split(" — ")
    df["Nom"]     = parties.str[0].str.strip()
    # "Type Valeur" = les dimensions complémentaires (courant vs volume, etc.)
    df["Type Valeur"] = parties.apply(
        lambda p: " — ".join(p[1:]).strip() if len(p) > 1 else ""
    )

    # ── 4. Détecter Secteur et Type PIB via le nom de base ─────────
    resultats      = df["Nom"].apply(detecter_secteur_type)
    df["Secteur"]  = resultats.apply(lambda x: x[0])
    df["Type PIB"] = resultats.apply(lambda x: x[1])

    # ── 5. Restructurer ───────────────────────────────────────────
    df_final = df[[
        "Secteur", "Nom", "Type PIB", "Type Valeur",
        "Valeur", "Date", "Mois_n", "Mois", "Année", "Trimestre"
    ]].copy()

    # ── 6. Trier ───────────────────────────────────────────────────
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


# ══════════════════════════════════════════════════════════════════════════════
# RESTRUCTURATION POPULATION
# ══════════════════════════════════════════════════════════════════════════════

def restructurer_population(df: pd.DataFrame) -> pd.DataFrame:
    """
    Restructure les données population en tableau par région :

        Année | Région | Féminin | Homme | Total

    Attend un DataFrame issu de parse_sdmx_to_dataframe dont les indicateurs
    sont composites : "Effectif de Population — {Région} — {Sexe}".
    La colonne "Sexe" est pivotée en 3 colonnes : Féminin, Homme, Total.
    """
    df = df.copy()

    # ── Éclater l'indicateur composite ────────────────────────────
    # Format : "Effectif de Population — {NOM_REGION} — {NOM_SEXE}"
    parties      = df["Indicateur"].str.split(" — ", expand=True)
    df["Région"] = parties[1].str.strip()
    df["Sexe"]   = parties[2].str.strip()

    # ── Extraire l'année ───────────────────────────────────────────
    df["Année"] = df["Période"].dt.year

    # ── Pivot : une colonne par valeur de Sexe ─────────────────────
    df_pivot = df.pivot_table(
        index=["Année", "Région"],
        columns="Sexe",
        values="Valeur",
        aggfunc="first"
    ).reset_index()
    df_pivot.columns.name = None

    # ── Renommer "Masculin" → "Homme" ──────────────────────────────
    df_pivot = df_pivot.rename(columns={"Masculin": "Homme"})

    # ── Ordonner les colonnes ──────────────────────────────────────
    cols_finales = ["Année", "Région"]
    for col in ["Féminin", "Homme", "Total"]:
        if col in df_pivot.columns:
            cols_finales.append(col)

    df_final = (
        df_pivot[cols_finales]
        .sort_values(["Année", "Région"])
        .reset_index(drop=True)
    )
    return df_final


# ══════════════════════════════════════════════════════════════════════════════
# SAUVEGARDE
# ══════════════════════════════════════════════════════════════════════════════

def sauvegarder_tableau(df, chemin_base):
    """
    Sauvegarde en CSV, Excel et TXT avec le chemin horodaté fourni.
    chemin_base : ex. 'historique/pib-courant/pib-courant_2024-01-15_14-30-00'

    Routing selon la source :
      - 'pib'        → enrichir_pib()
      - 'population' → restructurer_population()
      - autres       → transposer_indicateurs()
    """
    chemin_csv  = chemin_base + ".csv"
    chemin_xlsx = chemin_base + ".xlsx"
    chemin_txt  = chemin_base + ".txt"
    print(f"Chemins de sauvegarde :\n  CSV : {chemin_csv}\n  Excel : {chemin_xlsx}\n  TXT : {chemin_txt}")

    if 'pib' in chemin_xlsx:
        print("Enrichissement PIB...")
        df = enrichir_pib(df)
        print("Enrichissement terminé.")
    elif 'population' in chemin_xlsx:
        print("Restructuration population (Année | Région | Féminin | Homme | Total)...")
        df = restructurer_population(df)
        print("Restructuration terminée.")
    else:
        print("Transposition des indicateurs...")
        df = transposer_indicateurs(df)
        print("Transposition terminée.")

    df.to_csv(chemin_csv, index=False, encoding="utf-8-sig", sep=";")
    df.to_excel(chemin_xlsx, index=False, sheet_name="Données")
    df.to_string(open(chemin_txt, "w", encoding="utf-8"))

    return chemin_csv, chemin_xlsx, chemin_txt
