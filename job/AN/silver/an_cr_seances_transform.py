# Generated from the matching src notebook by job_deploy.sh.
# Edit the notebook in src, then regenerate this production script.
# Source: src/AN/silver/an_cr_seances_transform.ipynb

# %% [markdown]
# # Silver — transformation des comptes rendus de séance
#
# Cette étape contient une copie embarquée du code de `bronze_silver_RAG.py` : elle ne dépend pas de ce fichier ignoré par Git et ne le modifie pas. Les fonctions de parsing, les tables et le CLI sont copiés dans les cellules suivantes.
#
# Exécuter d’abord [la récupération Bronze](../bronze/an_cr_seances_fetch.ipynb). `OUTPUT_FORMAT` vaut `json` par défaut, comme le CLI source ; le régler à `tables` pour sélectionner son autre format de sortie.

# %% [markdown]
# ## Fonctions de parsing et classifications

# %%
"""Comptes rendus de séance XML (open data AN, `CRSANR...xml`) -> JSON « silver ».

Le JSON garde TOUT le contenu de la séance, typé, pour que chaque usage filtre ce dont il a besoin
(le RAG ne garde que les discours, les statistiques comptent aussi les interruptions et les votes) :

- `interventions` : une ligne par prise de parole, dans l'ordre de la séance, typée
    « discours »     : un orateur qui a la parole. Ses paragraphes consécutifs sont FUSIONNÉS même s'il a
                       été interrompu (« Très bien ! », « C'est faux ! », « Veuillez conclure »…) : le
                       discours reste d'un seul tenant et liste ses interruptions (`interruptions`).
    « interruption » : exclamation depuis les bancs (code_grammaire INTERRUPTION_1_10), rattachée au
                       discours interrompu (`interrompt`).
    « presidence »   : propos du président de séance (donner la parole, appeler un vote, rappeler à l'ordre…).
    « didascalie »   : texte du compte rendu sans orateur (« (Il est procédé au scrutin.) »…).
  Les réactions écrites en italique entre parenthèses (« (Applaudissements sur les bancs du groupe RN.) »)
  sont retirées du texte et rangées dans `reactions`.
- `votes`  : issue de chaque vote (amendement, article…) et, pour un scrutin public, le décompte.
- `points` : la structure de l'ordre du jour (titres de niveau 1, 2…).
- `elections` : élections et nominations annoncées par la présidence (tour, voix par candidat, élus).

Usage :
    uv run rag-silver docs/                    # un JSON par séance dans silver/
    python rag_an/silver.py docs/ --out silver # sans installer le projet : ce fichier n'utilise que la
                                               # bibliothèque standard de Python (3.10 ou plus)
    python rag_an/silver.py docs/ --format tables --out tables
        # pour charger dans des tables : seances.jsonl, interventions.jsonl, votes.jsonl, points.jsonl,
        # elections.jsonl (une ligne JSON = une ligne de table, colonnes plates). Les CREATE TABLE proposés :
        # python -c "import silver; print(silver.schema_sql())" > schema.sql
        # Les interventions gardent tout ce qu'il faut pour découper ensuite en chunks.

Les comptes rendus provisoires sont ignorés. Un fichier en erreur est signalé sans arrêter les autres, et
le code de sortie vaut alors 1.
"""
from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

NS = "{http://schemas.assemblee-nationale.fr/referentiel}"
SCHEMA = "an_cr_seance/1"

# Une interruption du président entre deux paragraphes d'un même orateur (« Veuillez conclure. »,
# « Monsieur X, s'il vous plaît ! ») ne coupe pas le discours si elle est plus courte que ça (caractères).
PRESIDENCE_COURTE = 200

_SPACES = re.compile(r"[ \t\xa0  ]+")
_ITAL_OPEN, _ITAL_CLOSE, _BR = "\x01", "\x02", "\x03"


def _tag(el: ET.Element) -> str:
    return el.tag.replace(NS, "")


def _raw(el: ET.Element | None) -> str:
    """Texte d'un nœud : <br/> -> saut de ligne, italique encadré de marqueurs (pour repérer les réactions)."""
    if el is None:
        return ""
    parts = [el.text or ""]
    for child in el:
        tag = _tag(child)
        if tag == "br":
            parts.append(_BR)
        elif tag == "italique":
            parts.append(_ITAL_OPEN + _raw(child) + _ITAL_CLOSE)
        else:
            parts.append(_raw(child))
        parts.append(child.tail or "")
    return "".join(parts)


def _clean(s: str) -> str:
    """Espaces normalisés ; seuls les <br/> deviennent des sauts de ligne (ceux du fichier XML sont de la mise en
    page)."""
    s = s.replace("\n", " ").replace(_BR, "\n")
    lines = (_SPACES.sub(" ", line).strip() for line in s.split("\n"))
    return "\n".join(line for line in lines if line)


def split_reactions(raw: str) -> tuple[str, list[str]]:
    """Sépare le discours des réactions de l'hémicycle (italique entre parenthèses)."""
    # « (Applaudissements » + « sur » + « les bancs… » sont souvent des italiques séparés : on les recolle
    raw = re.sub(_ITAL_CLOSE + r"(\s*)" + _ITAL_OPEN, r"\1", raw)
    reactions: list[str] = []

    def take(m: re.Match) -> str:
        inner = _clean(m.group(1))
        # « (Applaudissements.) », et dans une liste de résultats « (Plusieurs députés se lèvent…) ; »
        if r := re.fullmatch(r"\((.+)\)\s*([.;,]?)", inner, flags=re.S):
            reactions.append(r.group(1).strip(" .").strip())
            return {";": " ; ", ",": "\x04, "}.get(r.group(2), " ")  # \x04 : l'espace avant la virgule saute
        return m.group(1)  # italique ordinaire (titre d'ouvrage, locution latine…) : gardé dans le texte

    text = re.sub(_ITAL_OPEN + r"(.*?)" + _ITAL_CLOSE, take, raw, flags=re.S)
    text = text.replace(_ITAL_OPEN, "").replace(_ITAL_CLOSE, "")
    text = re.sub(r"\s*\x04", "", text)
    return _clean(text), reactions


def _num(s: str | None) -> int | None:
    return int(s) if s and s.lstrip("-").isdigit() else None


# ---------------------------------------------------------------------------
# Votes
# ---------------------------------------------------------------------------
_SCRUTIN = re.compile(
    r"Nombre de votants (\d+) Nombre de suffrages exprimés (\d+) Majorité absolue (\d+) "
    r"Pour l.adoption (\d+) Contre (\d+)"
)
_OBJETS = (
    ("sous_amendement", r"sous-amendements?"),
    ("amendement", r"amendements?"),
    ("article", r"articles?"),
    ("motion", r"motions?"),
    ("texte", r"projet de loi|proposition de loi|proposition de résolution|ensemble"),
)


def parse_scrutin(text: str) -> dict | None:
    m = _SCRUTIN.search(re.sub(r"\s+", " ", text))
    if not m:
        return None
    votants, exprimes, majorite, pour, contre = map(int, m.groups())
    return {"votants": votants, "exprimes": exprimes, "majorite_absolue": majorite, "pour": pour, "contre": contre}


# Votes dont le résultat n'est annoncé QUE par la présidence (pas de didascalie « (… est adopté.) ») :
# - motion de censure : seuls les « pour » sont comptés, face à la majorité des membres de l'Assemblée ;
# - déclaration de politique générale / engagement de responsabilité (art. 49 al. 1) : « Pour l'approbation ».
_CENSURE = re.compile(r"Majorité requise pour l.adoption de la motion de censure.*? (\d+) Pour l.adoption (\d+)")
_APPROBATION = re.compile(
    r"Nombre de votants (\d+) Nombre de suffrages exprimés (\d+) Majorité absolue des suffrages exprimés (\d+) "
    r"Pour l.approbation (\d+) Contre (\d+)"
)


def parse_vote_presidence(text: str) -> dict | None:
    """Résultat annoncé par la présidence (censure, déclaration de politique générale) -> vote complet."""
    t = re.sub(r"\s+", " ", text)
    if m := _CENSURE.search(t):
        majorite, pour = int(m[1]), int(m[2])
        return {"objet": "motion de censure", "numeros": [], "resultat": "adopté" if pour >= majorite else "rejeté",
                "scrutin": {"votants": None, "exprimes": None, "majorite_absolue": majorite, "pour": pour,
                            "contre": None}}
    if m := _APPROBATION.search(t):
        votants, exprimes, majorite, pour, contre = map(int, m.groups())
        return {"objet": "declaration du gouvernement", "numeros": [],
                "resultat": "adopté" if pour >= majorite else "rejeté",
                "scrutin": {"votants": votants, "exprimes": exprimes, "majorite_absolue": majorite, "pour": pour,
                            "contre": contre}}
    return None


def parse_decision(text: str) -> dict | None:
    """« (L'amendement no 21 n'est pas adopté.) » -> {objet, numeros, resultat}."""
    t = _SPACES.sub(" ", text.split("\n")[0]).strip("() .")  # 1re ligne : la suite = réactions
    if re.search(r"(n.est|ne sont) pas adopté", t):
        resultat = "rejeté"
    elif re.search(r"(est|sont) retiré", t):
        resultat = "retiré"
    elif re.search(r"(est|sont) adopté", t):
        resultat = "adopté"
    elif re.search(r"tombe", t):
        resultat = "tombé"
    else:
        return None
    head = re.split(r"[;,] en conséquence| est | sont | n.est | ne sont ", t, maxsplit=1)[0]
    objet = next((o for o, pat in _OBJETS if re.search(r"\b" + pat + r"\b", head, flags=re.I)), "autre")
    numeros = re.findall(r"\b(\d+(?: ?(?:rectifié|bis|ter|quater|quinquies|[A-Z])\b)?)", head)
    if objet == "article":
        m = re.search(r"articles? ((?:\d+|premier|liminaire)(?:er)?(?: (?:bis|ter|quater|[A-Z]))?)", head)
        numeros = [m.group(1)] if m else numeros
    return {"objet": objet, "numeros": numeros, "resultat": resultat}


# ---------------------------------------------------------------------------
# Élections et nominations (président, vice-présidents, questeurs, secrétaires, juges…)
# ---------------------------------------------------------------------------
# Annoncées par la présidence, en une prise de parole : « Voici le résultat du deuxième tour du scrutin pour
# l'élection des vice-présidents… Ont obtenu : M. X : 325 voix ; … M. X ayant obtenu…, je le proclame élu… »
_TOURS = {"premier": 1, "deuxième": 2, "second": 2, "troisième": 3}


def parse_election(text: str) -> dict | None:
    """Annonce de résultats (« Ont obtenu : … ») -> objet, tour, décompte, voix par candidat, élus."""
    if "Ont obtenu" not in text:
        return None
    candidats = [{"nom": m[1], "voix": int(m[2])} for m in re.finditer(r"^(.+?) : (\d+) voix", text, flags=re.M)
                 if not m[1].startswith("Autre")]
    objet = re.search(r"résultats? (?:d\w* (\w+) tour )?d\w* scrutin pour (.+?)[.:]?$", text, flags=re.M)

    def nombre(libelle: str) -> int | None:
        m = re.search(libelle + r" : (\d+)", text)
        return int(m[1]) if m else None

    return {
        "objet": objet[2].strip() if objet else None,
        "tour": _TOURS.get(objet[1]) if objet else None,  # None : pas écrit, déduit par l'appelant
        "votants": nombre("Nombre de votants"),
        "exprimes": nombre("[Ss]uffrages exprimés"),
        "majorite_absolue": nombre("Majorité absolue"),
        "candidats": sorted(candidats, key=lambda c: -c["voix"]),
        "elus": [],
        "proclamation": None,
    }


def parse_proclamation(text: str, candidats: list[dict]) -> tuple[list[str], str] | None:
    """« M. X et Mme Y ayant obtenu…, je les proclame… » -> (élus, phrase). Élu = candidat dont le nom de famille
    figure dans la phrase, à une coquille près (19/07/2024 : « Gabriel Amar » proclamé, candidat « Gabriel Amard »)."""
    m = re.search(r"^.*je (?:le|la|les) proclame.*$", text, flags=re.M)
    if not m:
        return None
    mots = re.findall(r"[\w’'-]{3,}", m[0])
    return [c["nom"] for c in candidats if c["nom"].split()[-1] in m[0]
            or difflib.get_close_matches(c["nom"].split()[-1], mots, n=1, cutoff=0.85)], m[0]


# Heure de fin : « (La séance est levée à dix-neuf heures cinquante-cinq.) »
_NOMBRES = {"zéro": 0, "une": 1, "deux": 2, "trois": 3, "quatre": 4, "cinq": 5, "six": 6, "sept": 7, "huit": 8,
            "neuf": 9, "dix": 10, "onze": 11, "douze": 12, "treize": 13, "quatorze": 14, "quinze": 15, "seize": 16,
            "vingt": 20, "trente": 30, "quarante": 40, "cinquante": 50, "quart": 15, "demie": 30}


def parse_heure_fin(text: str) -> str | None:
    """« … levée à vingt heures cinquante. » -> « 20:50 » ; « à midi quinze » -> « 12:15 »."""
    m = re.search(r"séance est levée[^)]*? à ([^.)]+)", text)
    if not m:
        return None
    t = m[1].replace("-heure", " heure")  # « vingt-deux-heures quinze » (coquille du compte rendu)
    t = re.sub(r"\bmidi\b", "douze heures", re.sub(r"\bminuit\b", "zéro heure", t))
    heures, *minutes = re.split(r" heures?", t)  # « seize heures vingt » -> « seize », « vingt »
    try:
        h, mn = (sum(_NOMBRES[w] for w in re.split(r"[- ]+", x) if w and w != "et") for x in (heures, "".join(minutes)))
    except KeyError:
        return None
    return f"{h:02d}:{mn:02d}"


# ---------------------------------------------------------------------------
# Séance
# ---------------------------------------------------------------------------

# %% [markdown]
# ## Extraction et classification des séances

# %%

def _role(nom: str, qualite: str, role_debat: str | None) -> str:
    q = qualite.lower()
    if role_debat == "president" or re.fullmatch(r"(M\.|Mme) l[ea] présidente?", nom):
        return "president_seance"
    if "ministre" in q or "secrétaire d" in q:
        return "gouvernement"
    if "rapporteu" in q:
        return "rapporteur"
    if "président" in q:
        return "president_commission"
    return "depute"


class _Parser:
    def __init__(self, path: Path):
        self.path = path
        self.root = ET.parse(path).getroot()
        self.uid = self.root.findtext(NS + "uid")
        self.president: str | None = None
        self.titres: dict[int, str] = {}
        self.ctx = {"article": None, "amendement": None, "sous_amendement": None}
        self.points: list[dict] = []
        self.rows: list[dict] = []  # un paragraphe = une ligne, avant fusion
        self.presidences: list[dict] = []

    # --- lecture séquentielle du contenu -------------------------------------------------------
    def walk(self, el: ET.Element) -> None:
        for child in el:
            tag = _tag(child)
            if tag == "changementPresidence" or child.get("code_style") == "Présidence":
                self.set_president(_clean(_raw(child.find(NS + "texte")) or _raw(child)))
            if tag == "point":
                self.on_point(child)
                self.walk(child)
            elif tag == "paragraphe":
                self.on_paragraph(child)
            elif tag in ("ouvertureSeance", "finSeance", "changementPresidence", "interExtraction"):
                self.walk(child)  # interExtraction = rappel au règlement, ses paragraphes sont dedans

    def set_president(self, txt: str) -> None:
        # « Présidence de Mme X\nvice-présidente » ; parfois « M. X,\nvice-président », ou une ligne du débat collée
        # derrière (« …\nvice-présidente\nLa parole est à… ») : on ne garde que le nom et sa qualité
        lignes = [_clean(ligne).strip(" ,") for ligne in txt.replace(_BR, "\n").split("\n")]
        lignes = [ligne for ligne in lignes if ligne]
        nom = re.sub(r"^Présidence de\s+", "", lignes[0]) if lignes else ""
        if len(lignes) > 1 and re.match(r"(vice-)?présidente?\b|doyen", lignes[1]):
            nom = f"{nom}, {lignes[1]}"
        self.president = re.sub(r"(?<!,) (vice-président\w*)$", r", \1", nom).strip() or self.president
        self.presidences.append({"president": self.president, "a_partir_de": len(self.rows)})

    def on_point(self, el: ET.Element) -> None:
        lvl = int(el.get("nivpoint") or 1)
        titre = re.sub(r"(\S)\(", r"\1 (", split_reactions(_raw(el.find(NS + "texte")))[0].replace("\n", " "))
        if titre:
            self.titres = {k: v for k, v in self.titres.items() if k < lvl}
            self.titres[lvl] = titre
            self.points.append({
                "ordre": _num(el.get("ordre_absolu_seance")),
                "id_syceron": el.get("id_syceron"),
                "niveau": lvl,
                "titre": titre,
                "chemin": " > ".join(t for _, t in sorted(self.titres.items())),
                "code_grammaire": el.get("code_grammaire"),
            })
        for key, attr in (("article", "art"), ("amendement", "adt"), ("sous_amendement", "ssadt")):
            value = el.get(attr)
            if value:
                self.ctx[key] = value.strip()
            elif value == "" and titre:
                self.ctx[key] = None

    def on_paragraph(self, el: ET.Element) -> None:
        texte_el = el.find(NS + "texte")
        raw = _raw(texte_el)
        orateurs = [
            {"nom": _clean(o.findtext(NS + "nom", "")), "qualite": _clean(o.findtext(NS + "qualite", ""))}
            for o in el.iter(NS + "orateur")
        ]
        if orateurs:
            texte, reactions = split_reactions(raw)
        else:  # texte du compte rendu, souvent tout en italique : « (L'amendement no 21 n'est pas adopté.) »
            texte, reactions = _clean(raw.replace(_ITAL_OPEN, "").replace(_ITAL_CLOSE, "")), []
        if not texte and not reactions:
            return
        code = el.get("code_grammaire") or ""
        if not orateurs:
            typ = "didascalie"
        elif code.startswith("INTERRUPTION"):
            typ = "interruption"
        elif _role(orateurs[0]["nom"], orateurs[0]["qualite"], el.get("roledebat")) == "president_seance":
            typ = "presidence"
        else:
            typ = "discours"
        first = orateurs[0] if orateurs else {"nom": None, "qualite": ""}
        role = _role(first["nom"] or "", first["qualite"], el.get("roledebat")) if orateurs else None
        nom, groupe = first["nom"], None
        if nom and (m := re.fullmatch(r"(.+?)\s*\(([^()]{1,12})\)", nom)):  # « Mme Mathilde Panot (LFI-NFP) »
            nom, groupe = m.group(1), m.group(2)  # au nom de son groupe (explications de vote…)
        if role == "president_seance" and self.president:
            nom = re.sub(r"\s*,\s*vice-président\w*$", "", self.president)
        self.rows.append({
            "ordre": _num(el.get("ordre_absolu_seance")),
            "id_syceron": el.get("id_syceron"),
            "code_grammaire": code,
            "type": typ,
            "orateur": nom,
            "orateur_libelle": first["nom"],  # tel qu'écrit au compte rendu (« Mme la présidente »)
            "groupe": groupe,  # sigle, seulement quand le compte rendu l'indique
            "orateurs_associes": [o["nom"] for o in orateurs[1:]],
            "id_acteur": el.get("id_acteur"),
            "id_mandat": el.get("id_mandat"),
            "qualite": first["qualite"] or None,
            "role": role,
            "president_seance": self.president,
            "point_odj": _num(el.get("valeur_ptsodj")),
            "section": self.titres.get(1),
            "sous_section": self.titres.get(2),
            "sujet": " > ".join(t for _, t in sorted(self.titres.items())) or "Ouverture de la séance",
            **self.ctx,
            "stime": float(texte_el.get("stime")) if texte_el is not None and texte_el.get("stime") else None,
            "texte": texte,
            "reactions": reactions,
        })

    # --- fusion des paragraphes en interventions ------------------------------------------------
    @staticmethod
    def _context(r: dict) -> tuple:
        return (r["sujet"], r["article"], r["amendement"], r["sous_amendement"])

    def merge(self) -> list[dict]:
        out: list[dict] = []
        open_speech: dict | None = None  # dernier discours, prolongeable si son orateur reprend

        for r in self.rows:
            if r["type"] == "discours" and open_speech is not None \
                    and r["id_acteur"] == open_speech["id_acteur"] \
                    and self._context(r) == self._context(open_speech):
                open_speech["texte"] += "\n" + r["texte"]
                open_speech["reactions"] += r["reactions"]
                open_speech["paragraphes"].append(r["id_syceron"])
                open_speech["ordre_fin"] = r["ordre"]
                continue

            rec = {
                "id": f"{self.uid}#{r['ordre']}",
                "ordre_debut": r["ordre"],
                "ordre_fin": r["ordre"],
                "paragraphes": [r["id_syceron"]],
                **{k: v for k, v in r.items() if k not in ("ordre", "id_syceron")},
            }
            if r["type"] == "interruption" or (r["type"] == "didascalie" and not r["texte"]):
                if open_speech is not None:
                    rec["interrompt"] = open_speech["id"]
                    open_speech["interruptions"].append(rec["id"])
            elif r["type"] == "presidence" and open_speech is not None and len(r["texte"]) < PRESIDENCE_COURTE \
                    and not re.match(r"La parole est (à|maintenant à)", r["texte"]):
                rec["interrompt"] = open_speech["id"]
                open_speech["interruptions"].append(rec["id"])
            else:
                open_speech = None
            if r["type"] == "discours":
                rec["interruptions"] = []
                open_speech = rec
            out.append(rec)

        for rec in out:
            # mots = suites séparées par des espaces contenant une lettre ou un chiffre : en français, « ! ? : ; »,
            # les tirets et les guillemets sont entourés d'espaces et ne doivent pas compter
            rec["nb_mots"] = sum(1 for w in rec["texte"].split() if any(c.isalnum() for c in w))
            rec["nb_caracteres"] = len(rec["texte"])
        return out

    # --- votes ----------------------------------------------------------------------------------
    def votes(self) -> list[dict]:
        votes, scrutin = [], None
        for r in self.rows:
            if r["type"] == "presidence" and "résultat du scrutin" in r["texte"]:
                if d := parse_vote_presidence(r["texte"]):
                    votes.append({"id": f"{self.uid}#{r['ordre']}", "ordre": r["ordre"], **d, "scrutin_public": True,
                                  "sujet": r["sujet"], "article_en_discussion": r["article"], "texte": r["texte"]})
                    scrutin = None
                else:
                    scrutin = parse_scrutin(r["texte"])
                continue
            if r["type"] != "didascalie":
                continue
            d = parse_decision(r["texte"])
            if d is None:
                continue
            votes.append({
                "id": f"{self.uid}#{r['ordre']}",
                "ordre": r["ordre"],
                **d,
                "scrutin_public": scrutin is not None,
                "scrutin": scrutin,
                "sujet": r["sujet"],
                "article_en_discussion": r["article"],
                "texte": r["texte"],
            })
            scrutin = None
        return votes

    def elections(self) -> list[dict]:
        out: list[dict] = []
        for r in self.rows:
            if r["type"] == "presidence" and (e := parse_election(r["texte"])):
                # « Voici le résultat du scrutin… » (tour non écrit) : tour suivant si le précédent n'a élu personne
                if e["tour"] is None:
                    e["tour"] = out[-1]["tour"] + 1 if out and not out[-1]["elus"] else 1
                out.append({"id": f"{self.uid}#{r['ordre']}", "ordre": r["ordre"], **e, "sujet": r["sujet"]})
            # la proclamation est dans la même prise de parole que les résultats, ou dans la suivante
            if r["type"] == "presidence" and out and not out[-1]["elus"] \
                    and (p := parse_proclamation(r["texte"], out[-1]["candidats"])):
                out[-1]["elus"], out[-1]["proclamation"] = p
        return out

    def heure_fin(self) -> str | None:
        for r in reversed(self.rows):
            if r["type"] == "didascalie" and (h := parse_heure_fin(r["texte"])):
                return h
        return None

    def run(self) -> dict:
        meta = self.root.find(NS + "metadonnees")
        date_raw = meta.findtext(NS + "dateSeance") or ""
        dt = datetime.strptime(date_raw[:12], "%Y%m%d%H%M") if len(date_raw) >= 12 else None
        p = meta.find(f"{NS}sommaire/{NS}presidentSeance")
        if p is not None:
            self.set_president(_raw(p))
        self.walk(self.root.find(NS + "contenu"))
        interventions = self.merge()
        votes = self.votes()
        elections = self.elections()
        by_type: dict[str, int] = {}
        for i in interventions:
            by_type[i["type"]] = by_type.get(i["type"], 0) + 1
        return {
            "schema": SCHEMA,
            "seance": {
                "uid": self.uid,
                # identifiant de la séance dans le jeu Scrutins (RUANR…)
                "seance_ref": self.root.findtext(NS + "seanceRef"),
                "date": dt.date().isoformat() if dt else None,
                "heure_debut": dt.strftime("%H:%M") if dt else None,
                "heure_fin": self.heure_fin(),  # heure de levée de la séance (00:00 = minuit ; peut être le lendemain)
                "date_libelle": meta.findtext(NS + "dateSeanceJour"),
                "num_seance": _num(meta.findtext(NS + "numSeance")),
                "num_seance_jour": _num(meta.findtext(NS + "numSeanceJour")),
                "legislature": _num(meta.findtext(NS + "legislature")),
                "session": meta.findtext(NS + "session"),
                "etat": meta.findtext(NS + "etat"),
                "version": meta.findtext(NS + "version"),
                "fichier": self.path.name,
            },
            "presidences": sorted({p["president"] for p in self.presidences if p["president"]}),
            "statistiques": {
                "paragraphes": len(self.rows),
                "interventions": by_type,
                "orateurs_distincts": len({i["id_acteur"] for i in interventions if i["type"] == "discours"}),
                "votes": len(votes),
                "scrutins_publics": sum(v["scrutin_public"] for v in votes),
                "elections": len(elections),
            },
            "points": self.points,
            "interventions": interventions,
            "votes": votes,
            "elections": elections,
        }


def parse_seance(path: str | Path) -> dict:
    return _Parser(Path(path)).run()


def est_provisoire(seance: dict) -> bool:
    """Compte rendu provisoire (publié avant le Journal officiel) : incomplet, et des numéros d'ordre y
    reviennent plusieurs fois (identifiants en double). L'AN le remplace ensuite par la version définitive,
    sous le même nom de fichier : il faut l'ignorer et attendre celle-ci."""
    return seance["seance"].get("etat") == "provisoire"


def load_seance(path: str | Path) -> dict:
    """XML (parsé à la volée) ou JSON silver déjà produit."""
    path = Path(path)
    if path.suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    return parse_seance(path)



# %% [markdown]
# ## Exports tables et schéma SQL

# %%
# ---------------------------------------------------------------------------
# Sortie en tables : une ligne par enregistrement, colonnes plates (--format tables)
# ---------------------------------------------------------------------------
TABLES_SCHEMA = "an_cr_tables/1"
_N_TYPE = {"discours": "nb_discours", "interruption": "nb_interruptions", "presidence": "nb_presidence",
           "didascalie": "nb_didascalies"}
# table -> colonne -> type PostgreSQL ; sert à la fois aux lignes écrites et à schema.sql
COLONNES: dict[str, dict[str, str]] = {
    "seances": {
        "uid": "text PRIMARY KEY", "seance_ref": "text", "date": "date", "heure_debut": "text", "heure_fin": "text",
        "date_libelle": "text", "num_seance": "int", "num_seance_jour": "int", "legislature": "int",
        "session": "text", "etat": "text", "version": "text", "fichier": "text", "presidences": "text[]",
        "nb_paragraphes": "int", **{c: "int" for c in _N_TYPE.values()}, "orateurs_distincts": "int",
        "nb_votes": "int", "nb_scrutins_publics": "int", "nb_elections": "int",
    },
    "interventions": {
        "id": "text PRIMARY KEY", "seance_uid": "text", "ordre_debut": "int", "ordre_fin": "int",
        "paragraphes": "text[]", "code_grammaire": "text", "type": "text", "orateur": "text",
        "orateur_libelle": "text", "groupe": "text", "orateurs_associes": "text[]", "id_acteur": "text",
        "id_mandat": "text", "qualite": "text", "role": "text", "president_seance": "text", "point_odj": "int",
        "section": "text", "sous_section": "text", "sujet": "text", "article": "text", "amendement": "text",
        "sous_amendement": "text", "stime": "double precision", "texte": "text", "reactions": "text[]",
        "interrompt": "text", "nb_mots": "int", "nb_caracteres": "int",
    },
    "votes": {
        "id": "text PRIMARY KEY", "seance_uid": "text", "ordre": "int", "objet": "text", "numeros": "text[]",
        "resultat": "text", "scrutin_public": "boolean", "votants": "int", "exprimes": "int",
        "majorite_absolue": "int", "pour": "int", "contre": "int", "sujet": "text",
        "article_en_discussion": "text", "texte": "text",
    },
    "points": {
        "id": "text PRIMARY KEY", "seance_uid": "text", "ordre": "int", "id_syceron": "text", "niveau": "int",
        "titre": "text", "chemin": "text", "code_grammaire": "text",
    },
    "elections": {
        "id": "text PRIMARY KEY", "seance_uid": "text", "ordre": "int", "objet": "text", "tour": "int",
        "votants": "int", "exprimes": "int", "majorite_absolue": "int", "candidats": "jsonb", "elus": "text[]",
        "proclamation": "text", "sujet": "text",
    },
}


def tables(s: dict) -> dict[str, list[dict]]:
    """Séance silver -> lignes de chaque table. Les tables filles portent `seance_uid` ; une intervention garde
    `interrompt` (le discours qu'elle interrompt) : la liste inverse se retrouve par jointure."""
    se, st, uid = s["seance"], s["statistiques"], s["seance"]["uid"]
    seance = {**se, "presidences": s["presidences"], "nb_paragraphes": st["paragraphes"],
              **{c: st["interventions"].get(t, 0) for t, c in _N_TYPE.items()},
              "orateurs_distincts": st["orateurs_distincts"], "nb_votes": st["votes"],
              "nb_scrutins_publics": st["scrutins_publics"], "nb_elections": st["elections"]}
    lignes = {
        "seances": [seance],
        "interventions": [{"seance_uid": uid, **i} for i in s["interventions"]],
        "votes": [{"seance_uid": uid, **v, **(v["scrutin"] or {})} for v in s["votes"]],
        "points": [{"id": f"{uid}#{p['ordre']}", "seance_uid": uid, **p} for p in s["points"]],
        "elections": [{"seance_uid": uid, **e} for e in s["elections"]],
    }
    return {t: [{c: r.get(c) for c in COLONNES[t]} for r in rows] for t, rows in lignes.items()}


def schema_sql() -> str:
    """CREATE TABLE proposés pour les tables (à adapter : noms, schéma, index)."""
    out = [f"-- Tables des comptes rendus de séance, format {TABLES_SCHEMA} (généré par silver.py --format tables)",
           "-- Supprimer une séance supprime ses lignes filles (ON DELETE CASCADE) : recharger une séance =",
           "-- DELETE FROM seances WHERE uid = '…' puis INSERT de ses lignes dans chaque table."]
    for table, cols in COLONNES.items():
        defs = [f"    {c} {typ}" + (" REFERENCES seances (uid) ON DELETE CASCADE" if c == "seance_uid" else "")
                for c, typ in cols.items()]
        out.append(f"CREATE TABLE IF NOT EXISTS {table} (\n" + ",\n".join(defs) + "\n);")
        if "seance_uid" in cols:
            out.append(f"CREATE INDEX IF NOT EXISTS {table}_seance ON {table} (seance_uid);")
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

# %% [markdown]
# ## CLI d’origine

# %%
def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sources", nargs="+", type=Path, help="fichiers XML ou dossiers qui en contiennent")
    ap.add_argument("--out", type=Path, default=Path("silver"), help="dossier de sortie (défaut : silver/)")
    ap.add_argument("--format", choices=["json", "tables"], default="json",
                    help="json : un JSON par séance ; tables : un JSONL par table")
    args = ap.parse_args(argv)

    files = [f for s in args.sources for f in (sorted(s.glob("*.xml")) if s.is_dir() else [s])]
    args.out.mkdir(parents=True, exist_ok=True)
    ecrits, ignores, erreurs = 0, [], []
    sorties, vues = {}, set()
    if args.format == "tables":  # fichiers réécrits à chaque lancement
        sorties = {t: (args.out / f"{t}.jsonl").open("w", encoding="utf-8") for t in COLONNES}
    for f in files:
        try:  # un fichier illisible est signalé, sans arrêter les autres
            s = parse_seance(f)
        except Exception as e:
            erreurs.append(f.name)
            print(f"❌ {f.name} : {type(e).__name__}: {e}", file=sys.stderr)
            continue
        if est_provisoire(s):
            ignores.append(f.name)
            print(f"⏭️  {f.name} : compte rendu provisoire, ignoré (la version définitive le remplacera)")
            continue
        if args.format == "json":
            (args.out / f"{f.stem}.json").write_text(json.dumps(s, ensure_ascii=False, indent=1), encoding="utf-8")
        elif s["seance"]["uid"] in vues:  # même séance passée deux fois : des clés primaires en double
            print(f"⏭️  {f.name} : séance {s['seance']['uid']} déjà écrite, ignorée")
            continue
        else:
            vues.add(s["seance"]["uid"])
            for t, rows in tables(s).items():
                sorties[t].writelines(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
        ecrits += 1
        st = s["statistiques"]
        print(f"{f.name} : {st['paragraphes']} paragraphes -> {sum(st['interventions'].values())} interventions "
              f"{st['interventions']}, {st['votes']} votes")
    for out in sorties.values():
        out.close()
    print(f"✅ {ecrits} séances -> {args.out}/" + (f" ; {len(ignores)} provisoire(s) ignorée(s)" if ignores else "")
          + (f" ; {len(erreurs)} en erreur : {', '.join(erreurs)}" if erreurs else ""))
    if erreurs:
        sys.exit(1)  # code de sortie non nul : l'orchestrateur de l'ETL voit l'échec


# %%
import json
import sys
from pathlib import Path, PurePosixPath

cwd = Path.cwd().resolve()
locations = [cwd]
try:
    locations.insert(0, Path(__file__).resolve().parent)
except NameError:
    pass

PROJECT_ROOT = None
for location in locations:
    candidates = [location, *location.parents]
    PROJECT_ROOT = next((path for path in candidates if (path / 'lib' / 'config.py').is_file()), None)
    if PROJECT_ROOT is None:
        PROJECT_ROOT = next((path / 'data' for path in candidates if (path / 'data' / 'lib' / 'config.py').is_file()), None)
    if PROJECT_ROOT is None:
        for child in location.iterdir():
            if (child / 'lib' / 'config.py').is_file():
                PROJECT_ROOT = child
                break
            if (child / 'data' / 'lib' / 'config.py').is_file():
                PROJECT_ROOT = child / 'data'
                break
    if PROJECT_ROOT is not None:
        break
if PROJECT_ROOT is None:
    raise FileNotFoundError('Impossible de trouver la racine du dépôt data (lib/config.py).')

sys.path.insert(0, str(PROJECT_ROOT))
from lib.config import AN_BRONZE, AN_SILVER

BRONZE_DIR = AN_BRONZE / 'cr_seances'
XML_DIR = BRONZE_DIR / 'xml'
BRONZE_MANIFEST = BRONZE_DIR / 'manifest.json'
SILVER_DIR = AN_SILVER / 'cr_seances'
SEANCES_DIR = SILVER_DIR / 'seances'
TABLES_DIR = SILVER_DIR / 'tables'
OUTPUT_FORMAT = 'json'
OUTPUT_DIR = SEANCES_DIR if OUTPUT_FORMAT == 'json' else TABLES_DIR

if OUTPUT_FORMAT not in {'json', 'tables'}:
    raise ValueError('OUTPUT_FORMAT doit être json ou tables.')
if not BRONZE_MANIFEST.is_file():
    raise FileNotFoundError(f'Manifeste Bronze absent : {BRONZE_MANIFEST}. Exécuter le notebook de récupération.')
bronze_manifest = json.loads(BRONZE_MANIFEST.read_text(encoding='utf-8'))
xml_paths = [XML_DIR.joinpath(*PurePosixPath(name).parts) for name in bronze_manifest['xml_files']]
if not xml_paths:
    raise ValueError('Le manifeste Bronze ne référence aucun XML.')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# ## Exécution du format choisi
#
# Le CLI source reçoit explicitement chaque XML listé dans le manifeste Bronze. Il conserve son comportement de sortie, y compris le filtrage des séances provisoires, la gestion des doublons en mode tables et le code d’échec en cas d’erreur.

# %%
source_args = [str(xml_path) for xml_path in xml_paths]
main([*source_args, '--out', str(OUTPUT_DIR), '--format', OUTPUT_FORMAT])

