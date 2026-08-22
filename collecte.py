#  COLLECTE & FILTRAGE — SWIO  v17.0
#  BASES SCIENTIFIQUES (sans clé) :
#    OpenAlex · Crossref · HAL
#  BASES JURIDIQUES — décisions de justice uniquement :
#    ITLOS    : décisions Tribunal international du droit de la mer
#    CIJ/ICJ  : arrêts Cour internationale de Justice
#    ECOLEX   : décisions de justice nationales (pêche)
#    CTOI/IOTC: résolutions et sanctions de compliance
#    Justice Administrative FR : amendes, confiscations, arraisonnements
#  BASES PRESSE :
#    The Guardian        : API, clé gratuite
#    Africa Eco News     : RSS sans clé
#    The Conversation Af.: RSS Atom sans clé
#    Africa Defense Forum: RSS sans clé
#    AllAfrica           : scraping sans clé
#    Presse locale îles  : RSS Clicanoo, Le Mauricien, L'Express,
#                          Mayotte Hebdo, SNA, Comores Infos, etc.
#  PLAFOND : 100 résultats max par catégorie (science / juridique / presse)
#  FILTRAGE DIFFÉRENCIÉ PAR CATÉGORIE :
#    Science  : zone + pêche + (conflit OU gouvernance) [strict]
#    Juridique: pêche dans le TITRE + zone obligatoire   [durci]
#    Presse   : pays_SWIO + pêche                        [adapté]
#  DÉDOUBLONNAGE 3 NIVEAUX :
#    D1 — intra-base (DOI + titre normalisé)
#    D2 — inter-catégories
#    D3 — Jaccard >= 82% sur tokens titre (final)
#  SORTIES :
#    articles_filtres_SWIO.ris · juridique_filtres_SWIO.ris
#    presse_filtres_SWIO.ris   · SWIO_dedup_global.ris
#  PRÉREQUIS :
#    pip install requests lxml

import requests, time, re, sys, html, unicodedata, os
from concurrent.futures import ThreadPoolExecutor, as_completed

print(f"Script chargé depuis : {__file__}")

#  CONFIGURATION

ZONES_A_COLLECTER = None   # None = toutes les zones
#  PLAFONDS PAR CATÉGORIE (après filtrage)
MAX_SCIENCE   = 100   # articles scientifiques retenus max
MAX_JURIDIQUE = 500   # décisions juridiques retenues max (CTOI fournit ~400 docs)
MAX_PRESSE    = 100   # articles de presse retenus max

NB_MAX_DEFAUT     = 600
ANNEE_MIN         = 1970
ANNEE_MAX         = 2026

SCRIPT_DIR        = os.path.dirname(os.path.abspath(__file__))
FICHIER_SCIENCE   = os.path.join(SCRIPT_DIR, "articles_filtres_SWIO.ris")
FICHIER_JURIDIQUE = os.path.join(SCRIPT_DIR, "juridique_filtres_SWIO.ris")
FICHIER_PRESSE    = os.path.join(SCRIPT_DIR, "presse_filtres_SWIO.ris")
FICHIER_GLOBAL    = os.path.join(SCRIPT_DIR, "SWIO_dedup_global.ris")


# Clé Guardian (gratuite sur open-platform.theguardian.com)
GUARDIAN_API_KEY   = "ac3bcea4-b8ac-409b-a3fe-591de68fa4d1"

# RSS africains — sans clé, articles récents uniquement
RSS_AFRICA_ECO_NEWS      = "https://africaeconews.co.ke/feed"
RSS_CONVERSATION_AFRICA  = "https://theconversation.com/africa/articles.atom"
RSS_AFRICA_DEFENSE_FORUM = "https://adf-magazine.com/feed"

# RSS presse locale îles SWIO — sans clé
RSS_ILES_SWIO = [
    # ── Flux qui répondent de façon fiable (testés) ──
    ("https://www.lemauricien.com/feed/",                   "Le Mauricien"),
    ("https://www.mayottehebdo.com/feed",                   "Mayotte Hebdo"),
    ("https://www.imaz.fr/feed",                            "Imaz Press Réunion"),
    # ── Flux désactivés : 403 (blocage), 404 (URL morte), DNS/SSL cassés ──
    # Réactiver si l'URL du flux change. Les conflits de ces îles sont
    # désormais aussi captés via les requêtes Guardian ciblées (Seychelles,
    # Maurice, Mayotte, Comores, Réunion, Chagos, Éparses).
    #   Clicanoo (403) · L'Express Maurice (404) · Seychelles News Agency (403)
    #   Al-Watwan (timeout) · Linfo.re (404) · iPreunion (404) · Zinfos974 (403)
    #   The Seychelles Nation (404) · SNA Seychelles (403)
    #   Comores Infos (DNS) · Komor Info (SSL)
]

#  ZONES SWIO

ZONES_SWIO = {
    "SWIO": {
        "requete": "fisheries Indian Ocean", "nb_max": 600,
        "filtres": [
            "indian ocean","océan indien","ocean indien","swio","iotc","ctoi",
            "western indian ocean","madagascar","mozambique",
            "la reunion","ile de la reunion","île de la réunion","reunion island",
            "mayotte","seychelles","mauritius","île maurice","ile maurice",
            "comoros","comores","tanzania","tanzanie","kenya","somalia","somalie",
            "mozambique channel","canal du mozambique","south africa","afrique du sud",
            "taaf","terres australes","tromelin","glorieuses","juan de nova","europa",
            "bassas da india","crozet","kerguelen","saint paul","amsterdam island"
        ]
    },
    "Madagascar":  {"requete":"fisheries Madagascar",
        "filtres":["madagascar","madagasikara","malagasy"]},
    "Mozambique":  {"requete":"fisheries Mozambique",
        "filtres":["mozambique","moçambique","mocambique"]},
    "La Reunion":  {"requete":"fisheries Reunion Island Indian Ocean",
        "filtres":["la reunion","ile de la reunion","île de la réunion","reunion island","974"]},
    "Mayotte":     {"requete":"fisheries Mayotte Indian Ocean",
        "filtres":["mayotte","maore","mahoré","976"]},
    "Comores":     {"requete":"fisheries Comoros",
        "filtres":["comores","union des comores","comoros","ngazidja","anjouan","moheli"]},
    "Seychelles":  {"requete":"fisheries Seychelles",
        "filtres":["seychelles","mahe","mahé","praslin"]},
    "Maurice":     {"requete":"fisheries Mauritius",
        "filtres":["île maurice","ile maurice","mauritius","rodrigues"]},
    "Tanzanie":    {"requete":"fisheries Tanzania",
        "filtres":["tanzanie","tanzania","zanzibar","dar es salaam"]},
    "Kenya":       {"requete":"fisheries Kenya Indian Ocean",
        "filtres":["kenya","mombasa","lamu","malindi","kenyan coast"]},
    "Somalie":     {"requete":"fisheries Somalia",
        "filtres":["somalie","somalia","somaliland","horn of africa","corne de l'afrique"]},
    "Canal du Mozambique": {"requete":"fisheries Mozambique Channel",
        "filtres":["canal du mozambique","mozambique channel"]},
    "Afrique du Sud": {"requete":"fisheries South Africa",
        "filtres":["afrique du sud","south africa","cape town","durban","kwazulu"]},
    "TAAF": {"requete":"fisheries French Southern Territories Indian Ocean",
        "filtres":["taaf","terres australes","tromelin","glorieuses","juan de nova","europa",
                   "bassas da india","crozet","kerguelen","amsterdam island","saint paul"]},
}

#  BLOCS DE FILTRAGE — partagés

BLOC_PECHE = [
    "fish","fishing","fisheries","fishery","fisherman","fishermen",
    "trawl","tuna","shrimp","squid","lobster","octopus",
    "iuu","aquaculture","mariculture","iotc","catch","harvest",
    "small pelagic","large pelagic","demersal","reef fish","marine resource",
    "pêche","peche","pêcheur","pecheur","halieutique","chalut","thon",
    "crevette","calamar","langouste","poulpe","ctoi","ressource halieutique"
]

#  BLOC CONFLIT STRICT — pour le filtre SCIENCE
#  Uniquement de VRAIS termes de conflit / pêche illégale / tension.
#  AUCUN terme de pure gouvernance (governance, management, policy,
#  quota, monitoring...) : un article de simple gestion halieutique
#  NE DOIT PAS passer s'il ne parle pas de conflit.
BLOC_CONFLIT_STRICT = [
    # Conflits / tensions / rivalités
    "conflict","dispute","tension","confrontation","contestation",
    "rivalry","competition for","clash","standoff","row over",
    # Pêche illégale / INN / braconnage / piraterie
    "illegal fishing","illegal, unreported","iuu","poaching","poacher",
    "piracy","pirate","overfishing","overexploit","overexploitation",
    "plunder","plundering","stolen fish","fish theft",
    # Flottes étrangères / incursions / souveraineté contestée
    "foreign vessel","foreign fleet","distant water","distant-water",
    "incursion","encroachment","territorial dispute","maritime dispute",
    "sovereignty dispute","contested water","cross-border tension",
    # Application de la loi / arraisonnements / sanctions
    "arrest","arrested","seized","seizure","seizing","boarded","boarding",
    "raid","detained","detention","confiscation","fine","sentenced","convicted",
    "navy","coast guard","patrol vessel","warship","gunboat","enforcement action",
    "violation","infringement","non-compliance","crackdown",
    # Dimension sociale / accès / déplacement
    "protest","strike","demonstration","resistance","blockade",
    "displacement","dispossession","exclusion","eviction",
    "gear conflict","access conflict","resource conflict","fishing war",
    "blue justice","inequity","grievance",
    # Français — conflits
    "conflit","différend","differend","tension","affrontement","rivalité",
    "pêche illégale","peche illegale","pêche illicite","peche illicite",
    "braconnage","piraterie","surpêche","surpeche","pillage","pillent",
    "navire étranger","flotte étrangère","incursion","arraisonnement",
    "arraisonné","saisie","saisi","confiscation","amende","condamné",
    "arrestation","arrêté","garde-côte","garde-côtes","patrouilleur",
    "infraction","violation","revendication","souveraineté contestée",
    "contentieux","différend maritime","grève","manifestation",
    "expulsion","éviction","conflit d'usage","conflit de ressource",
    "guerre de la pêche","guerre du poisson","justice bleue","incursions"
]

BLOC_CONFLIT_GOUVERNANCE = [
    # Conflits
    "conflict","dispute","tension","competition","rivalry","poaching",
    "illegal fishing","piracy","overfishing","overexploit","iuu",
    "pressure","threat","violation","infringement","enforcement",
    "foreign vessel","foreign fleet","sovereignty","transboundary",
    "access right","small-scale","artisanal","community","local fisher",
    "livelihood","displacement","blue justice","overallocation",
    # Gouvernance
    "governance","management","policy","agreement","regulation",
    "licence","license","permit","rights","co-management","framework",
    "treaty","law","legislation","reform","eez","zee","unclos","cnudm",
    "mpa","quota","allocation","monitoring","surveillance","rfmo",
    # Français
    "conflit","différend","braconnage","pêche illégale","piraterie",
    "surpêche","revendication","souveraineté","accord de pêche",
    "gouvernance","gestion","politique","réglementation","traité",
    "surveillance","contrôle","gestion des pêches","prise accessoire"
]

# Bloc conflit RESSERRÉ pour la presse uniquement
# Supprimés : pressure, impact, degradation, community, small-scale,
#             subsistence, livelihood, vulnerability, depletion, overuse,
#             unsustainable, stakeholder (trop génériques → bruit)
BLOC_CONFLIT_PRESSE = [
    # Conflits directs
    "illegal fishing","iuu","piracy","poaching",
    "conflict","dispute","tension","confrontation","contestation",
    "foreign vessel","foreign fleet","distant water",
    "overfishing","overexploit","overallocation",
    "violation","infringement","enforcement","non-compliance",
    "access right","fishing agreement","access agreement",
    "sovereignty","territorial","eez","zee","transboundary","cross-border",
    "arrest","arrested","seized","seizure","raid","fine","sentence","convicted",
    "protest","strike","demonstration","resistance","opposition",
    "displacement","dispossession","exclusion",
    "blue justice","inequity","asymmetry",
    "gear conflict","bycatch conflict",
    "navy","coast guard","patrol","warship","vessel boarded",
    "deal","accord","ban","moratorium",
    # Français
    "pêche illégale","peche illegale","piraterie","braconnage",
    "conflit","différend","differend","revendication",
    "accord de pêche","droits d'accès","droits dacces",
    "flotte étrangère","flotte etrangere","souveraineté","souverainete",
    "arraisonnement","saisie","amende","arrestation","condamné",
    "grève","greve","manifestation","résistance",
    "marine nationale","garde-côte","patrouille",
    "surpêche","surpeche","exclusion",
]

#  EAU DE MER vs EAU DOUCE
#  On veut UNIQUEMENT des conflits de pêche en milieu MARIN.
#  BLOC_MARIN : marqueurs clairs de mer / océan / pêche maritime.
#  EXCLUSIONS_EAU_DOUCE : lacs, fleuves, rivières, aquaculture continentale.
BLOC_MARIN = [
    # Milieu marin générique
    "sea","ocean","marine","maritime","offshore","coastal","coast",
    "high seas","open sea","seabed","continental shelf","territorial waters",
    "eez","exclusive economic zone","seawater","saltwater",
    # Espèces / pêcheries typiquement marines
    "tuna","yellowfin","skipjack","bigeye","billfish","swordfish","marlin",
    "shark","sardine","mackerel","anchovy","shrimp","prawn","lobster",
    "octopus","squid","sea cucumber","reef fish","demersal","pelagic",
    "trawler","purse seine","longline","gillnet","trawling","dhow",
    # Lieux / institutions marines SWIO
    "indian ocean","western indian ocean","swio","iotc","mozambique channel",
    "coast guard","navy","naval","fishing vessel","fishing fleet",
    # Français — marin
    "mer","océan","ocean","marin","maritime","côtier","cotier","côte","cote",
    "haute mer","large","zone économique exclusive","zee","eaux territoriales",
    "thon","thonier","albacore","listao","requin","sardine","maquereau",
    "crevette","langouste","poulpe","calmar","chalutier","senneur",
    "palangrier","océan indien","ocean indien","canal du mozambique",
    "garde-côte","garde-côtes","marine nationale","navire de pêche",
    "pélagique","pelagique","démersal","demersal","boutre",
]

EXCLUSIONS_EAU_DOUCE = [
    # Grands lacs africains
    "lake victoria","lac victoria","lake tanganyika","lac tanganyika",
    "lake malawi","lac malawi","lake kivu","lac kivu","lake turkana",
    "lac turkana","lake albert","lake edward","lake naivasha","lake kariba",
    "lac kariba","lake chad","lac tchad","lake nasser",
    # Génériques lac / fleuve / rivière
    "lake","lakes","lac","lacs","river","rivers","rivière","riviere",
    "fleuve","fleuves","freshwater","inland water","inland fishery",
    "inland fishing","wetland","marais","floodplain","dam","barrage",
    "reservoir","réservoir","pond","étang","etang",
    # Espèces / aquaculture d'eau douce
    "tilapia","nile perch","perche du nil","catfish","poisson-chat",
    "fish farm","fish pond","pisciculture","aquaculture continentale",
    "freshwater fish","poisson d'eau douce",
]

# Termes d'exclusion géographique (hors SWIO)
# Un article contenant l'un de ces termes est RETIRÉ, sauf s'il contient aussi
# un terme de FILTRES_ZONE_FORTS (= un pays/lieu SWIO précis).
EXCLUSIONS_HORS_ZONE = [
    # ── Asie de l'Est ──────────────────────────────────────────
    "korea","korean","south korea","north korea","japan","japanese",
    "china","chinese","taiwan","taiwanese","hong kong","mongolia",
    "yellow sea","east china sea","sea of japan","bohai",
    # ── Asie du Sud / Sud-Est (hors SWIO) ──────────────────────
    "pakistan","pakistani","bangladesh","sri lanka","sri lankan",
    "myanmar","burma","indonesia","indonesian","thailand","thai",
    "malaysia","malaysian","vietnam","vietnamese","philippines","philippine",
    "cambodia","laos","singapore","brunei","timor","papua","nepal",
    "java","sumatra","borneo","sulawesi","bali","mekong",
    "andaman","nicobar","laccadive","maldive","maldives",
    "bay of bengal","gulf of thailand","thailand gulf","south china sea",
    "china sea","strait of malacca","sunda",
    # ── Inde (côtes hors SWIO occidental) ──────────────────────
    "india coast","indian coast","eastern india","bengal","chennai",
    "kolkata","mumbai coast","gujarat","kerala","tamil nadu","goa",
    # ── Moyen-Orient / Golfe / mer Rouge ───────────────────────
    "oman","yemen","iran","iranian","iraq","saudi","saudi arabia",
    "qatar","kuwait","bahrain","emirates","uae","dubai",
    "persian gulf","arabian gulf","gulf of oman","strait of hormuz",
    "arabian sea","red sea","gulf of aden","suez",
    # ── Océanie / Australie / Pacifique ────────────────────────
    "australia","australian","western australia","northwest australia",
    "perth","sydney","queensland","new zealand","fiji","tonga","samoa",
    "papua new guinea","solomon islands","vanuatu","micronesia",
    "pacific ocean","south pacific","coral sea","tasman sea","great barrier",
    # ── Atlantique / Amériques ─────────────────────────────────
    "atlantic","north atlantic","south atlantic","caribbean","gulf of mexico",
    "brazil","brazilian","argentina","chile","peru","mexico","canada","canadian",
    "usa","united states","alaska","gulf of guinea","west africa coast",
    "namibia","angola","ghana","nigeria","senegal","mauritania","morocco","cape verde",
    # ── Europe / mers du Nord et intérieures ───────────────────
    "mediterranean","méditerranée","mediterran","adriatic","aegean","tyrrhenian",
    "north sea","baltic","baltic sea","black sea","caspian","norway","norwegian",
    "iceland","icelandic","greenland","faroe","celtic sea","bay of biscay",
    "english channel","la manche","irish sea","portugal","spain","france coast",
    # ── Polaire ────────────────────────────────────────────────
    "arctic","antarctic","antarctica","antarct","southern ocean","ross sea","weddell",
]

# Exclusions renforcées pour la SCIENCE (titre uniquement)
# Si ces termes sont dans le TITRE sans aucun terme SWIO dans le titre → exclure
EXCLUSIONS_TITRE_SCIENCE = [
    "mediterranean","méditerranée","mediterran",
    "korean","korea","north sea","baltic","arctic","antarct",
    "atlantic","pacific","caribbean","north atlantic","south atlantic",
    "persian gulf","arabian sea","red sea","bay of bengal",
    "alaska","norway","norwegian","iceland","greenland",
    "australia","new zealand","china sea","south china",
    # Est de l'océan Indien — hors SWIO
    "eastern indian ocean","east indian ocean","bay of bengal",
    "andaman","laccadive","maldive","sri lanka","india coast",
    "indonesia","indonesian","java","sumatra","borneo",
    "thailand gulf","gulf of thailand","vietnam","philippines",
    "western australia","northwest australia","perth",
]

# Termes qui SAUVENT un article même si une exclusion est présente dans le titre
# (ex: "Atlantic bluefin tuna and Indian Ocean fisheries" → garder)
SAUVE_EXCLUSION = [
    "swio","iotc","ctoi","western indian ocean","indian ocean",
    "océan indien","madagascar","mozambique","seychelles","mauritius",
    "reunion","réunion","mayotte","comoros","comores","tanzania","kenya",
    "somalia","canal du mozambique","mozambique channel","taaf",
]

# Termes zone FORTS (présence d'un seul suffit à contrebalancer une exclusion).
# NB : "indian ocean"/"océan indien" sont VOLONTAIREMENT exclus d'ici — trop
# génériques, ils laissaient passer le bruit (ex: "Korean fleet in the Indian Ocean").
# Seul un pays/lieu SWIO précis peut désormais annuler une exclusion hors-zone.
FILTRES_ZONE_FORTS = [
    "swio","iotc","ctoi","western indian ocean","south west indian ocean",
    "south-west indian ocean","southwest indian ocean",
    "madagascar","mozambique","la reunion","reunion island","mayotte",
    "comoros","comores","seychelles","mauritius","île maurice",
    "tanzania","tanzanie","zanzibar","kenya","mombasa","somalia","somalie",
    "mozambique channel","canal du mozambique","south africa","afrique du sud",
    "taaf","terres australes","tromelin","glorieuses","crozet","kerguelen",
]

#  UTILITAIRES

def normaliser(t):
    if not t: return ""
    nfkd = unicodedata.normalize("NFKD", t.lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))

def contient_un(texte, termes):
    txt = normaliser(texte)
    return any(normaliser(t) in txt for t in termes)

def contient_un_mot(texte, termes):
    """
    Comme contient_un, mais correspondance MOT ENTIER (frontières \\b).
    Évite que 'sea' matche 'disease'/'season', ou 'mer' matche 'commerce'.
    Pour les termes multi-mots, teste la séquence entière comme un bloc.
    """
    txt = normaliser(texte)
    for t in termes:
        tn = normaliser(t).strip()
        if not tn:
            continue
        if re.search(r'\b' + re.escape(tn) + r'\b', txt):
            return True
    return False

# Faux ami : en français « pêche » (l'activité) et « pêche » (le fruit) sont
# des homonymes parfaits. Un article fruitier dans un pays SWIO pouvait passer
# le filtre presse car BLOC_PECHE contient "pêche"/"peche". Garde-fou ciblé :
# on ne retire un article QUE si son contexte est clairement fruitier ET qu'il
# ne contient aucun VRAI terme halieutique (poisson, thon, chalut, fish, IUU…).
# « fish » et tous les vrais termes ne sont jamais affectés.
CONTEXTE_FRUIT = [
    "fruit","fruits","verger","vergers","confiture","compote","abricot",
    "nectarine","peche blanche","peche jaune","peche plate","arbre fruitier",
    "arboriculture","recolte de peche","pulpe","noyau","orchard","peach tree",
    "peach harvest","stone fruit",
]
# Vrais termes halieutiques sans ambiguïté (sert à « sauver » un article même
# si un mot fruitier traîne : un article pêche+poisson reste pertinent).
PECHE_NON_AMBIGU = [
    "fish","fishing","fisheries","fishery","fisherman","fishermen",
    "trawl","tuna","shrimp","squid","lobster","octopus","iuu","aquaculture",
    "mariculture","iotc","catch","harvest of fish","pelagic","demersal",
    "pecheur","pecheurs","halieutique","chalut","chalutier","thon","thonier",
    "crevette","calamar","langouste","poulpe","ctoi","senneur","palangrier",
    "navire de peche","poisson","poissons","boutre","peche illegale",
    "peche illicite","surpeche","peche maritime","peche industrielle",
    "peche artisanale","peche cotiere","peche hauturiere",
]

def est_bruit_fruit(texte):
    """
    True si l'article est vraisemblablement un faux positif « pêche = fruit ».
    Condition stricte : contexte fruitier présent ET aucun terme halieutique
    réel. Si un seul vrai terme pêche/poisson est là, on garde (return False).
    """
    if not contient_un(texte, CONTEXTE_FRUIT):
        return False
    if contient_un_mot(texte, PECHE_NON_AMBIGU):
        return False
    return True

def nettoyer(texte, max_len=None):
    if not texte: return ""
    texte = re.sub(r'[\r\n\t]', ' ', str(texte))
    texte = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', texte).strip()
    if max_len and len(texte) > max_len:
        texte = texte[:max_len].rsplit(" ", 1)[0] + "..."
    return texte

#  FILTRAGE DIFFÉRENCIÉ PAR CATÉGORIE

def score_pertinence(art):
    """
    Score de pertinence pour TRIER les articles (le plus pertinent en haut).
    Plus le score est élevé, plus l'article colle au sujet conflits+pêche+SWIO.
    Permet l'écrémage manuel : tu gardes le haut de la liste.
    """
    titre = (art.get("titre") or "").lower()
    abstr = (art.get("abstract") or "").lower()
    texte = titre + " " + abstr
    score = 0

    def compte(texte, termes):
        return sum(1 for t in termes if t.lower() in texte)

    # CONFLIT STRICT : cœur du sujet → poids très fort
    score += 5 * compte(texte, BLOC_CONFLIT_STRICT)
    # Un terme de conflit DANS LE TITRE = très pertinent → gros bonus
    score += 6 * compte(titre, BLOC_CONFLIT_STRICT)
    # Gouvernance générique : contexte utile mais secondaire → poids faible
    score += 1 * compte(texte, BLOC_CONFLIT_GOUVERNANCE)
    # Termes de pêche : pertinence thématique
    score += 2 * compte(texte, BLOC_PECHE)
    score += 3 * compte(titre, BLOC_PECHE)
    # Zone forte (SWIO explicite) → bonus
    score += 2 * compte(texte, FILTRES_ZONE_FORTS)
    # Abstract présent et riche = article exploitable → léger bonus
    if len(abstr) > 300:
        score += 2
    elif len(abstr) > 100:
        score += 1
    return score


def filtrer_science(articles, filtres_zone):
    """
    SCIENCE : filtre strict.
    Conditions : zone + pêche + (conflit OU gouvernance).
    Sans abstract : zone + pêche sur le titre suffisent.
    Exclusion renforcée : si le titre contient un terme hors-SWIO
    (Méditerranée, Corée, Atlantique...) sans aucun terme SWIO → exclure.
    Les retenus sont TRIÉS par pertinence décroissante (écrémage manuel facile).
    """
    retenus = []
    for art in articles:
        titre = art.get("titre") or ""
        abstr = art.get("abstract") or ""
        texte = titre + " " + abstr
        titre_norm = normaliser(titre)

        if len(texte.strip()) < 10:
            retenus.append(art); continue

        # Exclusion renforcée sur le titre : hors-SWIO sans terme SWIO dans le titre
        if (contient_un(titre_norm, EXCLUSIONS_TITRE_SCIENCE)
                and not contient_un(titre_norm, SAUVE_EXCLUSION)):
            continue

        # Sans abstract : critères assouplis sur le titre seul
        # (mais le CONFLIT reste exigé : c'est le cœur du sujet)
        if not abstr.strip():
            if (contient_un(titre, filtres_zone)
                    and contient_un(titre, BLOC_PECHE)
                    and contient_un(titre, BLOC_CONFLIT_STRICT)):
                retenus.append(art)
            continue

        if not contient_un(texte, filtres_zone): continue
        if (contient_un(texte, EXCLUSIONS_HORS_ZONE)
                and not contient_un(texte, FILTRES_ZONE_FORTS)): continue
        if not contient_un(texte, BLOC_PECHE): continue
        # CONFLIT OBLIGATOIRE : la gouvernance seule ne suffit plus.
        # Un article de pure gestion halieutique (sans conflit) est exclu.
        if not contient_un(texte, BLOC_CONFLIT_STRICT): continue
        retenus.append(art)

    # Tri par pertinence décroissante + plafond MAX_SCIENCE
    retenus.sort(key=score_pertinence, reverse=True)
    # Diagnostic : combien passent VRAIMENT le filtre (avant le plafond)
    if len(retenus) > MAX_SCIENCE:
        print(f"  [Filtre science] {len(retenus)} articles passent le filtre "
              f"→ plafonnés à {MAX_SCIENCE} (les plus pertinents par score)")
    else:
        print(f"  [Filtre science] {len(retenus)} articles passent le filtre "
              f"(sous le plafond de {MAX_SCIENCE})")
    return retenus[:MAX_SCIENCE]


def filtrer_juridique(docs, filtres_zone):
    """
    JURIDIQUE : filtre DURCI v15.
    Exige que le terme pêche/fisheries soit dans le TITRE (pas seulement le texte).
    Élimine les faux positifs comme les CDN climatiques qui mentionnent
    accessoirement la pêche dans leur corps de texte.
    Conditions : pêche dans TITRE + zone dans titre OU abstract.
    """
    PECHE_TITRE = [
        "fish","fishing","fisheries","fishery","fisherman","fishermen",
        "trawl","tuna","shrimp","squid","lobster","octopus",
        "iuu","aquaculture","mariculture","iotc","ctoi","catch",
        "pêche","peche","pêcheur","pecheur","halieutique","chalut","thon",
        "crevette","calamar","langouste","poulpe","ressource halieutique",
        "maritime","marine resource","eez","zee","unclos","cnudm",
        "fishing agreement","accord de pêche","fishing act","fisheries act",
        "fishing law","droit maritime","mpa","rfmo",
    ]
    retenus = []
    for doc in docs:
        titre  = doc.get("titre") or ""
        abstr  = doc.get("abstract") or ""
        kw     = doc.get("keywords") or ""
        note   = doc.get("note") or ""
        source = (doc.get("source_db") or "").upper()
        texte_complet = " ".join(filter(None, [titre, abstr, kw, note]))

        # Sources juridiques fiables par nature (tribunaux du droit de la mer /
        # organe de gestion de l'océan Indien) : on NE leur impose PAS le filtre
        # pêche, car leurs titres sont des noms de navires/d'affaires ou de
        # rapports de session. Le contenu (abstract/keywords) porte le sujet.
        SOURCES_FIABLES = ("ITLOS", "CIJ", "ICJ", "CTOI", "IOTC")
        est_source_fiable = any(s in source for s in SOURCES_FIABLES)

        # Pêche/maritime OBLIGATOIRE — désormais cherchée dans TITRE + ABSTRACT
        # + KEYWORDS (et non plus le titre seul). Récupère les affaires dont le
        # titre est un nom de navire ("Monte Confurco") mais dont le résumé
        # parle clairement de pêche.
        texte_peche = " ".join(filter(None, [titre, abstr, kw]))
        if not est_source_fiable and not contient_un(texte_peche, PECHE_TITRE):
            continue

        # Zone obligatoire (titre OU champs combinés)
        if not contient_un(texte_complet, filtres_zone):
            continue

        # Exclusion hors-zone sauf si terme fort présent
        if (contient_un(texte_complet, EXCLUSIONS_HORS_ZONE)
                and not contient_un(texte_complet, FILTRES_ZONE_FORTS)):
            continue

        retenus.append(doc)
    return retenus[:MAX_JURIDIQUE]


def filtrer_allafrica(articles, filtres_zone):
    """
    ALLAFRICA : filtre dédié, légèrement plus souple.
    Le contenu complet est disponible (étage 2), mais on n'exige PAS
    le terme de conflit — stratégie exhaustive : zone + pêche suffisent.
    Le tri fin (conflit ou non) se fait ensuite manuellement dans Zotero.
    """
    pays_swio = [
        "kenya","madagascar","mozambique","tanzania","tanzanie","seychelles",
        "mauritius","île maurice","ile maurice","comoros","comores",
        "mayotte","reunion","réunion","somalia","somalie","south africa",
        "afrique du sud","zanzibar","mombasa","dar es salaam","lamu",
        "tromelin","glorieuses","swio","iotc","ctoi","indian ocean","océan indien"
    ]
    retenus = []
    for art in articles:
        # Filtre date : on bloque l'actualité au-delà de ANNEE_MAX (ex. 2026).
        # AllAfrica étant un fil d'actu, il renvoie spontanément du récent.
        # Si l'année n'a pas pu être lue (None), on garde l'article par défaut
        # (ne pas pénaliser un article dont la date est juste illisible).
        an = art.get("annee")
        if an and (an < ANNEE_MIN or an > ANNEE_MAX):
            continue
        texte = (art.get("titre") or "") + " " + (art.get("abstract") or "")
        if not contient_un(texte, pays_swio): continue
        if (contient_un(texte, EXCLUSIONS_HORS_ZONE)
                and not contient_un(texte, FILTRES_ZONE_FORTS)): continue
        if not contient_un(texte, BLOC_PECHE): continue
        # Garde-fou faux ami « pêche = fruit » (voir est_bruit_fruit)
        if est_bruit_fruit(texte): continue
        # MILIEU MARIN OBLIGATOIRE : on exige explicitement un terme marin.
        # Correspondance MOT ENTIER pour éviter les faux positifs
        # ('sea' dans 'disease', 'mer' dans 'commerce'...).
        if not contient_un_mot(texte, BLOC_MARIN): continue
        # Exclusion eau douce PRIORITAIRE : un lac/fleuve nommé l'emporte,
        # même si un terme marin générique traîne dans le texte.
        if contient_un_mot(texte, EXCLUSIONS_EAU_DOUCE): continue
        # CONFLIT OBLIGATOIRE (choix strict) : la presse ne doit garder que
        # les articles porteurs d'une dimension de conflit, pas la simple
        # actualité halieutique. Cohérent avec le filtre scientifique.
        if not contient_un(texte, BLOC_CONFLIT_PRESSE): continue
        retenus.append(art)
    return retenus[:MAX_PRESSE]


#  EUROPRESSE — LECTURE D'UN EXPORT RIS LOCAL (Le Monde, AFP, etc.)
#  Déposez le fichier .ris exporté d'Europresse à côté du script.
#  Détecté automatiquement (n'importe quel .ris dont le nom commence
#  par un chiffre, type 20260610041538.ris, ou nommé europresse.ris).
#  Les articles sont ensuite filtrés par filtrer_presse() comme les
#  autres sources de presse (zone + pêche + conflit + marin + date).
EUROPRESSE_FICHIERS = [
    "europresse.ris",
]

def _trouver_fichier_europresse():
    """
    Cherche UNIQUEMENT le fichier 'europresse.ris' à côté du script.
    Renvoie son chemin, ou None s'il est absent.
    (Aucun repli : on ne ramasse plus n'importe quel .ris du dossier,
     pour éviter de lire le mauvais fichier quand plusieurs .ris coexistent.)
    """
    for nom in EUROPRESSE_FICHIERS:
        cand = os.path.join(SCRIPT_DIR, nom)
        if os.path.exists(cand):
            return cand
    return None

def collecter_europresse_ris():
    """
    Parse l'export RIS d'Europresse. Champs utilisés :
        TI / T1 = titre | N2 = extrait/texte | JF = source/journal
        PY = année | DA = date complète | UR = lien | AU = auteur
    Renvoie une liste d'articles au format interne du script.
    """
    chemin = _trouver_fichier_europresse()
    if not chemin:
        print("  [Europresse] Aucun fichier .ris trouvé (dépose l'export à côté du script)")
        return []

    print(f"  [Europresse] Export RIS détecté : {os.path.basename(chemin)}")

    # Lecture tolérante à l'encodage
    contenu = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with open(chemin, encoding=enc) as f:
                contenu = f.read()
            break
        except Exception:
            continue
    if contenu is None:
        print("  [Europresse] ⚠ lecture impossible")
        return []

    articles, courant = [], {}

    def _flush():
        if not courant:
            return
        titre = courant.get("TI") or courant.get("T1") or ""
        if not titre.strip():
            return
        # année : PY en priorité, sinon les 4 chiffres de DA
        annee = None
        if courant.get("PY"):
            m = re.search(r"\d{4}", courant["PY"])
            if m: annee = int(m.group())
        if annee is None and courant.get("DA"):
            m = re.search(r"\d{4}", courant["DA"])
            if m: annee = int(m.group())
        articles.append({
            "titre":    nettoyer(titre),
            "abstract": nettoyer(courant.get("N2", "")),
            "annee":    annee,
            "source":   courant.get("JF", "Europresse").strip() or "Europresse",
            "url":      courant.get("UR", "").strip(),
            "doi":      "",
            "origine":  "Europresse",
        })

    for ligne in contenu.splitlines():
        # Une entrée RIS = "XX  - valeur" ; "ER" termine la référence
        m = re.match(r"^([A-Z][A-Z0-9])  - ?(.*)$", ligne)
        if not m:
            # ligne de continuation (texte qui déborde) → on l'ajoute au dernier champ
            if courant.get("_last") and ligne.strip():
                k = courant["_last"]
                courant[k] = (courant.get(k, "") + " " + ligne.strip()).strip()
            continue
        tag, val = m.group(1), m.group(2).strip()
        if tag == "ER":
            courant.pop("_last", None)
            _flush()
            courant = {}
        elif tag == "TY":
            # nouvelle référence si on en avait déjà une en cours sans ER
            if courant:
                courant.pop("_last", None); _flush(); courant = {}
            courant["_last"] = "TY"; courant["TY"] = val
        else:
            # on garde la première occurrence utile, on concatène N2 si répété
            if tag in courant and tag == "N2":
                courant[tag] += " " + val
            else:
                courant[tag] = val
            courant["_last"] = tag
    # dernière référence si le fichier ne finit pas par ER
    courant.pop("_last", None); _flush()

    print(f"  [Europresse] {len(articles)} références lues dans le fichier")
    return articles


def filtrer_presse(articles, filtres_zone):
    """
    PRESSE : filtre adapté avec bloc conflit RESSERRÉ.
    Zone : pays SWIO suffisent (sans exiger "Indian Ocean").
    Conflit : bloc resserré — pas de termes génériques (pressure, community...).
    Couvre : conflits usage, souveraineté, IUU, social (grèves, arrestations).
    """
    pays_swio = [
        "kenya","madagascar","mozambique","tanzania","tanzanie","seychelles",
        "mauritius","île maurice","ile maurice","comoros","comores",
        "mayotte","reunion","réunion","somalia","somalie","south africa",
        "afrique du sud","zanzibar","mombasa","dar es salaam","lamu",
        "tromelin","glorieuses","swio","iotc","ctoi","indian ocean","océan indien"
    ]

    retenus = []
    for art in articles:
        titre = art.get("titre") or ""
        abstr = art.get("abstract") or ""
        texte = titre + " " + abstr

        # Zone : pays SWIO suffisent
        if not contient_un(texte, pays_swio): continue

        # Exclusion hors-zone
        if (contient_un(texte, EXCLUSIONS_HORS_ZONE)
                and not contient_un(texte, FILTRES_ZONE_FORTS)): continue

        # Pêche obligatoire
        if not contient_un(texte, BLOC_PECHE): continue

        # Garde-fou faux ami « pêche = fruit » (voir est_bruit_fruit)
        if est_bruit_fruit(texte): continue

        # Conflit resserré — termes vraiment liés à un conflit
        if not contient_un(texte, BLOC_CONFLIT_PRESSE): continue

        retenus.append(art)
    return retenus[:MAX_PRESSE]


def filtrer_europresse(articles, filtres_zone):
    """
    EUROPRESSE : filtre ASSOUPLI, dédié à l'export RIS local (Le Monde, AFP…).

    POURQUOI un filtre à part : Europresse n'exporte qu'un EXTRAIT tronqué de
    l'article (champ N2 ≈ 180 caractères, finissant par « ... »). Exiger en plus
    le milieu marin (mot entier) + l'absence d'eau douce + un terme de conflit
    — comme le fait filtrer_allafrica() — élimine la quasi-totalité du corpus
    alors que les articles sont pertinents : le texte qui porterait ces termes
    est simplement coupé.

    RÈGLE retenue (exhaustive, tri fin ensuite dans Zotero) :
        zone SWIO  +  pêche                          → on garde
    Le terme de conflit n'est PAS exigé, mais on l'ANNOTE dans la note RIS
    ([conflit:oui/non]) pour que tu puisses trier/écrémer rapidement.
    """
    pays_swio = [
        "kenya","madagascar","mozambique","tanzania","tanzanie","seychelles",
        "mauritius","île maurice","ile maurice","comoros","comores",
        "mayotte","reunion","réunion","somalia","somalie","south africa",
        "afrique du sud","zanzibar","mombasa","dar es salaam","lamu",
        "tromelin","glorieuses","juan de nova","europa","crozet","kerguelen",
        "taaf","terres australes","chagos","swio","iotc","ctoi",
        "indian ocean","océan indien","ocean indien","canal du mozambique",
    ]

    retenus = []
    for art in articles:
        titre = art.get("titre") or ""
        abstr = art.get("abstract") or ""
        texte = titre + " " + abstr

        # Zone obligatoire (pays SWIO suffisent)
        if not contient_un(texte, pays_swio): continue

        # Exclusion hors-zone (sauf terme zone fort)
        if (contient_un(texte, EXCLUSIONS_HORS_ZONE)
                and not contient_un(texte, FILTRES_ZONE_FORTS)): continue

        # Pêche obligatoire
        if not contient_un(texte, BLOC_PECHE): continue

        # Garde-fou faux ami « pêche = fruit » (voir est_bruit_fruit)
        if est_bruit_fruit(texte): continue

        # Conflit NON exigé (abstract tronqué) — mais annoté pour ton tri Zotero
        a_conflit = contient_un(texte, BLOC_CONFLIT_PRESSE)
        note_prefix = "[conflit:oui] " if a_conflit else "[conflit:non] "
        art["note"] = note_prefix + (art.get("note") or "")

        retenus.append(art)

    # Tri : les articles avec marqueur de conflit d'abord (plus pertinents)
    retenus.sort(key=lambda a: 0 if (a.get("note") or "").startswith("[conflit:oui]") else 1)

    nb_conflit = sum(1 for a in retenus if (a.get("note") or "").startswith("[conflit:oui]"))
    print(f"  [Filtre Europresse] {len(retenus)} retenus "
          f"(dont {nb_conflit} avec marqueur de conflit, triés en tête)")
    return retenus[:MAX_PRESSE]

#  DÉDOUBLONNAGE 3 NIVEAUX

def dedoublonner(articles):
    """D1 — intra-base : DOI exact + titre normalisé (80 chars)."""
    vus_doi, vus_titre, uniques = set(), set(), []
    for art in articles:
        doi   = (art.get("doi") or "").strip().lower()
        titre = normaliser(art.get("titre") or "")[:80]
        if doi and doi in vus_doi: continue
        if titre and titre in vus_titre: continue
        if doi:   vus_doi.add(doi)
        if titre: vus_titre.add(titre)
        uniques.append(art)
    return uniques


def dedoublonner_inter(listes):
    """D2 — inter-catégories : DOI exact + titre normalisé."""
    vus_doi, vus_titre, uniques = set(), set(), []
    for lst in listes:
        for art in lst:
            doi   = (art.get("doi") or "").strip().lower()
            titre = normaliser(art.get("titre") or "")[:80]
            if doi and doi in vus_doi: continue
            if titre and titre in vus_titre: continue
            if doi:   vus_doi.add(doi)
            if titre: vus_titre.add(titre)
            uniques.append(art)
    return uniques


def _jaccard(t1, t2):
    s1 = set(normaliser(t1).split())
    s2 = set(normaliser(t2).split())
    if not s1 or not s2: return 0.0
    return len(s1 & s2) / len(s1 | s2)


def dedoublonner_final(articles, seuil=0.82):
    """
    D3 — final approfondi (Jaccard ≥ seuil sur les tokens du titre).
    Détecte les quasi-doublons inter-bases.
    Conserve la référence la plus complète (DOI > abstract long > URL).
    """
    def score(a):
        s = 0
        if a.get("doi"): s += 4
        if len(a.get("abstract") or "") > 50: s += 2
        if a.get("url"): s += 1
        return s

    def meilleur(a, b): return a if score(a) >= score(b) else b

    vus_doi, vus_titre = {}, {}
    titres_l, uniques  = [], []
    nb_jac = 0

    for art in articles:
        doi    = (art.get("doi") or "").strip().lower()
        titre_c = normaliser(art.get("titre") or "")[:80]
        titre_l = normaliser(art.get("titre") or "")

        if doi:
            if doi in vus_doi:
                idx = vus_doi[doi]; uniques[idx] = meilleur(uniques[idx], art); continue
            vus_doi[doi] = len(uniques)
        if titre_c:
            if titre_c in vus_titre:
                idx = vus_titre[titre_c]; uniques[idx] = meilleur(uniques[idx], art); continue
            vus_titre[titre_c] = len(uniques)

        doublon = False
        if len(titre_l.split()) >= 5:
            for t_ref, idx in titres_l:
                if _jaccard(titre_l, t_ref) >= seuil:
                    uniques[idx] = meilleur(uniques[idx], art)
                    doublon = True; nb_jac += 1; break

        if not doublon:
            titres_l.append((titre_l, len(uniques)))
            uniques.append(art)

    print(f"  [D3-Jaccard] {len(articles)} → {len(uniques)} "
          f"({len(articles)-len(uniques)} doublons, {nb_jac} par similarité titre)")
    return uniques

#  EXPORT RIS

TYPE_RIS = {
    "article":"JOUR","journal-article":"JOUR","book-chapter":"CHAP",
    "report":"RPRT","preprint":"UNPB","dissertation":"THES",
    "working-paper":"UNPB","book":"BOOK","thesis":"THES",
    "legislation":"STAT","regulation":"STAT","policy":"RPRT",
    "legal":"STAT","treaty":"STAT","decision":"RPRT","directive":"STAT",
    "news":"NEWS","newspaper":"NEWS",
}

def exporter_ris(articles, chemin):
    with open(chemin, "w", encoding="utf-8-sig") as f:
        for art in articles:
            type_ris = TYPE_RIS.get((art.get("type_doc") or "").lower(), "JOUR")
            f.write(f"TY  - {type_ris}\n")
            if art.get("titre"):   f.write(f"TI  - {nettoyer(art['titre'])}\n")
            for au in (art.get("auteurs") or "").split(", "):
                au = nettoyer(au)
                if au and au not in ("et al.","?"): f.write(f"AU  - {au}\n")
            if art.get("annee"):   f.write(f"PY  - {art['annee']}\n")
            if art.get("revue"):   f.write(f"JO  - {nettoyer(art['revue'])}\n")
            if art.get("abstract"):f.write(f"AB  - {nettoyer(art['abstract'], 2000)}\n")
            if art.get("doi"):     f.write(f"DO  - {nettoyer(art['doi'])}\n")
            if art.get("url"):     f.write(f"UR  - {nettoyer(art['url'])}\n")
            if art.get("note"):    f.write(f"N1  - {nettoyer(art['note'])} [score:{score_pertinence(art)}]\n")
            if not art.get("note"): f.write(f"N1  - [score:{score_pertinence(art)}]\n")
            for kw in (art.get("keywords") or "").split(";"):
                kw = kw.strip()
                if kw: f.write(f"KW  - {nettoyer(kw)}\n")
            f.write(f"DB  - {art.get('source_db','')}\n")
            f.write("ER  - \n\n")
    print(f"✅ Export RIS : {os.path.basename(chemin)} ({len(articles)} références)")

#  BASES SCIENTIFIQUES

def reconstruire_abstract(aii):
    if not aii: return ""
    try:
        pts = [(pos, mot) for mot, positions in aii.items() for pos in positions]
        pts.sort(key=lambda x: x[0])
        return " ".join(m for _, m in pts)
    except: return ""


def collecter_openalex(requete, nb_max, a_min, a_max):
    articles, page, pp = [], 1, 50
    print(f"  [OpenAlex] '{requete}'")
    essais_429 = 0
    while len(articles) < nb_max:
        url = (f"https://api.openalex.org/works?search={requests.utils.quote(requete)}"
               f"&per-page={pp}&page={page}&filter=publication_year:{a_min}-{a_max}"
               f"&sort=cited_by_count:desc"
               f"&select=title,authorships,publication_year,primary_location,"
               f"abstract_inverted_index,doi,open_access,type&mailto=20240905@webmail.universita.corsica")
        try:
            r = requests.get(url, timeout=20)
            if r.status_code == 429:
                # rate limit : on attend (de plus en plus longtemps) et on réessaie
                essais_429 += 1
                if essais_429 > 4:
                    print(f"  [OpenAlex] 429 persistant après 4 essais — on arrête cette zone")
                    break
                attente = 5 * essais_429   # 5s, 10s, 15s, 20s
                print(f"  [OpenAlex] HTTP 429 (rate limit) — pause {attente}s et réessai")
                time.sleep(attente)
                continue   # on refait la MÊME page, sans incrémenter
            if r.status_code != 200: print(f"  [OpenAlex] HTTP {r.status_code}"); break
            essais_429 = 0   # succès : on réinitialise le compteur
            res = r.json().get("results", [])
            if not res: break
            for it in res:
                doi = it.get("doi") or ""
                auteurs_raw = it.get("authorships", [])[:3]
                auteurs = ", ".join(a.get("author",{}).get("display_name","?") for a in auteurs_raw)
                if len(it.get("authorships",[])) > 3: auteurs += " et al."
                oa = it.get("open_access") or {}
                loc = it.get("primary_location") or {}
                articles.append({
                    "source_db":"OpenAlex","titre":it.get("title") or "",
                    "abstract":reconstruire_abstract(it.get("abstract_inverted_index")),
                    "auteurs":auteurs,"annee":it.get("publication_year"),
                    "revue":(loc.get("source") or {}).get("display_name") or "",
                    "doi":doi,"url":oa.get("oa_url") or (f"https://doi.org/{doi}" if doi else ""),
                    "type_doc":it.get("type") or "article"
                })
            print(f"  [OpenAlex] p{page} → {len(res)} | total:{len(articles)}")
            if len(res) < pp: break
            page += 1; time.sleep(1.0)   # pause portée à 1s (était 0.3s) : évite le 429
        except Exception as e: print(f"  [OpenAlex] {e}"); break
    return articles


def collecter_crossref(requete, nb_max, a_min, a_max):
    articles, offset, pp = [], 0, 50
    print(f"  [Crossref] '{requete}'")
    filtre = f"&filter=from-pub-date:{a_min},until-pub-date:{a_max}"
    while len(articles) < nb_max:
        url = (f"https://api.crossref.org/works?query={requests.utils.quote(requete)}"
               f"&rows={pp}&offset={offset}&sort=relevance&order=desc"
               f"&select=title,author,published,container-title,DOI,abstract,type,URL"
               f"{filtre}&mailto=20240905@webmail.universita.corsica")
        try:
            r = requests.get(url, timeout=20)
            if r.status_code != 200: print(f"  [Crossref] HTTP {r.status_code}"); break
            items = r.json().get("message",{}).get("items",[])
            if not items: break
            for it in items:
                titre_raw = it.get("title",[])
                auteurs_raw = it.get("author",[])[:3]
                auteurs = ", ".join(f"{a.get('family','?')}, {a.get('given','')[:1]}."
                                    for a in auteurs_raw)
                if len(it.get("author",[])) > 3: auteurs += " et al."
                pub = it.get("published") or it.get("published-print") or {}
                pts = pub.get("date-parts",[[None]])
                annee = pts[0][0] if pts and pts[0] else None
                revue_raw = it.get("container-title",[])
                doi = it.get("DOI") or ""
                articles.append({
                    "source_db":"Crossref","titre":titre_raw[0] if titre_raw else "",
                    "abstract":re.sub(r"<[^>]+>","",it.get("abstract") or ""),
                    "auteurs":auteurs,"annee":annee,
                    "revue":revue_raw[0] if revue_raw else "",
                    "doi":doi,"url":it.get("URL") or (f"https://doi.org/{doi}" if doi else ""),
                    "type_doc":it.get("type") or "journal-article"
                })
            print(f"  [Crossref] offset:{offset} → {len(items)} | total:{len(articles)}")
            if len(items) < pp: break
            offset += pp; time.sleep(0.3)
        except Exception as e: print(f"  [Crossref] {e}"); break
    return articles


def collecter_hal(requete, nb_max, a_min, a_max):
    articles, start, pp = [], 0, 50
    print(f"  [HAL] '{requete}'")
    fl = "title_s,authFullName_s,producedDateY_i,journalTitle_s,abstract_s,doiId_s,uri_s,docType_s"
    fq = f"producedDateY_i:[{a_min} TO {a_max}]"
    type_map = {"ART":"journal-article","COMM":"article","OUV":"book","COUV":"book-chapter",
                "REPORT":"report","THESE":"dissertation","HDR":"dissertation","PREPRINT":"preprint"}
    while len(articles) < nb_max:
        url = (f"https://api.archives-ouvertes.fr/search/?q={requests.utils.quote(requete)}"
               f"&fl={requests.utils.quote(fl)}&fq={requests.utils.quote(fq)}"
               f"&sort={requests.utils.quote('producedDateY_i desc')}&rows={pp}&start={start}&wt=json")
        try:
            r = requests.get(url, timeout=20)
            if r.status_code != 200: print(f"  [HAL] HTTP {r.status_code}"); break
            docs = r.json().get("response",{}).get("docs",[])
            if not docs: break
            for it in docs:
                def lst(k):
                    v = it.get(k,[])
                    return (v[0] if isinstance(v,list) and v else v) or ""
                auteurs_raw = it.get("authFullName_s",[])[:3]
                auteurs = ", ".join(auteurs_raw)
                if len(it.get("authFullName_s",[])) > 3: auteurs += " et al."
                doi = it.get("doiId_s","")
                articles.append({
                    "source_db":"HAL","titre":lst("title_s"),"abstract":lst("abstract_s"),
                    "auteurs":auteurs,"annee":it.get("producedDateY_i"),
                    "revue":it.get("journalTitle_s",""),"doi":doi,
                    "url":it.get("uri_s","") or (f"https://doi.org/{doi}" if doi else ""),
                    "type_doc":type_map.get(it.get("docType_s","ART"),"article")
                })
            print(f"  [HAL] start:{start} → {len(docs)} | total:{len(articles)}")
            if len(docs) < pp: break
            start += pp; time.sleep(0.3)
        except Exception as e: print(f"  [HAL] {e}"); break
    return articles


def enrichir_abstracts(articles):
    a_enrich = [a for a in articles
                if a["source_db"]=="Crossref" and not a["abstract"].strip() and a["doi"].strip()]
    if not a_enrich: print("  [Enrichissement] Rien à enrichir."); return
    print(f"  [Enrichissement] {len(a_enrich)} articles Crossref sans abstract → OpenAlex")
    enrichis = 0
    for i, art in enumerate(a_enrich, 1):
        enc = requests.utils.quote(f"https://doi.org/{art['doi']}", safe="")
        try:
            r = requests.get(f"https://api.openalex.org/works/{enc}"
                             f"?select=abstract_inverted_index&mailto=20240905@webmail.universita.corsica",
                             timeout=15)
            if r.status_code == 429:
                time.sleep(5)   # rate limit : on souffle puis on réessaie une fois
                r = requests.get(f"https://api.openalex.org/works/{enc}"
                                 f"?select=abstract_inverted_index&mailto=20240905@webmail.universita.corsica",
                                 timeout=15)
            if r.status_code == 200:
                ab = reconstruire_abstract(r.json().get("abstract_inverted_index"))
                if ab.strip(): art["abstract"] = ab; enrichis += 1
            time.sleep(0.5)   # pause portée à 0.5s (était 0.2s)
        except: pass
        if i % 20 == 0 or i == len(a_enrich):
            print(f"  [Enrichissement] {i}/{len(a_enrich)} — {enrichis} récupérés")

#  BASES JURIDIQUES

def collecter_itlos():
    """
    ITLOS — Tribunal International du Droit de la Mer.
    33 affaires au total depuis 1997. On encode les affaires pertinentes
    pêche/maritime en données statiques (fiables) + scraping de la liste
    pour capturer les nouvelles affaires.
    URL directe : https://www.itlos.org/en/main/cases/list-of-cases/case-no-XX/
    """
    # Affaires ITLOS directement pertinentes pêche/ZEE/SWIO — données statiques
    ITLOS_STATIQUES = [
        {"no":1,  "titre":"The M/V 'SAIGA' Case (Saint Vincent and the Grenadines v. Guinea) — Prompt Release",
         "annee":1997, "abstract":"Arraisonnement d'un navire citerne approvisionnant des thoniers dans la ZEE guinéenne. Prompt release, droit de poursuite, pêche en haute mer.",
         "keywords":"navire;arraisonnement;ZEE;prompt release;pêche;Atlantique"},
        {"no":2,  "titre":"The M/V 'SAIGA' (No.2) Case (Saint Vincent and the Grenadines v. Guinea)",
         "annee":1999, "abstract":"Fond de l'affaire SAIGA : usage excessif de la force lors de l'arraisonnement, droits de l'État du pavillon, pêche en ZEE.",
         "keywords":"navire;arraisonnement;force;ZEE;droit maritime"},
        {"no":3,  "titre":"Southern Bluefin Tuna Cases (New Zealand v. Japan; Australia v. Japan) — Provisional Measures",
         "annee":1999, "abstract":"Mesures conservatoires pour le thon rouge du sud — surpêche, conservation des espèces, droit international de la pêche. Océan Indien concerné.",
         "keywords":"thon;surpêche;mesures conservatoires;océan indien;pêche"},
        {"no":5,  "titre":"The 'Camouco' Case (Panama v. France) — Prompt Release",
         "annee":2000, "abstract":"Arraisonnement du Camouco pour pêche illégale dans la ZEE des Terres australes françaises (TAAF) — zones SWIO. Cautionnement, prompt release.",
         "keywords":"TAAF;ZEE;pêche illégale;arraisonnement;prompt release;SWIO;terres australes"},
        {"no":6,  "titre":"The 'Monte Confurco' Case (Seychelles v. France) — Prompt Release",
         "annee":2000, "abstract":"Navire seychellois arraisonné pour pêche illégale dans la ZEE des Kerguelen (TAAF). Prompt release. Directement lié à la pêche dans les zones SWIO françaises.",
         "keywords":"Seychelles;France;TAAF;Kerguelen;ZEE;pêche illégale;prompt release;SWIO"},
        {"no":7,  "titre":"Case concerning the Conservation and Sustainable Exploitation of Swordfish Stocks (Chile/European Community)",
         "annee":2000, "abstract":"Gestion durable des stocks d'espadon, accès aux ports, mesures de conservation. Précédent important pour les conflits de pêche SWIO.",
         "keywords":"espadon;stocks;pêche durable;accords;ports;pêche"},
        {"no":8,  "titre":"The 'Grand Prince' Case (Belize v. France) — Prompt Release",
         "annee":2001, "abstract":"Navire arraisonné pour pêche illégale dans la ZEE des Kerguelen (TAAF). Prompt release. Zone SWIO.",
         "keywords":"TAAF;Kerguelen;ZEE;pêche illégale;prompt release;SWIO"},
        {"no":11, "titre":"The 'Volga' Case (Russian Federation v. Australia) — Prompt Release",
         "annee":2002, "abstract":"Navire russe arraisonné pour pêche illégale de légine dans les eaux australiennes (océan Indien austral). Cautionnement, prompt release.",
         "keywords":"légine;pêche illégale;océan indien;arraisonnement;prompt release;Australie"},
        {"no":13, "titre":"Volga Case — Prompt Release (Russian Federation v. Australia)",
         "annee":2002, "abstract":"Pêche illégale de légine australe dans l'océan Indien. Prompt release.",
         "keywords":"légine;pêche illégale;océan indien austral;prompt release"},
        {"no":14, "titre":"The 'Juno Trader' Case (Saint Vincent and the Grenadines v. Guinea-Bissau) — Prompt Release",
         "annee":2004, "abstract":"Arraisonnement pour pêche illégale, cautionnement raisonnable, prompt release.",
         "keywords":"pêche illégale;prompt release;arraisonnement;ZEE"},
        {"no":15, "titre":"The 'Hoshinmaru' Case (Japan v. Russian Federation) — Prompt Release",
         "annee":2005, "abstract":"Navire japonais arraisonné pour pêche illégale. Prompt release, cautionnement.",
         "keywords":"pêche illégale;prompt release;Japon;Russie"},
        {"no":21, "titre":"Request for Advisory Opinion submitted by the Sub-Regional Fisheries Commission (SRFC)",
         "annee":2015, "abstract":"Avis consultatif sur les obligations des États du pavillon pour la pêche IUU en ZEE d'Afrique de l'Ouest. Précédent majeur pour la pêche illégale en Afrique, directement applicable SWIO.",
         "keywords":"SRFC;pêche IUU;ZEE;Afrique;obligations État pavillon;avis consultatif;pêche illégale"},
        {"no":28, "titre":"Dispute concerning delimitation of the maritime boundary between Mauritius and Maldives in the Indian Ocean (Mauritius/Maldives)",
         "annee":2021, "abstract":"Délimitation de la frontière maritime entre Maurice et les Maldives dans l'océan Indien. Directement dans la zone SWIO.",
         "keywords":"Maurice;Maldives;océan indien;délimitation maritime;ZEE;SWIO"},
    ]

    articles = []
    BASE = "https://www.itlos.org"

    print("  [ITLOS] Chargement affaires statiques...")
    for af in ITLOS_STATIQUES:
        no  = af["no"]
        url = f"{BASE}/en/main/cases/list-of-cases/case-no-{no}/"
        articles.append({
            "source_db": "ITLOS",
            "titre":     f"Affaire ITLOS n°{no} — {af['titre']}",
            "abstract":  af["abstract"],
            "auteurs":   "ITLOS / Tribunal International du Droit de la Mer",
            "annee":     af["annee"],
            "revue":     "ITLOS Reports",
            "doi":       "",
            "url":       url,
            "type_doc":  "decision",
            "keywords":  af["keywords"],
            "note":      f"ITLOS|Case No.{no}|{url}",
        })

    # Scraping pour capturer les nouvelles affaires (no > 28)
    print("  [ITLOS] Scraping nouvelles affaires...")
    try:
        r = requests.get(f"{BASE}/en/main/cases/list-of-cases/",
                         timeout=20, headers={"User-Agent":"SWIO-Research/15.0"})
        if r.status_code == 200:
            # Chercher les liens case-no-XX
            nums = re.findall(r'/cases/list-of-cases/case-no-(\d+)', r.text)
            nums_vus = {af["no"] for af in ITLOS_STATIQUES}
            for n in set(int(x) for x in nums):
                if n in nums_vus: continue
                url_af = f"{BASE}/en/main/cases/list-of-cases/case-no-{n}/"
                try:
                    rf = requests.get(url_af, timeout=15,
                                      headers={"User-Agent":"SWIO-Research/15.0"})
                    titre = f"Affaire ITLOS n°{n}"
                    abstract = ""
                    annee = None
                    if rf.status_code == 200:
                        m_t = re.search(r'<h1[^>]*>(.*?)</h1>', rf.text, re.DOTALL)
                        if m_t:
                            titre = re.sub(r'<[^>]+>','',m_t.group(1)).strip()
                        m_d = re.search(r'(19|20)\d{2}', rf.text)
                        if m_d: annee = int(m_d.group(0))
                        m_p = re.search(r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
                                        rf.text, re.DOTALL|re.IGNORECASE)
                        if m_p:
                            abstract = re.sub(r'<[^>]+>',' ',m_p.group(1))
                            abstract = re.sub(r'\s+',' ',abstract).strip()[:400]
                    articles.append({
                        "source_db":"ITLOS","titre":titre,"abstract":abstract,
                        "auteurs":"ITLOS / TIDM","annee":annee,
                        "revue":"ITLOS Reports","doi":"","url":url_af,
                        "type_doc":"decision",
                        "keywords":"ITLOS;droit de la mer;ZEE;droit maritime",
                        "note":f"ITLOS|Case No.{n}|{url_af}",
                    })
                    time.sleep(0.5)
                except: pass
    except Exception as e:
        print(f"  [ITLOS] Scraping nouvelles affaires : {e}")

    print(f"  [ITLOS] Total : {len(articles)} affaires")
    return articles


def collecter_icj():
    """
    CIJ — affaires statiques sélectionnées.
    Inclut les 2 affaires SWIO clés identifiées :
    - Somalia v. Kenya (case 161) — délimitation OI, ressources halieutiques
    - Chagos/Maurice (case 169) — souveraineté maritime OI, droits de pêche
    URL directe : https://www.icj-cij.org/case/NNN
    """
    ICJ_STATIQUES = [
        {"no":1,
         "titre":"Fisheries Case (United Kingdom v. Norway)",
         "annee":1951,
         "abstract":"Délimitation des eaux territoriales et droits de pêche exclusifs. Arrêt fondateur du droit international de la pêche côtière et des lignes de base.",
         "keywords":"pêche;eaux territoriales;délimitation;droit maritime"},
        {"no":62,
         "titre":"Fisheries Jurisdiction (United Kingdom v. Iceland)",
         "annee":1974,
         "abstract":"Zone de pêche exclusive de l'Islande étendue à 50 milles. Droits préférentiels de pêche. Précédent fondamental ZEE et droit de la pêche.",
         "keywords":"pêche;ZEE;zone exclusive;droits préférentiels"},
        {"no":63,
         "titre":"Fisheries Jurisdiction (Germany v. Iceland)",
         "annee":1974,
         "abstract":"Droits préférentiels de pêche, négociations équitables entre États en matière de ressources halieutiques.",
         "keywords":"pêche;zone exclusive;droits préférentiels;droit maritime"},
        {"no":161,
         "titre":"Maritime Delimitation in the Indian Ocean (Somalia v. Kenya)",
         "annee":2021,
         "abstract":"Délimitation de la frontière maritime entre la Somalie et le Kenya dans l'océan Indien occidental. Accès aux ressources halieutiques, ZEE, plateau continental. Directement dans la zone SWIO — enjeux de pêche explicitement discutés dans l'arrêt.",
         "keywords":"Somalie;Kenya;océan indien;délimitation maritime;ZEE;pêche;SWIO;ressources halieutiques"},
        {"no":169,
         "titre":"Legal Consequences of the Separation of the Chagos Archipelago from Mauritius in 1965 (Advisory Opinion)",
         "annee":2019,
         "abstract":"Avis consultatif sur la décolonisation de l'archipel des Chagos et sa séparation de Maurice. Souveraineté maritime, droits de pêche dans l'océan Indien. Directement pertinent pour les droits maritimes de Maurice dans la zone SWIO.",
         "keywords":"Chagos;Maurice;Mauritius;océan indien;souveraineté maritime;droits de pêche;SWIO;décolonisation"},
    ]

    articles = []
    BASE = "https://www.icj-cij.org"
    print("  [ICJ] Chargement affaires statiques...")

    for af in ICJ_STATIQUES:
        url = f"{BASE}/case/{af['no']}"
        articles.append({
            "source_db": "CIJ/ICJ",
            "titre":     af["titre"],
            "abstract":  af["abstract"],
            "auteurs":   "Cour Internationale de Justice",
            "annee":     af["annee"],
            "revue":     "CIJ Recueil / ICJ Reports",
            "doi":       "",
            "url":       url,
            "type_doc":  "decision",
            "keywords":  af["keywords"],
            "note":      f"ICJ|Case No.{af['no']}|{url}",
        })

    # Scraping affaires maritimes en cours
    try:
        r = requests.get(f"{BASE}/en/pending-cases", timeout=20,
                         headers={"User-Agent":"SWIO-Research/15.0"})
        if r.status_code == 200:
            MOTS_MARITIME = ["fish","maritime","ocean","sea","eez","coast",
                             "pêche","mer","océan","delimitation","zone"]
            vus = {af["no"] for af in ICJ_STATIQUES}
            for no_str, titre_brut in re.findall(
                    r'href="/case/(\d+)"[^>]*>([^<]{10,200})', r.text):
                no = int(no_str)
                if no in vus: continue
                titre = re.sub(r'\s+', ' ', titre_brut).strip()
                if not contient_un(titre.lower(), MOTS_MARITIME): continue
                vus.add(no)
                articles.append({
                    "source_db":"CIJ/ICJ","titre":titre,
                    "abstract":"Affaire CIJ — droit maritime / pêche / ZEE",
                    "auteurs":"Cour Internationale de Justice",
                    "annee":None,"revue":"CIJ Recueil","doi":"",
                    "url":f"{BASE}/case/{no}","type_doc":"decision",
                    "keywords":"CIJ;droit maritime;pêche;ZEE",
                    "note":f"ICJ|Case No.{no}",
                })
    except Exception as e:
        print(f"  [ICJ] Scraping : {e}")

    print(f"  [ICJ] Total : {len(articles)} affaires")
    return articles


def collecter_ecolex_decisions():
    """
    ECOLEX — décisions de justice ciblées depuis les URLs de recherche filtrées :
    pays SWIO (Afrique du Sud, France, Madagascar, Seychelles, Kenya, Mozambique,
    Tanzanie) + requête pêche/fisheries.
    URL directe vers chaque fiche — pas de slug deviné, uniquement scraping réel.
    """
    articles = []
    vus_urls = set()
    print("  [ECOLEX] Collecte décisions ciblées par pays SWIO...")

    # URLs de recherche ECOLEX avec filtres pays/région précis
    URLS_RECHERCHE = [
        "https://www.ecolex.org/fr/result/?q=p%C3%AAche&type=court_decision&xcountry=Afrique+du+Sud&xregion=Afrique",
        "https://www.ecolex.org/fr/result/?q=p%C3%AAche&type=court_decision&xcountry=Madagascar&xregion=Afrique",
        "https://www.ecolex.org/fr/result/?q=fisheries&type=court_decision&xcountry=Seychelles",
        "https://www.ecolex.org/fr/result/?q=fishing&type=court_decision&xcountry=Seychelles",
        "https://www.ecolex.org/fr/result/?q=p%C3%AAche+maritime&type=court_decision&xcountry=France",
        "https://www.ecolex.org/fr/result/?q=fisheries&type=court_decision&xcountry=Kenya",
        "https://www.ecolex.org/fr/result/?q=fisheries&type=court_decision&xcountry=Mozambique",
        "https://www.ecolex.org/fr/result/?q=fisheries&type=court_decision&xcountry=Tanzania",
        "https://www.ecolex.org/result/?q=Indian+Ocean+fisheries&type=court_decision",
        "https://www.ecolex.org/result/?q=IUU+fishing+Indian+Ocean&type=court_decision",
        "https://www.ecolex.org/result/?q=IOTC+CTOI+fisheries&type=court_decision",
        "https://www.ecolex.org/result/?q=prompt+release+fishing&type=court_decision",
    ]

    for url_rech in URLS_RECHERCHE:
        try:
            r = requests.get(url_rech, timeout=20,
                             headers={"User-Agent":"SWIO-Research/15.0",
                                      "Accept-Language":"fr,en;q=0.9"})
            if r.status_code != 200:
                time.sleep(1.0); continue

            # Extraire tous les liens vers des fiches court-decision
            liens = re.findall(
                r'href="(https?://www\.ecolex\.org/(?:fr/|es/)?details/court-decision/[^"?]+)"',
                r.text
            )
            nb = 0
            for lien in set(liens):
                if lien in vus_urls: continue
                vus_urls.add(lien)
                nb += 1

                titre, abstract, annee, pays = "", "", None, ""
                try:
                    rf = requests.get(lien, timeout=15,
                                      headers={"User-Agent":"SWIO-Research/15.0"})
                    if rf.status_code == 200:
                        m_t = re.search(r'<h1[^>]*>(.*?)</h1>', rf.text, re.DOTALL)
                        if m_t: titre = re.sub(r'<[^>]+>','',m_t.group(1)).strip()
                        m_d = re.search(r'(19|20)\d{2}', rf.text)
                        if m_d: annee = int(m_d.group(0))
                        m_ab = re.search(
                            r'(?:abstract|description|résumé)[^>]*>(.*?)</(?:div|p)>',
                            rf.text, re.DOTALL|re.IGNORECASE)
                        if m_ab:
                            abstract = re.sub(r'<[^>]+>',' ',m_ab.group(1))
                            abstract = re.sub(r'\s+',' ',abstract).strip()[:500]
                        m_c = re.search(r'(?:Country|Pays)[^:]*:\s*([^\n<]{3,50})', rf.text)
                        if m_c: pays = m_c.group(1).strip()
                    time.sleep(0.4)
                except: pass

                if not titre:
                    titre = lien.rstrip("/").split("/")[-1].replace("-"," ")[:80]

                articles.append({
                    "source_db": "ECOLEX",
                    "titre":     titre,
                    "abstract":  abstract or "Décision de justice — droit de la pêche",
                    "auteurs":   pays or "Juridiction nationale/internationale",
                    "annee":     annee,
                    "revue":     "ECOLEX — FAO/IUCN/UNEP",
                    "doi":       "",
                    "url":       lien,
                    "type_doc":  "decision",
                    "keywords":  f"pêche;droit maritime;ECOLEX;{pays}",
                    "note":      f"ECOLEX|{pays}|{lien}",
                })

            print(f"  [ECOLEX] {url_rech[-55:]} → {nb} fiches")
            time.sleep(1.5)

        except Exception as e:
            print(f"  [ECOLEX] Erreur : {e}")

    print(f"  [ECOLEX] Total : {len(articles)} décisions")
    return articles

#  CTOI — PARSING DE L'EXPORT LOCAL (ctoi.txt)

# Noms de fichiers acceptés (cherchés à côté du script)
CTOI_FICHIERS_LOCAUX = [
    "ctoi.txt", "ctoi.tsv", "ctoi.csv",
    "iotc.txt", "iotc.tsv", "iotc.csv",
    "ctoi_documents.txt", "documents_ctoi.txt",
]

# ── RÈGLE DE TRI STRICTE (v17) ────────────────────────────────────────────
# Un document n'est retenu QUE si les DEUX conditions sont vraies :
#   (1) NATURE : c'est une décision/sanction/rapport IUU ou la mention d'un
#       problème de pêche d'un État (liste INN, infraction, non-conformité,
#       souveraineté, statut CNCP, cas de pêche illégale)…
#   (2) ZONE   : …ET un pays/territoire de la zone SWIO est explicitement nommé.
# Les listes INN globales, les courriers de pays tiers et les rapports ROP
# génériques (sans pays de zone) sont volontairement écartés.

# (2) Pays / territoires de la zone SWIO — au moins UN doit être nommé.
CTOI_PAYS_ZONE = [
    "madagascar", "madagasikara", "malgache",
    "mozambique", "mocambique", "moçambique",
    "comores", "comoros", "ngazidja", "anjouan", "moheli", "mohéli",
    "seychelles",
    "maurice", "mauritius", "rodrigues",
    "tanzanie", "tanzania", "zanzibar", "republique-unie de tanzanie",
    "république-unie de tanzanie", "republique unie de tanzanie",
    "kenya",
    "somalie", "somalia", "somaliland",
    "afrique du sud", "south africa",
    "reunion", "réunion", "mayotte", "maore", "mahoré",
    "taaf", "terres australes", "tromelin", "glorieuses", "juan de nova",
    "europa", "bassas da india",
    # Territoires britanniques de l'océan Indien (BIOT / Chagos) = zone
    "biot", "chagos", "ru (to", "ru(to", "ru (tom", "ru(tom",
    "royaume-uni (territoires", "united kingdom (ot", "united kingdom(ot",
]

# (1) NATURE retenue — vraie décision / sanction / rapport IUU / problème État
CTOI_NATURE_OK = [
    "navires inn", "navire inn", "liste inn", "liste des navires inn",
    "liste de navires inn", "iuu vessel", "iuu fishing",
    "peche inn", "pêche inn", "peche illegale", "pêche illégale",
    "peche illicite", "pêche illicite", "illegal fishing",
    "activites de peche", "activités de pêche", "activite de peche",
    "non-conformite", "non conformite", "non-conformité", "non conformité",
    "non compliance", "non-compliance",
    "infraction", "infractions", "sanction", "sanctions",
    "presumee", "présumée", "presumees", "présumées", "presumed",
    "souverainete", "souveraineté", "sovereignty",
    "niveau de conformite", "niveau de conformité", "level of compliance",
    "statut cncp", "renouvellement.*cncp", "cas de peche inn",
    "cas de pêche inn", "resolution du cas",
    "feuille de route pour combattre la peche inn",
]

# (1bis) NATURE écartée — procédure / technique / science / admin / ONG /
# candidatures, et tout ce qui n'est pas une décision de fond.
CTOI_NATURE_EXCLUE = [
    "template", "assessment criteria", "country template",
    "criteres d'evaluation", "critères d'évaluation",
    "e-maris", "workflow", "glossary", "glossaire", "manual", "manuel",
    "terms of references", "termes de reference", "termes de référence",
    "methodology", "methodologie", "méthodologie",
    "comite scientifique", "comité scientifique", "scientific committee",
    "groupe de travail sur les", "working party on",
    "prises accessoires", "porte-epees", "porte-épées",
    "thons tropicaux", "thons temperes", "thons tempérés",
    "thons neritiques", "thons néritiques", "collecte des donnees",
    "collecte des données", "ecosystemes", "écosystèmes", "marquage",
    "socio-econom", "administration et des finances", "comite permanent",
    "comité permanent",
    "wwf", "oceana", "pew", "policy brief", "ngo",
    "ordre du jour", "agenda", "liste des documents", "list of documents",
    "information document", "general information",
    "informations pour les participants", "calendrier",
    "indicative schedule", "information paper", "information from",
    "fonctionnalite de recherche", "fonctionnalité de recherche",
    "tool to publish",
    # candidatures (≠ sanctions) : on les retire explicitement
    "candidature", "accession au statut", "demande du statut d'observateur",
    "statut d'observateur", "demande du statut cncp", "demande de statut",
    "demande statut", "application du statut cncp", "candidature cncp",
    # téléchargements groupés / présentations
    "download all", "telecharger tous", "télécharger tous",
    "telecharger les", "télécharger les", "download compliance",
    "download responses", "chairperson", "presentation to",
    # projets pilotes & analyses techniques de transbordement
    "projet pilote", "pilot project", "national report on transhipment",
    "report on transhipment", "comparative assessment",
    "develop a methodology", "establish a baseline",
    "provide recommendations", "review of the assessment",
    "aligning rfmo", "trade measures to combat",
    # versions de travail
    "provisoire", "provisional", "projet de rapport", "draft report",
    "ebauche", "ébauche",
    "google drive link", "tous les fichiers word",
]

# Références à TOUJOURS inclure si présentes (récupérées manuellement par
# l'utilisateur, même si elles ne passent pas les règles automatiques).
# Ici : l'échange 2013 Tanzanie/Seychelles autour de la liste INN provisoire.
CTOI_REFS_FORCEES = {
    "2013-72", "2013-73", "2013-78",
}


def collecter_ctoi_fichier_local(articles):
    """
    Parse un export TSV/CSV de la page « Documents » de la CTOI déposé à côté
    du script (ctoi.txt par défaut). Colonnes attendues :
        Reference | Titre | Year | Meeting | Availability | Download
    Sépare automatiquement par tabulation ; bascule sur la virgule sinon.

    N'INJECTE que les lignes à portée juridique (listes INN, non-conformité,
    infractions, statuts CNCP, rapports de niveau de conformité, déclarations
    de souveraineté). Dédoublonne par titre normalisé.

    Chaque entrée reçoit un abstract mentionnant « océan indien » afin de
    franchir le filtre zone obligatoire de filtrer_juridique().

    Retourne le nombre d'entrées ajoutées (0 si aucun fichier trouvé).
    """
    chemin = None
    for nom in CTOI_FICHIERS_LOCAUX:
        cand = os.path.join(SCRIPT_DIR, nom)
        if os.path.exists(cand):
            chemin = cand
            break
    if not chemin:
        return 0

    print(f"  [CTOI/IOTC] Export local détecté : {os.path.basename(chemin)}")

    # Lecture tolérante à l'encodage
    contenu = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with open(chemin, encoding=enc) as f:
                contenu = f.read()
            break
        except Exception:
            continue
    if not contenu:
        print("  [CTOI/IOTC] Impossible de lire le fichier local.")
        return 0

    lignes = contenu.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    titres_vus = {normaliser(a.get("titre", "")) for a in articles}
    nb = 0

    for ligne in lignes:
        if not ligne.strip():
            continue
        # Séparateur : tabulation prioritaire, sinon virgule
        champs = ligne.split("\t") if "\t" in ligne else ligne.split(",")
        champs = [c.strip() for c in champs]
        if len(champs) < 3:
            continue

        ref = champs[0]
        titre = champs[1] if len(champs) > 1 else ""
        annee_brute = champs[2] if len(champs) > 2 else ""
        meeting = champs[3] if len(champs) > 3 else ""
        date_dispo = champs[4] if len(champs) > 4 else ""

        # Ignorer la ligne d'en-tête
        if titre.lower() == "titre" or ref.lower() == "reference":
            continue

        # Année valable ?
        m_an = re.search(r"(19|20)\d{2}", annee_brute)
        annee = int(m_an.group(0)) if m_an else None

        blob = normaliser(" ".join([ref, titre, meeting]))

        # Référence explicitement forcée par l'utilisateur → on saute les filtres
        # automatiques (mais on garde le dédoublonnage par titre plus bas).
        ref_forcee = ref.strip() in CTOI_REFS_FORCEES

        # Écarter les questionnaires de conformité (CQ) et rapports individuels
        # (IR) par pays : titre = simple nom de pays, aucune décision de fond.
        if not ref_forcee and re.search(r"-(CQ|IR)\d", ref, re.IGNORECASE):
            continue

        # ── CONDITION 1 — NATURE + CONDITION 2 — ZONE ─────────────────────
        # a) Exclure procédure/technique/science/admin/ONG/candidatures/etc.
        #    (avec exception « provisoire » pour les listes INN d'un pays zone).
        # b) Exiger un marqueur de décision/sanction/IUU/problème d'État.
        # c) Exiger qu'un pays de la zone SWIO soit nommé.
        # Tout ceci est sauté pour une référence explicitement forcée.
        if not ref_forcee:
            exception_inn = (
                any(normaliser(p) in blob for p in CTOI_PAYS_ZONE)
                and any(m in blob for m in ("liste inn", "navires inn",
                                            "liste de navires inn",
                                            "liste des navires inn"))
            )
            exclu = False
            for x in CTOI_NATURE_EXCLUE:
                if normaliser(x) not in blob:
                    continue
                # seul « provisoire/provisional » est neutralisable (cas liste INN)
                if normaliser(x) in ("provisoire", "provisional") and exception_inn:
                    continue
                exclu = True
                break
            if exclu:
                continue
            # b) Exiger un marqueur de décision/sanction/IUU/problème d'État.
            #    'renouvellement.*cncp' est traité comme motif (sanction de statut),
            #    contrairement aux 'candidature/demande de statut' déjà exclues.
            nature_match = False
            for pat in CTOI_NATURE_OK:
                if ".*" in pat:
                    if re.search(pat, normaliser(blob)):
                        nature_match = True
                        break
                elif normaliser(pat) in blob:
                    nature_match = True
                    break
            if not nature_match:
                continue

            # ── CONDITION 2 — ZONE ────────────────────────────────────────
            # Un pays/territoire de la zone SWIO DOIT être explicitement nommé.
            # (Les listes INN globales et les courriers de pays tiers écartés.)
            if not any(normaliser(p) in blob for p in CTOI_PAYS_ZONE):
                continue

        # Dédoublonnage par titre
        titre_norm = normaliser(titre)
        if not titre_norm or titre_norm in titres_vus:
            continue
        titres_vus.add(titre_norm)

        # URL : la page de recherche CTOI préfiltrée sur la référence
        # (lien direct non déductible de façon fiable depuis l'export)
        ref_url = ref.replace(" ", "+")
        url_doc = f"https://iotc.org/fr/documents?search_api_fulltext={ref_url}"

        # Abstract : on injecte « océan indien » pour le filtre zone global + on
        # qualifie la nature juridique du document.
        abstract = (
            f"Document CTOI/IOTC ({meeting or 'CTOI'}) relatif à la conformité, "
            f"aux infractions, aux navires INN (pêche illégale) ou au statut des "
            f"parties dans la zone de compétence de la Commission — océan Indien. "
            f"Référence : {ref}."
            + (f" Disponible depuis le {date_dispo}." if date_dispo else "")
        )

        articles.append({
            "source_db": "CTOI/IOTC",
            "titre":     re.sub(r"\s+", " ", titre).strip(),
            "abstract":  abstract,
            "auteurs":   "Commission des Thons de l'Océan Indien (CTOI/IOTC)",
            "annee":     annee,
            "revue":     f"CTOI/IOTC — {meeting}" if meeting else "CTOI/IOTC",
            "doi":       "",
            "url":       url_doc,
            "type_doc":  "decision",
            "keywords":  "CTOI;IOTC;conformité;non-conformité;navires INN;IUU;"
                         "pêche illégale;CNCP;sanction;océan indien;pêche;thon",
            "note":      f"CTOI/IOTC|fichier_local|{ref}",
        })
        nb += 1

    return nb


def collecter_ctoi():
    """
    CTOI/IOTC — Commission des Thons de l'Océan Indien.
    La CTOI n'est PAS un tribunal : son Comité d'application (CoC) produit des
    rapports de conformité, des déclarations de non-conformité, des résolutions
    et la liste des navires INN (IUU). Ce sont des « sanctions de compliance »
    au sens diplomatique, intégrées ici au corpus juridique.

    Tous ces documents relèvent par définition de l'océan Indien (zone de
    compétence de la CTOI) : chaque entrée porte donc « océan indien » pour
    franchir le filtre zone. Liste statique d'URL réelles vérifiées (iotc.org).
    """
    articles = []
    print("  [CTOI/IOTC] Chargement rapports de conformité et résolutions...")

    BASE = "https://iotc.org"

    # ── SOURCE 0 — Fichier local exporté du site CTOI (PRIORITAIRE) ──────────
    # Si l'utilisateur a déposé un export TSV de la page « Documents » de la
    # CTOI (colonnes : Reference | Titre | Year | Meeting | Availability |
    # Download), on le parse et on en extrait UNIQUEMENT les décisions à portée
    # juridique : listes de navires INN, non-conformité, infractions, statuts
    # CNCP, rapports de niveau de conformité, déclarations de souveraineté.
    # Bien plus fiable que le scraping HTML (qui dépend du markup du site).
    n_ctoi_local = collecter_ctoi_fichier_local(articles)
    if n_ctoi_local:
        print(f"  [CTOI/IOTC] Fichier local → {n_ctoi_local} décisions pertinentes extraites")

    # Rapports et documents CTOI/IOTC — URLs réelles vérifiées
    CTOI_STATIQUES = [
        {"titre":"Rapport de la 23e Session du Comité d'Application de la CTOI (2026)",
         "annee":2026,
         "url":f"{BASE}/fr/documents/rapport-de-la-23e-session-du-comit%C3%A9-dapplication-de-la-ctoi",
         "abstract":"Rapport du Comité d'application de la CTOI : évaluation de la conformité des Parties contractantes aux mesures de conservation et de gestion dans l'océan Indien, déclarations de non-conformité, liste des navires INN (pêche illégale).",
         "keywords":"CTOI;IOTC;conformité;non-conformité;navires INN;IUU;océan indien;pêche;thon"},
        {"titre":"Rapport de la 22e Session du Comité d'Application de la CTOI (2025)",
         "annee":2025,
         "url":f"{BASE}/sites/default/files/documents/2025/04/IOTC-2025-CoC22-RE_-_ADOPTED_0.pdf",
         "abstract":"Rapport du Comité d'application de la CTOI : évaluation de la conformité des États membres aux mesures de conservation et de gestion dans l'océan Indien, déclarations de non-conformité, liste provisoire des navires INN (pêche illégale).",
         "keywords":"CTOI;IOTC;conformité;non-conformité;navires INN;IUU;océan indien;pêche;thon"},
        {"titre":"Rapport de la 11e Session du Comité d'Application de la CTOI",
         "annee":2014,
         "url":f"{BASE}/documents/report-eleventh-session-compliance-committee-0",
         "abstract":"Rapport identifiant les infractions possibles aux règlements de la CTOI par des navires de pêche, recommandations sur l'arraisonnement et l'inspection en haute mer dans l'océan Indien, suivi des irrégularités.",
         "keywords":"CTOI;IOTC;infractions;arraisonnement;inspection;océan indien;pêche illégale;thon"},
        {"titre":"Rapport résumé sur le niveau de conformité — CTOI (CoC17)",
         "annee":2020,
         "url":f"{BASE}/documents/summary-report-level-compliance-6",
         "abstract":"Rapport résumant le niveau de conformité des Parties contractantes de la CTOI aux résolutions contraignantes, manquements identifiés, gestion des pêches de thon dans l'océan Indien.",
         "keywords":"CTOI;IOTC;conformité;résolutions;océan indien;pêche;thon"},
        {"titre":"Rapport de conformité — Madagascar (CTOI/IOTC)",
         "annee":2014,
         "url":f"{BASE}/documents/compliance-report-madagascar",
         "abstract":"Rapport de conformité de Madagascar évalué par le Comité d'application de la CTOI : respect des mesures de conservation et de gestion, obligations de déclaration, pêche au thon dans l'océan Indien occidental (SWIO).",
         "keywords":"CTOI;IOTC;Madagascar;conformité;océan indien;SWIO;pêche;thon"},
        {"titre":"Rapport de conformité — Maldives (CTOI/IOTC)",
         "annee":2014,
         "url":f"{BASE}/documents/compliance-report-maldives",
         "abstract":"Rapport de conformité des Maldives évalué par le Comité d'application de la CTOI : mesures de conservation, déclaration des captures, pêche au thon dans l'océan Indien.",
         "keywords":"CTOI;IOTC;Maldives;conformité;océan indien;pêche;thon"},
        {"titre":"Liste des navires INN (IUU Vessel List) — CTOI/IOTC",
         "annee":2025,
         "url":f"{BASE}/iuu-vessels-list",
         "abstract":"Liste officielle des navires pratiquant la pêche illicite, non déclarée et non réglementée (INN/IUU) dans la zone de compétence de la CTOI — océan Indien. Sanction de compliance frappant les navires identifiés en infraction.",
         "keywords":"CTOI;IOTC;navires INN;IUU;pêche illégale;océan indien;sanction;liste noire"},
        {"titre":"Mesures de conservation et de gestion en vigueur — CTOI/IOTC",
         "annee":2025,
         "url":f"{BASE}/cmms",
         "abstract":"Recueil des résolutions et mesures de conservation et de gestion contraignantes adoptées par la CTOI pour les pêcheries de thon de l'océan Indien : limitation de capacité, MCS, mesures du ressort de l'État du port.",
         "keywords":"CTOI;IOTC;résolutions;mesures de conservation;océan indien;pêche;thon;MCS"},
    ]

    for c in CTOI_STATIQUES:
        if c["annee"] and (c["annee"] < ANNEE_MIN or c["annee"] > ANNEE_MAX):
            # On garde quand même les documents-cadres (liste INN, CMM, rapports clés)
            pass
        articles.append({
            "source_db": "CTOI/IOTC",
            "titre":     c["titre"],
            "abstract":  c["abstract"],
            "auteurs":   "Commission des Thons de l'Océan Indien (CTOI/IOTC)",
            "annee":     c["annee"],
            "revue":     "CTOI/IOTC — Comité d'application",
            "doi":       "",
            "url":       c["url"],
            "type_doc":  "decision",
            "keywords":  c["keywords"],
            "note":      f"CTOI/IOTC|{c['url']}",
        })

    # ── Scraping ciblé du tableau « Derniers rapports » (HTML lisible) ──
    # Capture automatiquement les nouveaux rapports d'application (CoC) et la
    # liste INN, SANS ramener les milliers de documents techniques de la CTOI.
    print("  [CTOI/IOTC] Scraping des derniers rapports d'application...")
    urls_vues = {a["url"] for a in articles}
    # Mots-clés qui qualifient un document pertinent (application / conformité / INN)
    MOTS_PERTINENTS = [
        "comité d'application", "comite d'application", "compliance committee",
        "rapport d'application", "rapports d'application", "compliance report",
        "navires inn", "iuu vessel", "liste des navires", "non-compliance",
        "coc", "infraction", "conformité",
    ]
    # Exclusions : on ignore les versions PROVISOIRES (versions de travail,
    # remplacées par une version finale) pour éviter d'analyser des doublons.
    MOTS_EXCLUS = [
        "provisoire", "provisional", "draft", "projet de rapport",
    ]
    try:
        r = requests.get(f"{BASE}/fr/documents", timeout=20,
                         headers={"User-Agent": "SWIO-Research/16.0",
                                  "Accept-Language": "fr,en;q=0.9"})
        if r.status_code == 200:
            # Lignes de tableau : <a href="...">Titre</a>
            liens = re.findall(
                r'href="(https?://iotc\.org/fr/documents/[^"]+)"[^>]*>([^<]{8,200})</a>',
                r.text, re.IGNORECASE)
            nb_ajout = 0
            for url_doc, titre in liens:
                titre_norm = normaliser(titre)
                if not any(normaliser(m) in titre_norm for m in MOTS_PERTINENTS):
                    continue
                # Écarter les rapports provisoires
                if any(normaliser(x) in titre_norm for x in MOTS_EXCLUS):
                    continue
                if url_doc in urls_vues:
                    continue
                urls_vues.add(url_doc)
                m_an = re.search(r'(20\d{2})', titre + url_doc)
                annee = int(m_an.group(1)) if m_an else None
                articles.append({
                    "source_db": "CTOI/IOTC",
                    "titre":     re.sub(r"\s+", " ", titre).strip(),
                    "abstract":  "Document du Comité d'application de la CTOI relatif à la conformité, aux infractions ou aux navires INN dans l'océan Indien.",
                    "auteurs":   "Commission des Thons de l'Océan Indien (CTOI/IOTC)",
                    "annee":     annee,
                    "revue":     "CTOI/IOTC — Comité d'application",
                    "doi":       "",
                    "url":       url_doc,
                    "type_doc":  "decision",
                    "keywords":  "CTOI;IOTC;conformité;application;navires INN;IUU;océan indien;pêche;thon",
                    "note":      f"CTOI/IOTC|scraping|{url_doc}",
                })
                nb_ajout += 1
            print(f"  [CTOI/IOTC] Scraping → {nb_ajout} rapports pertinents ajoutés")
        else:
            print(f"  [CTOI/IOTC] Scraping indisponible (HTTP {r.status_code}) — liste statique conservée")
    except Exception as e:
        print(f"  [CTOI/IOTC] Scraping échoué ({e}) — liste statique conservée")

    print(f"  [CTOI/IOTC] Total : {len(articles)} documents (rapports de conformité, résolutions, liste INN)")
    return articles


#  BASES PRESSE

REQUETES_PRESSE = [
    "fisheries conflict Indian Ocean",
    "illegal fishing Indian Ocean",
    "IUU fishing Indian Ocean",
    "fishing dispute Madagascar Mozambique",
    "fishing rights Kenya Tanzania Seychelles",
    "piracy Somalia fishing",
    "artisanal fishing conflict East Africa",
    "foreign fishing vessels East Africa",
    # --- Ciblage ÎLES SWIO (pour ne pas couvrir que l'Afrique continentale) ---
    "Seychelles tuna fishing dispute",
    "Mauritius fishing illegal",
    "Mayotte fishing France",
    "Chagos marine protected area fishing",
    "Comoros fishing conflict",
    "Reunion island fishing",
    "Scattered Islands Eparses fishing France",
    # --- Français ---
    "pêche conflit océan indien",
    "pêche illégale océan indien",
    "conflit pêche Madagascar Kenya Mozambique",
    "pêche Mayotte Comores Glorieuses",
]

# ── AllAfrica : codes pays (slugs URL) de la zone SWIO ──
# AllAfrica indexe par pays via /<slug>/ ou ?countries=<code>.
# On utilise la recherche par mot-clé + filtrage pays a posteriori.
ALLAFRICA_PAYS = [
    "southafrica", "mozambique", "tanzania", "kenya", "somalia",
    "madagascar", "mauritius", "comoros", "seychelles",
]

# Requêtes pêche larges (en + fr) pour la recherche AllAfrica
ALLAFRICA_REQUETES = [
    "fishing", "fisheries", "illegal fishing", "fishermen",
    "trawler", "fishing vessel", "fishing rights",
    "pêche", "pêcheurs", "pêche illégale",
]


def _lire_article_allafrica(url, headers, timeout=10):
    """
    Étage 2 : visite la page d'un article et extrait son texte.
    Retourne (texte, annee) ou ("", None) si échec.
    Parsing robuste multi-sélecteur.
    """
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return "", None
        h = r.text

        # Extraire le corps : AllAfrica met le texte dans <div class="story-body">
        # ou des balises <p>. On tente plusieurs pistes.
        corps = ""
        m = re.search(r'<div[^>]*class="[^"]*story-body[^"]*"[^>]*>(.*?)</div>',
                      h, re.IGNORECASE | re.DOTALL)
        if m:
            corps = m.group(1)
        if not corps:
            # Repli : concaténer tous les paragraphes
            paras = re.findall(r'<p[^>]*>(.*?)</p>', h, re.IGNORECASE | re.DOTALL)
            corps = " ".join(paras)

        texte = html.unescape(re.sub(r'<[^>]+>', ' ', corps))
        texte = re.sub(r'\s+', ' ', texte).strip()

        # Date : chercher une année dans les métadonnées ou l'URL
        annee = None
        md = re.search(r'datePublished"?\s*:?\s*"?(\d{4})', h)
        if md:
            annee = int(md.group(1))
        else:
            mu = re.search(r'/(\d{4})\d{4}\.html', url)  # parfois dans l'URL
            if mu:
                annee = int(mu.group(1))

        return texte[:2000], annee
    except Exception:
        return "", None


def collecter_allafrica(nb_max=150, pages_par_requete=2, lire_contenu=True):
    """
    AllAfrica.com — agrégateur de presse africaine locale.
    ⚠ PAS d'API officielle : scraping HTML du moteur de recherche.
    Stratégie : requête pêche large → collecte exhaustive → filtrage en aval.
    Parsing robuste à plusieurs sélecteurs (la structure HTML peut varier).

    NOTE : sélecteurs à ajuster après le 1er test réel (réseau requis).
    Si 0 article collecté → inspecter le HTML renvoyé (voir DEBUG ci-dessous).
    """
    articles, vus_urls = [], set()
    print("  [AllAfrica] Collecte (scraping recherche, sans clé)...")

    BASE = "https://allafrica.com/search/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en,fr;q=0.8",
    }

    DEBUG = False  # passe à True si 0 résultat → affiche un extrait du HTML

    for rq in ALLAFRICA_REQUETES:
        if len(articles) >= nb_max:
            break
        for page in range(1, pages_par_requete + 1):
            if len(articles) >= nb_max:
                break
            params = {"search_string": rq, "page": page}
            try:
                # ── Réessais automatiques sur timeout (3 tentatives) ──
                r = None
                for tentative in range(1, 4):
                    try:
                        r = requests.get(BASE, params=params, headers=headers, timeout=25)
                        break  # succès → on sort de la boucle de réessais
                    except requests.exceptions.Timeout:
                        if tentative < 3:
                            print(f"  [AllAfrica] timeout '{rq}' p{page} "
                                  f"(tentative {tentative}/3), nouvel essai dans 3s...")
                            time.sleep(3)
                        else:
                            print(f"  [AllAfrica] timeout '{rq}' p{page} "
                                  f"après 3 tentatives — abandon de cette requête")
                if r is None:
                    break  # les 3 tentatives ont échoué → requête suivante
                if r.status_code == 403:
                    print("  [AllAfrica] ⚠ HTTP 403 — site bloque le scraping. Abandon.")
                    return articles
                if r.status_code != 200:
                    print(f"  [AllAfrica] HTTP {r.status_code} — '{rq}' p{page}")
                    break

                html_txt = r.text
                if DEBUG and page == 1:
                    print(f"  [AllAfrica][DEBUG] HTML reçu ({len(html_txt)} car.) :")
                    print("  " + html_txt[:600].replace("\n", " "))

                # Parsing multi-sélecteur : on cherche les liens d'articles.
                # AllAfrica utilise des URL /stories/NNNNNNNN.html ou /<pays>/...
                liens = re.findall(
                    r'<a[^>]+href="(https?://[^"]*allafrica\.com/stories/\d+\.html)"[^>]*>(.*?)</a>',
                    html_txt, re.IGNORECASE | re.DOTALL
                )
                # Repli : liens relatifs /stories/
                if not liens:
                    liens_rel = re.findall(
                        r'<a[^>]+href="(/stories/\d+\.html)"[^>]*>(.*?)</a>',
                        html_txt, re.IGNORECASE | re.DOTALL
                    )
                    liens = [("https://allafrica.com" + u, t) for u, t in liens_rel]

                nb_page = 0
                for url_art, titre_brut in liens:
                    if len(articles) >= nb_max:   # plafond DUR pendant la collecte
                        break
                    titre = html.unescape(re.sub(r'<[^>]+>', '', titre_brut)).strip()
                    if not titre or len(titre) < 8:
                        continue
                    if url_art in vus_urls:
                        continue
                    vus_urls.add(url_art)
                    articles.append({
                        "source_db": "AllAfrica",
                        "titre":     titre,
                        "abstract":  "",   # le titre seul ; abstract via fetch optionnel
                        "auteurs":   "AllAfrica",
                        "annee":     None,  # date non fiable depuis la page de recherche
                        "revue":     "AllAfrica",
                        "doi":       "",
                        "url":       url_art,
                        "type_doc":  "news",
                        "keywords":  "",
                        "note":      f"AllAfrica|recherche:{rq}"
                    })
                    nb_page += 1

                print(f"  [AllAfrica] '{rq}' p{page} → {nb_page} liens | total:{len(articles)}")
                if nb_page == 0:
                    break  # plus de résultats pour cette requête
                time.sleep(1.5)  # rester poli : scraping lent
            except Exception as e:
                print(f"  [AllAfrica] Erreur ('{rq}' p{page}): {e}")
                break

    print(f"  [AllAfrica] Total liens collectés : {len(articles)} articles")

    # ── ÉTAGE 2 : lire le contenu de chaque article ──
    # Plafond de sécurité : on ne lit pas plus de MAX_LECTURE articles,
    # pour éviter un run interminable si le site répond lentement.
    MAX_LECTURE = 200
    if lire_contenu and articles:
        a_lire = articles[:MAX_LECTURE]
        if len(articles) > MAX_LECTURE:
            print(f"  [AllAfrica] ⚠ {len(articles)} articles, lecture limitée aux {MAX_LECTURE} premiers")
        print(f"  [AllAfrica] Lecture du contenu de {len(a_lire)} articles "
              f"(~{len(a_lire)*1.5/60:.0f} min, sois patient)...")
        for i, art in enumerate(a_lire, 1):
            texte, annee = _lire_article_allafrica(art["url"], headers)
            if texte:
                art["abstract"] = texte
            if annee:
                art["annee"] = annee
            if i % 25 == 0:
                print(f"  [AllAfrica] ...{i}/{len(articles)} articles lus")
            time.sleep(1.2)   # pause polie entre chaque page
        print(f"  [AllAfrica] Contenu récupéré pour {sum(1 for a in articles if a['abstract'])} articles")

    if len(articles) == 0:
        print("  [AllAfrica] ⚠ 0 article. Causes possibles :")
        print("  [AllAfrica]   • Site bloque le scraping (403/captcha)")
        print("  [AllAfrica]   • Structure HTML changée → mettre DEBUG=True")
        print("  [AllAfrica]   • URL de recherche modifiée")
    return articles


def collecter_guardian(nb_max=200):
    articles, vus_ids = [], set()
    if not GUARDIAN_API_KEY: print("  [Guardian] ⚠ Clé vide"); return []
    print("  [Guardian] Collecte...")
    # Quota par requête : on répartit le plafond sur toutes les requêtes
    # pour que les requêtes ciblées (îles, pays précis) soient atteintes,
    # au lieu de tout consommer sur la première.
    par_requete = max(20, nb_max // max(1, len(REQUETES_PRESSE)) + 10)
    for rq in REQUETES_PRESSE:
        debut_rq = len(articles)
        page, pp = 1, 50
        while len(articles) < nb_max and (len(articles) - debut_rq) < par_requete:
            url = (f"https://content.guardianapis.com/search?q={requests.utils.quote(rq)}"
                   f"&from-date={ANNEE_MIN}-01-01&to-date={ANNEE_MAX}-12-31"
                   f"&page={page}&page-size={pp}"
                   f"&show-fields=headline,trailText,bodyText,byline"
                   f"&api-key={GUARDIAN_API_KEY}")
            try:
                r = requests.get(url, timeout=20)
                if r.status_code == 401: print("  [Guardian] ⚠ Clé invalide"); return articles
                if r.status_code != 200: break
                data = r.json().get("response",{})
                res  = data.get("results",[])
                for it in res:
                    aid = it.get("id","")
                    if aid in vus_ids: continue
                    vus_ids.add(aid)
                    flds = it.get("fields",{})
                    titre = flds.get("headline") or it.get("webTitle","")
                    abstr = flds.get("trailText") or ""
                    body  = flds.get("bodyText") or ""
                    if not abstr and body: abstr = body[:500]
                    dr = it.get("webPublicationDate","")
                    am = re.search(r'(\d{4})', dr)
                    sec = it.get("sectionName","")
                    articles.append({"source_db":"The Guardian","titre":titre,"abstract":abstr,
                                     "auteurs":flds.get("byline") or "The Guardian",
                                     "annee":int(am.group(1)) if am else None,
                                     "revue":f"The Guardian — {sec}","doi":"","url":it.get("webUrl",""),
                                     "type_doc":"news","keywords":"",
                                     "note":f"Section:{sec}|Date:{dr[:10]}"})
                total_p = data.get("pages",1)
                print(f"  [Guardian] '{rq[:35]}' p{page}/{min(total_p,5)} → {len(res)}")
                if page >= min(total_p,5) or len(res) < pp: break
                page += 1; time.sleep(0.3)
            except Exception as e: print(f"  [Guardian] {e}"); break
        if len(articles) >= nb_max: break
    print(f"  [Guardian] Total : {len(articles)}")
    return articles


def _parser_rss(contenu, source_nom):
    """
    Parse générique RSS/Atom — retourne liste de dicts normalisés.
    Gère RSS 2.0, RDF/RSS 1.0 et Atom.
    """
    articles = []
    vus_liens = set()

    # Extraire tous les items (RSS) ou entries (Atom)
    items = re.findall(
        r'<(?:item|entry)[^>]*>(.*?)</(?:item|entry)>',
        contenu, re.IGNORECASE | re.DOTALL
    )

    for item_xml in items:
        def get(tag):
            # Essaie avec CDATA, puis sans
            m = re.search(
                rf'<{tag}[^>]*>(?:<!\[CDATA\[(.*?)\]\]>|(.*?))</{tag}>',
                item_xml, re.DOTALL | re.IGNORECASE
            )
            if not m: return ""
            val = m.group(1) or m.group(2) or ""
            val = re.sub(r'<[^>]+>', '', val)
            return html.unescape(val).strip()

        titre    = get("title")
        lien     = get("link") or get("id") or get("guid")
        # Nettoyer le lien (parfois Atom met l'URL dans href)
        if not lien:
            m = re.search(r'<link[^>]+href=["\']([^"\']+)["\']', item_xml, re.IGNORECASE)
            if m: lien = m.group(1)
        abstract = (get("description") or get("summary") or get("content") or "")[:600]
        date_raw = get("pubDate") or get("published") or get("updated") or get("dc:date")

        if not titre or not lien: continue
        if lien in vus_liens: continue
        vus_liens.add(lien)

        am    = re.search(r'(\d{4})', date_raw)
        annee = int(am.group(1)) if am else None

        articles.append({
            "source_db": source_nom,
            "titre":     titre,
            "abstract":  abstract,
            "auteurs":   source_nom,
            "annee":     annee,
            "revue":     source_nom,
            "doi":       "",
            "url":       lien,
            "type_doc":  "news",
            "keywords":  "",
            "note":      f"{source_nom}|{date_raw[:30]}"
        })

    return articles


def collecter_rss_africains(nb_max=150):
    """
    Collecte via 3 flux RSS africains sans clé.
    ⚠ Limitation : articles récents uniquement (pas d'archives).
    - Africa Eco News    : pêche illégale, environnement, Afrique orientale
    - The Conversation Africa : articles académiques grand public, OI/pêche
    - Africa Defense Forum   : sécurité maritime, IUU, flottes étrangères OI
    """
    articles = []

    flux = [
        (RSS_AFRICA_ECO_NEWS,      "Africa Eco News"),
        (RSS_CONVERSATION_AFRICA,  "The Conversation Africa"),
        (RSS_AFRICA_DEFENSE_FORUM, "Africa Defense Forum"),
    ]

    for url, nom in flux:
        if len(articles) >= nb_max: break
        print(f"  [RSS] {nom}...")
        try:
            r = requests.get(url, timeout=20,
                             headers={"User-Agent": "SWIO-Research/15.0",
                                      "Accept": "application/rss+xml, application/atom+xml, */*"})
            if r.status_code != 200:
                print(f"  [RSS] HTTP {r.status_code} — {nom}"); continue

            items = _parser_rss(r.text, nom)
            articles.extend(items)
            print(f"  [RSS] {nom} → {len(items)} articles")
            time.sleep(0.5)

        except Exception as e:
            print(f"  [RSS] Erreur ({nom}): {e}")

    print(f"  [RSS africains] Total : {len(articles)} articles (récents uniquement)")
    return articles


def collecter_rss_iles_swio(nb_max=200):
    """
    RSS presse locale des îles SWIO — sans clé.
    Couvre : La Réunion (Clicanoo, Linfo.re), Maurice (Le Mauricien, L'Express),
    Mayotte Hebdo, Seychelles News Agency, Al-Watwan (Comores).
    Articles récents uniquement — idéal pour les conflits locaux de pêche.
    """
    articles = []
    print("  [RSS îles SWIO] Collecte presse locale des îles...")

    for url_rss, nom in RSS_ILES_SWIO:
        if len(articles) >= nb_max: break
        print(f"  [RSS îles] {nom}...")
        try:
            r = requests.get(url_rss, timeout=20,
                             headers={"User-Agent": "SWIO-Research/15.0",
                                      "Accept": "application/rss+xml, application/atom+xml, */*"})
            if r.status_code != 200:
                print(f"  [RSS îles] HTTP {r.status_code} — {nom}"); continue

            items = _parser_rss(r.text, nom)
            # Pour les médias des îles : on garde tout ce qui parle de pêche
            # sans exiger le terme conflit (les conflits locaux sont souvent
            # traités comme des faits divers sans le mot "conflit")
            retenus = []
            for art in items:
                texte = (art.get("titre") or "") + " " + (art.get("abstract") or "")
                if contient_un(texte, BLOC_PECHE):
                    retenus.append(art)

            articles.extend(retenus)
            print(f"  [RSS îles] {nom} → {len(items)} total, {len(retenus)} sur pêche")
            time.sleep(0.5)

        except Exception as e:
            print(f"  [RSS îles] Erreur ({nom}): {e}")

    print(f"  [RSS îles SWIO] Total : {len(articles)} articles")
    return articles


#  JUSTICE ADMINISTRATIVE FRANÇAISE (opendata.justice-administrative.fr)
#  API non officielle documentée par fondamentaux.org (mars 2025)
#  Aucune clé requise — accès libre

def collecter_justice_administrative():
    """
    Open Data Justice Administrative — tribunaux administratifs,
    cours administratives d'appel, Conseil d'Etat.
    Décisions sur : pêche, ZEE, arraisonnement, licences de pêche,
    confiscation de navires, amendes, contentieux halieutiques.

    API non officielle (reverse-engineered) :
    https://opendata.justice-administrative.fr/recherche/api/
    Routes documentées par fondamentaux.org (28 mars 2025).

    URL de chaque décision :
    https://opendata.justice-administrative.fr/decision/[id]
    """
    BASE = "https://opendata.justice-administrative.fr/recherche/api"
    BASE_DECISION = "https://opendata.justice-administrative.fr/decision"

    # Requêtes ciblées — mot seul ou mot + période
    # Route mot + période : model_searchANDdates/Date_Lecture/MOT/date_debut/date_fin/NB
    # Route mot seul     : Simple_Search/MOT/NB
    REQUETES = [
        "pêche illégale",
        "pêche maritime",
        "licence de pêche",
        "accord de pêche",
        "zone économique exclusive pêche",
        "arraisonnement navire pêche",
        "confiscation navire pêche",
        "police de la pêche",
        "pêche ocean indien",
        "infraction pêche",
        "navire de pêche",
        "droits de pêche",
        "ressources halieutiques",
        "pêche illicite",
        "chalutier",
        "IUU fishing",
    ]

    MOTS_ZONE = [
        "océan indien","ocean indien","indian ocean","swio","iotc","ctoi",
        "mayotte","réunion","reunion","madagascar","mozambique","seychelles",
        "maurice","comores","comoros","tanzanie","tanzania","kenya","somalia",
        "somalie","taaf","terres australes","tromelin","glorieuses","crozet",
        "kerguelen","juan de nova","europa","bassas da india","canal du mozambique",
        "zone économique exclusive","zee","eez","droit maritime",
    ]

    MOTS_CONFLIT_DECISION = [
        "pêche","peche","navire","chalutier","chalut","arraisonnement",
        "confiscation","saisie","amende","sanction","infraction","contravention",
        "licence","autorisation","police","surveillance","garde-côtes","marine",
        "filet","capture","quota","ressource halieutique","halieutique",
        "poisson","thon","crevette","langouste","poulpe","espèce marine",
        "zone économique","zee","eez","droit maritime","maritime",
    ]

    articles = []
    vus_ids  = set()

    headers = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Referer":         "https://opendata.justice-administrative.fr/recherche",
    }

    print("  [JusAdmin] Collecte Open Data Justice Administrative...")

    for mot in REQUETES:
        mot_enc = requests.utils.quote(mot)
        # Route avec période 1970–2025
        url = (f"{BASE}/model_searchANDdates/Date_Lecture"
               f"/{mot_enc}/{ANNEE_MIN}-01-01/{ANNEE_MAX}-12-31/200")
        try:
            r = requests.get(url, headers=headers, timeout=25)
            if r.status_code != 200:
                # Repli : route simple sans période
                url2 = f"{BASE}/Simple_Search/{mot_enc}/200"
                r = requests.get(url2, headers=headers, timeout=25)
                if r.status_code != 200:
                    print(f"  [JusAdmin] HTTP {r.status_code} — '{mot}'")
                    time.sleep(1.0)
                    continue

            data = r.json()
            # La réponse est une liste de décisions ou un dict avec une clé "hits"
            if isinstance(data, dict):
                decisions = data.get("hits", data.get("results",
                             data.get("decisions", [])))
            else:
                decisions = data if isinstance(data, list) else []

            nb_brut = len(decisions)
            nb_retenu = 0

            for dec in decisions:
                # Extraire l'identifiant unique
                doc_id = (dec.get("id") or dec.get("_id") or
                          dec.get("numero") or dec.get("identifiant") or "")
                if not doc_id:
                    # Chercher dans _source si structure Elasticsearch
                    src = dec.get("_source", {})
                    doc_id = src.get("id") or src.get("numero") or ""
                    dec = {**dec, **src}  # aplatir _source

                if doc_id and doc_id in vus_ids:
                    continue
                if doc_id:
                    vus_ids.add(str(doc_id))

                # Extraire les champs
                titre   = (dec.get("titre") or dec.get("title") or
                           dec.get("numero") or dec.get("identifiant") or
                           f"Décision {doc_id}")
                texte   = (dec.get("texte") or dec.get("text") or
                           dec.get("contenu") or dec.get("content") or
                           dec.get("resume") or "")
                date_d  = (dec.get("date") or dec.get("date_lecture") or
                           dec.get("dateDecision") or dec.get("Date_Lecture") or "")
                juridiction = (dec.get("juridiction") or dec.get("tribunal") or
                               dec.get("jurisdiction") or "")
                solution    = (dec.get("solution") or dec.get("sens") or "")

                m_annee = re.search(r'(19|20)\d{2}', str(date_d))
                annee   = int(m_annee.group(0)) if m_annee else None
                if annee and (annee < ANNEE_MIN or annee > ANNEE_MAX):
                    continue

                texte_complet = f"{titre} {texte[:1000]}".lower()

                # Filtre 1 : doit parler de pêche/maritime dans le texte
                if not contient_un(texte_complet, MOTS_CONFLIT_DECISION):
                    continue

                # Filtre 2 : zone SWIO OU décision de justice maritime générale
                # (on garde les décisions sur la ZEE française même sans pays SWIO explicite
                #  car elles concernent souvent Mayotte/Réunion/TAAF)
                zone_ok = contient_un(texte_complet, MOTS_ZONE)

                # Construire l'URL directe vers la décision
                url_decision = ""
                if doc_id:
                    url_decision = f"{BASE_DECISION}/{doc_id}"

                abstract = texte[:400].strip() if texte else (
                    f"Décision de justice administrative — {juridiction} — {solution}"
                )

                articles.append({
                    "source_db":  "Justice Administrative FR",
                    "titre":      titre or f"Décision {doc_id}",
                    "abstract":   abstract,
                    "auteurs":    juridiction or "Juridiction administrative",
                    "annee":      annee,
                    "revue":      juridiction or "Justice Administrative",
                    "doi":        "",
                    "url":        url_decision,
                    "type_doc":   "decision",
                    "keywords":   f"pêche;droit maritime;justice administrative;{solution}",
                    "note":       (f"JusAdmin|ID:{doc_id}|{juridiction}"
                                   f"|{date_d[:10]}|Zone:{zone_ok}"),
                })
                nb_retenu += 1

            print(f"  [JusAdmin] '{mot}' → {nb_brut} bruts, {nb_retenu} retenus")
            time.sleep(1.5)

        except Exception as e:
            print(f"  [JusAdmin] Erreur ('{mot}') : {e}")
            time.sleep(2.0)

    # Filtre final zone : on garde tout ce qui parle de pêche maritime
    # (même sans zone SWIO explicite — le TA de Mayotte, Saint-Denis, etc.
    #  n'indique pas toujours "océan indien" dans la décision)
    print(f"  [JusAdmin] Total brut : {len(articles)}")

    # Filtre zone strict : on exclut ce qui n'a AUCUN lien avec la zone
    # mais on garde les décisions françaises sur ZEE/pêche maritime
    # même sans mention explicite SWIO (elles concernent souvent les DOM)
    retenus_zone = []
    for art in articles:
        texte = f"{art['titre']} {art['abstract']}".lower()
        note  = art.get("note", "")
        # Garder si : zone SWIO explicite OU juridiction DOM OU droit maritime ZEE
        if ("Zone:True" in note or
            contient_un(texte, MOTS_ZONE) or
            contient_un(texte, ["mayotte","réunion","reunion","taaf","saint-denis",
                                 "saint pierre","polynésie","nouvelle-calédonie"])):
            retenus_zone.append(art)

    print(f"  [JusAdmin] Après filtre zone : {len(retenus_zone)} décisions")
    return retenus_zone


#  POINT D'ENTRÉE

if __name__ == "__main__":
    zones_a_traiter = ZONES_A_COLLECTER or list(ZONES_SWIO.keys())

    print("=" * 65)
    print("  COLLECTE SWIO v17.0 — Filtrage différencié + dédup 3 niveaux")
    print(f"  Zones : {len(zones_a_traiter)} | Années : {ANNEE_MIN}–{ANNEE_MAX}")
    print(f"  Plafond : {MAX_SCIENCE} science / {MAX_JURIDIQUE} juridique / {MAX_PRESSE} presse")
    print(f"  Science  : zone + pêche + conflit/gouvernance [strict]")
    print(f"  Juridique: décisions de justice (ITLOS, CIJ, CTOI)")
    print(f"  Presse   : The Guardian + RSS africains")
    print(f"  Dédup    : D1 intra → D2 inter-cat → D3 Jaccard titre")
    print("=" * 65)

    # Filtres zone globaux
    filtres_zone_global = []
    vus_f = set()
    for z in zones_a_traiter:
        if z not in ZONES_SWIO: print(f"Zone '{z}' inconnue."); sys.exit(1)
        for t in ZONES_SWIO[z]["filtres"]:
            if t not in vus_f: filtres_zone_global.append(t); vus_f.add(t)

    # ── PARTIE A — SCIENCE ────────────────────────────────────
    print("\n" + "━"*65 + "\n  PARTIE A — BASES SCIENTIFIQUES\n" + "━"*65)
    tous_bruts = []
    total_oa = total_cr = total_hal = 0

    for zone_nom in zones_a_traiter:
        zc = ZONES_SWIO[zone_nom]
        rq = zc["requete"]; nb_max = zc.get("nb_max", NB_MAX_DEFAUT)
        print(f"\n  ── {zone_nom} | max={nb_max}")
        resultats = {}
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {
                pool.submit(collecter_openalex, rq, nb_max, ANNEE_MIN, ANNEE_MAX): "OpenAlex",
                pool.submit(collecter_crossref, rq, nb_max, ANNEE_MIN, ANNEE_MAX): "Crossref",
                pool.submit(collecter_hal,      rq, nb_max, ANNEE_MIN, ANNEE_MAX): "HAL",
            }
            for fut in as_completed(futures):
                nom = futures[fut]
                try:
                    resultats[nom] = fut.result()
                    print(f"  ✓ [{nom}] {len(resultats[nom])}")
                except Exception as e:
                    print(f"  ✗ [{nom}] {e}"); resultats[nom] = []
        oa  = resultats.get("OpenAlex",[]); total_oa  += len(oa)
        cr  = resultats.get("Crossref",[]); total_cr  += len(cr)
        hal = resultats.get("HAL",[]);      total_hal += len(hal)
        tous_bruts += oa + cr + hal

    print(f"\n  Total brut : {len(tous_bruts)}")
    uniques_science = dedoublonner(tous_bruts)
    print(f"  [D1] → {len(uniques_science)} uniques")
    print("\n  Enrichissement abstracts Crossref...")
    enrichir_abstracts(uniques_science)
    print("\n  Filtrage science (zone + pêche + conflit/gouvernance)...")
    filtres_science = filtrer_science(uniques_science, filtres_zone_global)
    print(f"  Retenus : {len(filtres_science)} / {len(uniques_science)} (plafond {MAX_SCIENCE})")
    exporter_ris(filtres_science, FICHIER_SCIENCE)

    # ── PARTIE B — JURIDIQUE (décisions de justice) ───────────
    print("\n" + "━"*65 + "\n  PARTIE B — DÉCISIONS DE JUSTICE\n" + "━"*65)
    tous_jur = []

    print("\n  B1 — ITLOS (Tribunal International du Droit de la Mer)")
    itlos_docs = collecter_itlos()
    tous_jur.extend(itlos_docs)

    print("\n  B2 — CIJ/ICJ (Cour Internationale de Justice)")
    icj_docs = collecter_icj()
    tous_jur.extend(icj_docs)

    # B3 — ECOLEX : DÉSACTIVÉ (site en JavaScript, scraping renvoyait 0)
    ecolex_docs = []

    print("\n  B4 — CTOI/IOTC (résolutions et sanctions de compliance)")
    ctoi_docs = collecter_ctoi()
    tous_jur.extend(ctoi_docs)

    # B5 — Justice Administrative française : DÉSACTIVÉ (API en 404)
    jusadmin_docs = []

    uniques_jur = dedoublonner(tous_jur)
    print(f"\n  [D1] Total juridique : {len(tous_jur)} → {len(uniques_jur)} uniques")
    print("\n  Filtrage juridique (pêche dans titre + zone obligatoire)...")
    filtres_juridiques = filtrer_juridique(uniques_jur, filtres_zone_global)
    print(f"  Retenus : {len(filtres_juridiques)} / {len(uniques_jur)} (plafond {MAX_JURIDIQUE})")
    exporter_ris(filtres_juridiques, FICHIER_JURIDIQUE)

    # ── PARTIE C — PRESSE ─────────────────────────────────────
    print("\n" + "━"*65 + "\n  PARTIE C — BASES PRESSE\n" + "━"*65)
    tous_presse = []

    print("\n  C1 — The Guardian")
    guardian_arts = collecter_guardian(nb_max=200)
    tous_presse.extend(guardian_arts)

    print("\n  C2 — RSS africains (Africa Eco News + The Conversation + Africa Defense Forum)")
    rss_arts = collecter_rss_africains(nb_max=150)
    tous_presse.extend(rss_arts)

    print("\n  C3 — AllAfrica (presse locale africaine, scraping sans clé)")
    allafrica_arts = collecter_allafrica(nb_max=400)
    tous_presse.extend(allafrica_arts)

    print("\n  C4 — RSS presse locale îles SWIO (Réunion, Maurice, Mayotte, Seychelles...)")
    iles_arts = collecter_rss_iles_swio(nb_max=200)
    tous_presse.extend(iles_arts)

    print("\n  C5 — Europresse (export RIS local : Le Monde, AFP, presse FR...)")
    europresse_arts = collecter_europresse_ris()
    # NB : Europresse n'entre PAS dans `tous_presse`. Ses abstracts sont
    # tronqués (~180 car.) : on lui réserve filtrer_europresse() (zone + pêche,
    # conflit annoté mais non exigé). Le mélanger au filtre global le détruirait.

    # ── Sources « web » (abstract complet) : filtre presse standard ──
    uniques_presse = dedoublonner(tous_presse)
    print(f"\n  [D1] Total presse web : {len(tous_presse)} → {len(uniques_presse)} uniques")

    # ── DIAGNOSTIC : ventilation collecté vs retenu par source ──
    print("\n  ┌─ DIAGNOSTIC PRESSE (collecté → retenu après filtrage) ─")
    sources_presse = {
        "The Guardian":    (guardian_arts, filtrer_presse),
        "RSS africains":   (rss_arts,      filtrer_presse),
        "AllAfrica":       (allafrica_arts, filtrer_allafrica),
        "RSS îles SWIO":   (iles_arts,     filtrer_presse),
        "Europresse":      (europresse_arts, filtrer_europresse),
    }
    for nom_src, (arts_src, fn_filtre) in sources_presse.items():
        if not arts_src:
            print(f"  │  {nom_src:<18} : 0 collecté (rien renvoyé par la source)")
            continue
        retenus_src = fn_filtre(list(arts_src), filtres_zone_global)
        print(f"  │  {nom_src:<18} : {len(arts_src)} collecté → {len(retenus_src)} retenu")
    print("  └────────────────────────────────────────────────────")

    print("\n  Filtrage presse web (zone + pêche + conflit)...")
    filtres_presse_web = filtrer_presse(uniques_presse, filtres_zone_global)
    print(f"  Presse web retenue : {len(filtres_presse_web)} / {len(uniques_presse)}")

    print("\n  Filtrage Europresse (zone + pêche, conflit annoté)...")
    uniques_europresse = dedoublonner(europresse_arts)
    filtres_europresse = filtrer_europresse(uniques_europresse, filtres_zone_global)
    print(f"  Europresse retenue : {len(filtres_europresse)} / {len(uniques_europresse)}")

    # Fusion des deux flux presse + dédoublonnage + plafond global
    filtres_presse = dedoublonner_inter([filtres_presse_web, filtres_europresse])[:MAX_PRESSE]
    print(f"  Total presse (web + Europresse, dédoublonné) : {len(filtres_presse)} (plafond {MAX_PRESSE})")
    exporter_ris(filtres_presse, FICHIER_PRESSE)

    # ── PARTIE D — DÉDUP INTER-CATÉGORIES [D2] ────────────────
    print("\n" + "━"*65 + "\n  PARTIE D — DÉDOUBLONNAGE INTER-CATÉGORIES [D2]\n" + "━"*65)
    total_avant = len(filtres_science) + len(filtres_juridiques) + len(filtres_presse)
    global_d2 = dedoublonner_inter([filtres_science, filtres_juridiques, filtres_presse])
    print(f"  {len(filtres_science)} science + {len(filtres_juridiques)} juridique + {len(filtres_presse)} presse = {total_avant}")
    print(f"  [D2] → {len(global_d2)} ({total_avant - len(global_d2)} doublons inter-catégories)")

    # ── PARTIE E — DÉDUP FINAL JACCARD [D3] ───────────────────
    print("\n" + "━"*65 + "\n  PARTIE E — DÉDOUBLONNAGE FINAL JACCARD [D3]\n" + "━"*65)
    print("  Seuil Jaccard : 82% — quasi-doublons inter-bases...")
    global_final = dedoublonner_final(global_d2, seuil=0.82)
    exporter_ris(global_final, FICHIER_GLOBAL)

    # ── RÉSUMÉ ────────────────────────────────────────────────
    print("\n" + "═"*65)
    print("  RÉSUMÉ FINAL")
    print("═"*65)
    print(f"  SCIENTIFIQUE  (plafond {MAX_SCIENCE})")
    print(f"    OpenAlex : {total_oa} | Crossref : {total_cr} | HAL : {total_hal}")
    print(f"    [D1] dédoublonné : {len(uniques_science)} | filtres : {len(filtres_science)}")
    print(f"    → {os.path.basename(FICHIER_SCIENCE)}")
    print()
    print(f"  JURIDIQUE — décisions de justice  (plafond {MAX_JURIDIQUE})")
    print(f"    ITLOS:{len(itlos_docs)} | CIJ:{len(icj_docs)} | CTOI:{len(ctoi_docs)}")
    print(f"    [D1] dédoublonné : {len(uniques_jur)} | filtres : {len(filtres_juridiques)}")
    print(f"    → {os.path.basename(FICHIER_JURIDIQUE)}")
    print()
    print(f"  PRESSE  (plafond {MAX_PRESSE})")
    print(f"    Guardian : {len(guardian_arts)} | RSS africains : {len(rss_arts)}")
    print(f"    AllAfrica : {len(allafrica_arts)} | RSS îles SWIO : {len(iles_arts)}")
    print(f"    [D1] dédoublonné : {len(uniques_presse)} | filtres : {len(filtres_presse)}")
    print(f"    → {os.path.basename(FICHIER_PRESSE)}")
    print()
    print(f"  GLOBAL")
    print(f"    Avant D2 : {total_avant} | [D2] inter-cat : {len(global_d2)} | [D3] Jaccard : {len(global_final)}")
    print(f"    Total doublons supprimés en D2+D3 : {total_avant - len(global_final)}")
    print(f"    → {os.path.basename(FICHIER_GLOBAL)}")
    print("═"*65)
    print()
    print("  IMPORT ZOTERO : Fichier → Importer → sélectionner le(s) .ris")
    print()
    print("  NOTES :")
    print("  ITLOS     : https://www.itlos.org/en/main/cases/list-of-cases/")
    print("  CTOI/IOTC : https://www.iotc.org/fr/documents/resolutions")
    print("  CTOI local: déposez 'ctoi.txt' (export TSV de la page Documents)")
    print("              à côté du script → décisions INN/conformité/CNCP triées")
    print("  RSS Africa: articles récents uniquement (pas d'archives)")
    print("═"*65)
