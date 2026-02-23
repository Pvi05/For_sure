import requests
import csv

FR_OUTLET = "bsky.app/starter-pack/florianhuguenin.com/3lk7xbxetrr2o"


def resolve_uri(starter_pack_url):
    parts = starter_pack_url.replace("bsky.app/starter-pack/", "").split("/")
    handle = parts[0]
    pack_id = parts[1]

    response = requests.get(
        "https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle",
        params={"handle": handle}
    )
    did = response.json()["did"]

    return f"at://{did}/app.bsky.graph.starterpack/{pack_id}"


def get_full_list(list_uri):
    response = requests.get(
        "https://public.api.bsky.app/xrpc/app.bsky.graph.getList",
        params={"list": list_uri, "limit": 100}
    )
    items = response.json()["items"]
    return [item["subject"]["handle"] for item in items]


def get_starter_pack(url):
    uri = resolve_uri(url)

    response = requests.get(
        "https://public.api.bsky.app/xrpc/app.bsky.graph.getStarterPack",
        params={"starterPack": uri}
    )
    data = response.json()

    list_uri = data["starterPack"]["list"]["uri"]
    return get_full_list(list_uri)


handles = get_starter_pack(FR_OUTLET)
print(handles)
print(f"{len(handles)} handles found")


TIER_1 = [
    "reuters.com", "apnews.com", "en.afp.com", "nytimes.com",
    "washingtonpost.com", "wsj.com", "financialtimes.com", "bloomberg.com", 'afp.com', 'lemonde.fr', 'lefigaro.fr', 'franceinfo.fr', "nature.com", "science.org", "pubmed.ncbi.nlm.nih.gov",
    "who.int", "cdc.gov", "pasteur.fr", "inserm.fr", "factcheck.org", "snopes.com", "lesdecodeurs.fr", "checknews.fr"
]

TIER_2 = [
    "theguardian.com", "us.theguardian.com", "australia.theguardian.com",
    "economist.com", "npr.org", "theatlantic.com", "data.ft.com",
    "cnn.com", "euronews.com", 'liberation.fr', 'humanite.fr', 'rfi.fr', "publicsenat.fr", "mediapart.fr", "rtbf.be", "rts.ch", "gouvernement.fr", "elysee.fr", "europa.eu"
]

TIER_3 = [
    "latimes.com", "bostonglobe.com", "chicagotribune.com", "usatoday.com",
    "newsweek.com", "axios.com", "politico.com", "politico.eu", "forbes.com",
    "aljazeera.com", "wired.com", "theverge.com", "propublica.org",
    "huffpost.com", "thetimes.com", "the-independent.com", "irishtimes.com",
    "japantimes.co.jp", "theglobeandmail.com", "lbc.co.uk", 'nouvelobs.com', 'lepoint.fr'
]

TIER_4 = [
    "theintercept.com", "thenation.com", "democracynow.org",
    "kyivindependent.com", "kyivpost.com", "themoscowtimes.com",
    "inquirer.com", "thestar.com", "newstatesman1913.bsky.social",
    "talkingpointsmemo.com", "us.theconversation.com", "uk.theconversation.com", 'la-croix.com'
]

TIER_5 = [
    "news.rte.ie", "english.nv.ua", "pravda.ua", "ajiunit.com",
    "channel4news.bsky.social", "itvnews.bsky.social", "bbcnewsnight.bsky.social",
    "bylinesnetwork.co.uk", "politicshome.bsky.social", "bylinetimes.bsky.social",
    "thenewworldmag.bsky.social", "theipaper.com", "newseye.bsky.social", 'lesechosfr.bsky.social', 'courrierinter.bsky.social'
]

tiers = [
    (TIER_1, 0.95),
    (TIER_2, 0.90),
    (TIER_3, 0.80),
    (TIER_4, 0.70),
    (TIER_5, 0.60),
]

with open("assets/TrustedSources.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["handle", "score"])
    for tier, score in tiers:
        for handle in tier:
            writer.writerow([handle, score])
