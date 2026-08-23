#!/usr/bin/env python3
"""Build and validate VEVO batch 45 fabric-structure articles."""

from __future__ import annotations

import html
import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests


BASE = "https://www.vevo.sk"
PUBLISH_DATE = "2026-08-23"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-45-candidates-2026-08-23.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-45-2026-08-23-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-45-2026-08-23-link-preflight.json")

PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
CATEGORY_NAME = "Pracie gély"
CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"

COTTONWORKS_WOVEN = "https://cottonworks.com/learning-hub/weaving/basic-woven-fabric-designs/"
COTTONWORKS_TWILL = "https://cottonworks.com/encyclopedia-item/twill-2/"
COTTONWORKS_FINISHING = "https://cottonworks.com/learning-hub/finishing/mechanical-finishing/"
COTTONINC_SEERSUCKER = "https://www.cottoninc.com/wp-content/uploads/2017/12/TRI-2003-Processing-Woven-Cotton-Seersucker-Fabrics.pdf"
AATCC_DIMENSION = "https://members.aatcc.org/store/tm135/543/"
ISO_WOVEN_COUNT = "https://www.iso.org/standard/86700.html"
ASTM_WOVEN_COUNT = "https://store.astm.org/d3775-17r23.html"
ASTM_TEAR = "https://store.astm.org/d1424-25.html"
ASTM_PILE = "https://store.astm.org/d4685_d4685m-25.html"
ISO_PILLING = "https://www.iso.org/standard/75377.html"
WOOLMARK_VELVET = "https://www.woolmark.com/industry/use-wool/product-innovations/eco-velvet/"
EU_FIBRE_LABEL = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011R1007-20180215"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"

ARTICLE_COTTON = "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"
ARTICLE_VISCOSE = "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"
ARTICLE_POLYESTER = "/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal"
ARTICLE_BLEND = "/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_GSM = "/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach"
ARTICLE_COUNT = "/n/pocet-niti-pri-oblieckach-co-znamena-thread-count-a-co-o-kvalite-nehovori"
ARTICLE_TENSILE = "/n/pevnost-textilu-v-tahu-a-proti-roztrhnutiu-co-skusky-hovoria-o-odolnosti"
ARTICLE_SEAMS = "/n/pevnost-sva-a-posun-niti-preco-oblecenie-praska-pri-svoch"
ARTICLE_SNAGGING = "/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_COLORFASTNESS = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"
ARTICLE_IRONING = "/n/ako-vyzehlit-koselu-kompletny-sprievodca-pre-dokonaly-vysledok"
ARTICLE_STAIN = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_ODOR = "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"
ARTICLE_SOFTENER = "/n/avivaz-vs-parfum-do-prania-aky-je-rozdiel"

FORBIDDEN_PUBLIC_RE = re.compile(
    r"longtail|long-tail|long tail|\bkeyword(?:s)?\b|kľúčov\w*\s+slov\w*|"
    r"\bSEO\b|search\s+intent|sub[- ]?quer(?:y|ies)|sub[- ]?query|"
    r"fan[- ]?out|fanout|\bCTA\b",
    re.IGNORECASE,
)
FIXED_PRICE_RE = re.compile(r"\b\d{1,4}(?:[.,]\d{2})?\s*(?:EUR|€)\b", re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
WORD_RE = re.compile(r"[^\W_]+(?:[-'][^\W_]+)?", re.UNICODE)


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def table(headers: list[str], rows: list[tuple[str, ...]]) -> str:
    head = "".join(
        '<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left; background: #fafafa;">'
        f"{esc(header)}</th>"
        for header in headers
    )
    body = "\n".join(
        "<tr>"
        + "".join(
            '<td style="border: 1px solid #e5e5e5; padding: 10px; vertical-align: top;">'
            f"{cell}</td>"
            for cell in row
        )
        + "</tr>"
        for row in rows
    )
    return (
        '<div style="overflow-x: auto; margin: 20px 0;">'
        '<table style="width: 100%; min-width: 680px; border-collapse: collapse;">'
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"
    )


def callout(title: str, items: list[str], *, background: str = "#fffaf5", border: str = "#e6ded2") -> str:
    bullets = "".join(f"<li>{item}</li>" for item in items)
    return (
        f'<div style="border: 1px solid {border}; border-radius: 8px; padding: 18px; '
        f'margin: 22px 0; background: {background};">'
        f'<h2 style="margin-top: 0;">{esc(title)}</h2><ul>{bullets}</ul></div>'
    )


def source_box(article: dict[str, object]) -> str:
    links = "".join(
        f'<li><a rel="noopener" href="{url}" target="_blank">{esc(label)}</a></li>'
        for label, url in article["sources"]
    )
    return (
        '<div style="border-left: 4px solid #111; padding: 16px 18px; margin: 24px 0; background: #fbfbfb;">'
        '<h2 style="margin-top: 0;">Odborné zdroje a hranice porovnávania</h2>'
        f"<p>{article['source_intro']}</p><ul>{links}</ul></div>"
    )


def commercial_blocks(article: dict[str, object]) -> str:
    return f"""
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">Prací prostriedok prispôsobte hotovému výrobku</h2>
<p>{article['product_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{PRODUCT_NAME}</h3>
<p>{article['product_text']}</p>
<p><strong>Dôležitá hranica:</strong> {article['product_limit']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{PRODUCT_URL}">Pozrieť produkt</a></p>
</div>
</div>
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">Porovnajte pracie gély pre bežnú bielizeň</h2>
<p>{article['category_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{CATEGORY_NAME}</h3>
<p>{article['category_text']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{CATEGORY_URL}">Pozrieť kategóriu</a></p>
</div>
</div>
""".strip()


def related_links(items: list[tuple[str, str]]) -> str:
    links = "".join(f'<li><a href="{href}">{esc(label)}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2><ul>{links}</ul>"


def faq(article: dict[str, object]) -> str:
    parts = [f"<h2>FAQ: {esc(article['faq_title'])}</h2>"]
    for question, answer in article["faq"]:
        parts.append(f"<h3>{esc(question)}</h3><p>{answer}</p>")
    return "\n".join(parts)


def common_sections(article: dict[str, object]) -> list[dict[str, object]]:
    name = str(article["name"])
    return [
        {
            "heading": f"{name.capitalize()} nie je jedno konkrétne vlákno",
            "paragraphs": [
                f"Názov {name} opisuje predovšetkým {article['construction_summary']}. Nehovorí automaticky, či je výrobok z bavlny, polyesteru, viskózy, vlny alebo zo zmesi. Vlákno ovplyvňuje prijímanie vlhkosti, citlivosť na teplo, schnutie a reakciu na chémiu; konštrukcia zas povrch, pružnosť, splývavosť a spôsob mechanického opotrebovania. Bez oboch údajov nemožno bezpečne určiť starostlivosť.",
                f"Nariadenie EÚ o textilných názvoch vyžaduje informáciu o vláknovom zložení, no obchodný názov {name} odpovedá na inú otázku. Na etikete preto hľadajte percentá vlákien a symboly ošetrovania, nie iba veľký názov materiálu na obale. Ak hotový kus obsahuje podšívku, výplň, lepidlo, potlač alebo elastický diel, bezpečnú hranicu môže určiť práve tento najcitlivejší komponent.",
            ],
        },
        {
            "heading": "Gramáž, hustota a počet nití hovoria o odlišných veciach",
            "paragraphs": [
                f"Gramáž vyjadruje hmotnosť plochy, no rovnaké číslo môže vzniknúť z hrubšej priadze, hustejšej konštrukcie, viacerých vrstiev alebo povrchovej úpravy. Pri {name} preto samotné GSM neprezradí jemnosť, priedušnosť ani životnosť. Dve látky s podobnou hmotnosťou sa môžu pri praní správať odlišne, ak majú inú priadzu, väzbu, vlas alebo dodatočné dokončenie.",
                "Dostava tkaniny sa meria ako počet osnovných a útkových nití na určenú dĺžku. ISO 7211-2 a ASTM D3775 opisujú metódy určovania tohto počtu, nie univerzálnu stupnicu kvality. Výsledok treba spájať s priadzou, smerom, väzbou a konečným použitím. Vyššie číslo nemusí znamenať lepší komfort a nižšie číslo nemusí znamenať chybu, ak je otvorenejšia konštrukcia zámerom.",
            ],
        },
        {
            "heading": f"Ako čítať etiketu pred praním {article['genitive']}",
            "paragraphs": [
                f"Najprv oddeľte tri informácie: zloženie, symboly ošetrovania a konštrukčné detaily hotového kusu. Symbol vaničky stanovuje povolený spôsob prania, trojuholník bielenie, štvorec sušenie a žehlička tepelné ošetrenie. Pri {name} si navyše všimnite {article['label_details']}. Všeobecná rada z internetu nemá prednosť pred presným pokynom pre daný výrobok.",
                "Ak symbol chýba alebo je nečitateľný, nezačínajte najteplejším cyklom. Zistite zloženie, skontrolujte stálofarebnosť na skrytom mieste a pri hodnotnom kuse kontaktujte výrobcu alebo odbornú čistiareň. Opatrnejší postup znižuje riziko, ale nie je zárukou: neznáma živica, farbivo alebo lepený diel môže reagovať aj pri nízkej teplote.",
            ],
        },
        {
            "heading": "Triedenie a naplnenie bubna rozhoduje o trení",
            "paragraphs": [
                f"{name.capitalize()} neperte automaticky s každým kusom rovnakej farby. Zipsy, háčiky, suché zipsy, hrubé uteráky a ťažké nohavice môžu vytvoriť lokálne trenie, zachytiť {article['surface_object']} alebo za mokra stlačiť jemnú štruktúru. Pred praním zatvorte kovanie, vyprázdnite vrecká, odstráňte voľné nečistoty a citlivý kus obráťte alebo vložte do primerane veľkého ochranného vrecka.",
                "Bubon musí nechať textíliám priestor na pohyb a oplach. Preplnenie zvyšuje tlak, zadržiava nerozpustené nečistoty a môže vytvoriť záhyby, ktoré sa sušením zafixujú. Príliš malá náplň pri agresívnom programe zas nemusí byť šetrná, pretože kus opakovane naráža do bubna. Voľba programu, náplň a otáčky musia fungovať spolu.",
            ],
        },
        {
            "heading": "Dávkovanie prostriedku a oplach bez zvyškov",
            "paragraphs": [
                f"Viac prostriedku neznamená automaticky čistejší {name}. Dávku prispôsobte tvrdosti vody, veľkosti a znečisteniu náplne podľa obalu produktu. Nadbytok sa môže zachytiť v {article['residue_place']}, zmeniť omak, zvýrazniť pach po nedosušení a pritiahnuť nové nečistoty. Nedostatok zas nemusí odstrániť maz a častice, ktoré sa pri ďalšom nosení opäť uvoľnia.",
                "Ak je kus po praní tuhý, klzký alebo nerovnomerne voňavý, najprv preverujte dávku, naplnenie, prívod vody a oplach. Ďalšia vrstva prostriedku alebo vône príčinu nevyrieši. Doplnkový oplach má zmysel iba vtedy, keď ho etiketa a materiál povoľujú; pri citlivej farbe, špeciálnej úprave alebo ručnom praní postupujte podľa výrobcu.",
            ],
        },
        {
            "heading": f"Sušenie {article['genitive']} bez zbytočnej deformácie",
            "paragraphs": [
                f"Po skončení programu kus vyberte bez dlhého ležania vo vlhkom bubne. Nežmýkajte ho krútením a nenoste ho za jediný mokrý roh. {article['drying_advice']} Pri sušení zabezpečte prúdenie vzduchu aj medzi vrstvami, v záhyboch, vo švoch a pod ozdobami; suchý povrch ešte nemusí znamenať suché vnútro.",
                f"Sušičku použite iba pri povolenom symbole a vhodnom nastavení. Teplo, mechanické prevaľovanie a presušenie môžu pri {name} ovplyvniť {article['heat_risk']}. AATCC TM135 meria rozmerové zmeny po definovaných postupoch domáceho prania, no sama upozorňuje, že štandardizovaný postup nereplikuje každú domácnosť. Jediný výsledok preto nie je sľubom pre všetky programy a výrobky.",
            ],
        },
        {
            "heading": "Škvrnu riešte lokálne, ale bez agresívneho drhnutia",
            "paragraphs": [
                f"Najprv odstráňte prebytok lyžičkou alebo savou bielou handričkou a zistite, či je škvrna vodná, mastná, bielkovinová alebo pigmentová. Pri {name} môže silné trenie zmeniť {article['stain_risk']}, aj keď farba čiastočne zmizne. Prostriedok najprv otestujte na skrytom mieste a pracujte od okraja škvrny k stredu bez rozširovania mokrej mapy.",
                "Zafarbené miesto nevkladajte do vysokej teploty, kým neviete, či škvrna odišla. Teplo môže niektoré zvyšky zafixovať a súčasne zmeniť povrch. Chlórové bielidlo, alkohol, rozpúšťadlo alebo koncentrovaná kyselina nie sú univerzálne riešenie; kompatibilitu musí povoľovať vlákno, farbivo aj dokončenie.",
            ],
        },
        {
            "heading": "Kedy domáce pranie radšej zastaviť",
            "paragraphs": [
                f"Domáci postup nie je vhodný, ak etiketa vyžaduje profesionálne čistenie, ak sa na skúšobnom mieste uvoľňuje farba, ak sa {article['failure_sign']} alebo ak výrobok obsahuje neznámu výstuhu, lepenie či podšívku. Riziko rastie pri hodnotnom kabáte, čalúnení, historickom textile a odeve, ktorého tvar drží vnútorná konštrukcia.",
                "Pranie neobnoví pretrhnutú niť, opotrebovaný vlas, degradovanú živicu ani trvalo zdeformovaný šev. Pri mechanickom poškodení najprv stabilizujte miesto, aby sa cyklom nezväčšilo, a zvážte odbornú opravu. Pri reklamácii nového kusu odfoťte stav, etiketu a použitý postup; opakované pokusy môžu sťažiť rozlíšenie výrobnej chyby od následnej starostlivosti.",
            ],
        },
    ]


def render_article(article: dict[str, object]) -> str:
    parts = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['answer']}</p>",
        f"<p>{article['intro']}</p>",
        callout("Najdôležitejšie zistenia v skratke", article["quick"]),
        f"<h2>{esc(article['overview_heading'])}</h2>",
    ]
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["overview"])
    parts.append(f"<h2>{esc(article['table1_heading'])}</h2>")
    parts.append(f"<p>{article['table1_intro']}</p>")
    parts.append(table(article["table1_headers"], article["table1_rows"]))
    for section in list(article["sections"]) + common_sections(article):
        parts.append(f"<h2>{esc(section['heading'])}</h2>")
        parts.extend(f"<p>{paragraph}</p>" for paragraph in section["paragraphs"])
        if section.get("callout"):
            note = section["callout"]
            parts.append(callout(note["title"], note["items"], background=note.get("background", "#fffaf5"), border=note.get("border", "#e6ded2")))
    parts.append(f"<h2>{esc(article['table2_heading'])}</h2>")
    parts.append(f"<p>{article['table2_intro']}</p>")
    parts.append(table(article["table2_headers"], article["table2_rows"]))
    parts.append(f"<h2>{esc(article['steps_heading'])}</h2>")
    parts.append("<ol>" + "".join(f"<li>{item}</li>" for item in article["steps"]) + "</ol>")
    parts.append(callout("Čo si skontrolovať pred praním", article["remember"], background="#f7fbf8", border="#dbe5de"))
    parts.append(callout("Najčastejšie chyby", article["mistakes"], background="#fff7f7", border="#eadada"))
    parts.append(f"<h2>{esc(article['expert_heading'])}</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    parts.append(source_box(article))
    parts.append(commercial_blocks(article))
    parts.append(related_links(article["related"]))
    parts.append(faq(article))
    return "\n".join(parts)


ARTICLES: list[dict[str, object]] = [
    {
        "title": "Čo je mušelín: vzdušná gázovina, zrážanie a správna starostlivosť",
        "link": "co-je-muselin-vzdusna-gazovina-zrazanie-a-spravna-starostlivost",
        "meta": "Čo je mušelín, aký je rozdiel medzi jednovrstvovou a dvojitou gázovinou, prečo sa zráža a ako prať mušelínové oblečenie, plienky či obliečky.",
        "short": "Mušelín nie je jedno vlákno ani jedna kvalita. Spoznajte jednoduchú a dvojitú gázovinu, zrazenie, zachytávanie nečistôt a bezpečné pranie bez vyťahania vrstiev.",
        "name": "mušelín",
        "genitive": "mušelínu",
        "construction_summary": "ľahkú, spravidla plátnovo previazanú a často otvorenejšiu tkaninu; v maloobchode aj viac vrstiev gázoviny spojených drobnými väzbovými bodmi",
        "label_details": "počet vrstiev, voľnosť väzby, krčivý povrch, ozdobné strapce a spôsob spojenia vrstiev",
        "surface_object": "voľné nite alebo spojovacie body medzi vrstvami",
        "residue_place": "otvorenej väzbe a priestore medzi vrstvami",
        "drying_advice": "Ľahký kus rozložte bez ťahania; viacvrstvovú prikrývku alebo osušku niekoľkokrát premiestnite, aby pod preložením neostala vlhkosť.",
        "heat_risk": "rozmer, zvlnenie, mäkkosť, farbu a rozdielne napätie medzi vrstvami",
        "stain_risk": "otvorenú väzbu, jemné priadze a prirodzené zvlnenie",
        "failure_sign": "vrstvy oddeľujú, spojovacie nite praskajú alebo sa po navlhčení vytvorí trvalá deformácia",
        "answer": "Mušelín je ľahká tkanina, tradične spájaná s jemnou bavlnenou plátnovou väzbou, no dnešný obchodný názov pokrýva aj jednoduchú, dvojitú a viacvrstvovú gázovinu z rozličných vlákien. Typická vzdušnosť vzniká otvorenejšou konštrukciou, nie zárukou konkrétnej bavlny. Pred praním preto skontrolujte zloženie, symboly a počet vrstiev. Kus perte oddelene od zipsov a háčikov, v nepreplnenom bubne, s presnou dávkou vhodného prostriedku. Po praní ho neťahajte za mokré rohy a dôkladne vysušte aj medzi vrstvami. Mierne zvýraznenie zvlnenia môže byť prirodzené; veľká rozmerová zmena alebo rozpojenie vrstiev už vyžaduje kontrolu postupu a kvality výrobku.",
        "intro": "Pri slove mušelín si niekto predstaví tenkú hladkú látku, iný pokrčenú detskú plienku a ďalší hrubší štvorvrstvový prehoz. Všetky sa môžu predávať pod rovnakým názvom, hoci ich dostava, počet vrstiev, priadza a dokončenie sú odlišné. Preto neexistuje jedna univerzálna teplota ani sľub, že každý mušelín bude rovnako jemný. Praktická starostlivosť sa začína identifikáciou konkrétneho kusu a pochopením, kde sa v otvorenej a viacvrstvovej konštrukcii zachytáva mechanické napätie, vlhkosť a zvyšky nečistôt.",
        "quick": [
            "<strong>Názov nie je zloženie:</strong> mušelín môže byť bavlnený, viskózový, syntetický aj zmesový.",
            "<strong>Jedna vrstva nie je dvojitá gázovina:</strong> viac vrstiev mení hmotnosť za mokra, schnutie aj riziko nerovnomerného zrazenia.",
            "<strong>Krčivosť môže byť zámer:</strong> silné plošné žehlenie môže potlačiť reliéf, ktorý patrí k vzhľadu.",
            "<strong>Otvorená väzba sa ľahšie zachytí:</strong> zipsy, háčiky a suché zipsy oddeľte od jemného kusu.",
            "<strong>Etiketa rozhoduje:</strong> bežná rada pre bavlnu neplatí automaticky na farbenú zmes, výšivku alebo vrstvený výrobok.",
        ],
        "overview_heading": "Čo presne znamená mušelín a prečo sa názvy líšia",
        "overview": [
            "Historicky sa mušelínom označovali jemné, ľahké bavlnené tkaniny z južnej Ázie. V odevnej tvorbe sa anglické slovo muslin používa aj pre lacnejšiu nefarbenú plátnovú tkaninu na skúšobný model odevu. V súčasnom predaji posteľného, detského a domáceho textilu sa pod mušelínom často myslí single gauze, double gauze alebo viacvrstvová pokrčená gázovina. Tieto použitia sú príbuzné ľahkou tkanou konštrukciou, nie jednotnou technickou špecifikáciou.",
            "Jednoduchá gázovina je jedna tkaná plocha. Dvojitá gázovina má dve jemné vrstvy spojené počas výroby drobnými bodmi alebo spojovacími niťami, aby sa nesprávali ako dve voľné plachty. Pri troch či štyroch vrstvách rastie objem, savosť a množstvo vnútorného priestoru. Zároveň rastie čas potrebný na vysušenie a možnosť, že sa vrstvy pri nevhodnom zaobchádzaní posunú alebo zrazia rozdielne.",
            "Pokrčený vzhľad nemusí znamenať zanedbanie. Môže vzniknúť rozdielnym napätím priadzí, zmršťovacou úpravou alebo reakciou vrstiev pri dokončení a praní. Pri kúpe sa preto pýtajte, či je reliéf trvalou vlastnosťou, aké zloženie má každá vrstva a či bol výrobok predzrazený. Marketingové slovo „praný“ alebo „stonewashed“ samo neurčuje budúcu rozmerovú stabilitu.",
        ],
        "table1_heading": "Jednoduchý, dvojitý a viacvrstvový mušelín",
        "table1_intro": "Rozdiel nie je iba v hrúbka. Počet vrstiev ovplyvňuje správanie pri namočení, prúdenie vzduchu, zachytávanie nečistôt aj kontrolu schnutia.",
        "table1_headers": ["Konštrukcia", "Typické použitie", "Silná stránka", "Riziko pri starostlivosti"],
        "table1_rows": [
            ("Jednovrstvová jemná tkanina", "Šatky, ľahké odevy, skúšobné modely a dekoračné použitie.", "Nízka hmotnosť a prúdenie vzduchu podľa hustoty.", "Zachytenie voľnej nite, presvitanie, deformácia a strapkanie rezu."),
            ("Dvojitá gázovina", "Detské odevy, plienky, osušky, šaty a ľahké obliečky.", "Väčší objem bez kompaktnej ťažkej plochy.", "Posun vrstiev, poškodenie spojovacích bodov a vlhkosť medzi vrstvami."),
            ("Troj- a štvorvrstvový mušelín", "Deky, prehozy, uteráky a objemnejší domáci textil.", "Vyššia savosť a mäkký objem.", "Veľká hmotnosť za mokra, dlhšie schnutie a nerovnomerné napätie."),
            ("Hladší odevný mušelín", "Blúzky, podšívky, kostýmové skúšky a historické interpretácie.", "Jemný omak a splývavosť podľa priadze.", "Citlivosť farby, zrazenie, presvitanie a poškodenie pri žehlení."),
        ],
        "sections": [
            {
                "heading": "Mušelín verzus gázovina: kde je hranica",
                "paragraphs": [
                    "Gázovina je širší opis riedkej, ľahkej textílie. Mušelín sa v súčasnom predaji často používa práve pre gázovú bavlnenú látku, ale názvy nie sú na každom trhu technicky zameniteľné. Zdravotnícka gáza, dekoračná gázovina a dvojitý odevný mušelín majú rozdielne požiadavky. Pri starostlivosti je užitočnejšie opísať konkrétnu väzbu, hustotu, vrstvy a zloženie než sa spoliehať na samotný názov.",
                    f"Pri veľmi otvorenej jednovrstvovej textílii je dôležitá prevencia zachytenia; praktické kroky vysvetľuje návod <a href=\"{ARTICLE_SNAGGING}\">ako predchádzať zatrhávaniu textilu</a>. Pri viacvrstvovom mušelíne navyše sledujte spojovacie body a vnútorné schnutie. Domáca skúška proti svetlu môže ukázať otvorenosť a nerovnosti, ale nenahrádza normované meranie dostavy ani kontrolu pevnosti.",
                ],
            },
            {
                "heading": "Prečo je mušelín vzdušný, ale nie vždy chladivý",
                "paragraphs": [
                    "Otvorenejšie rozostupy medzi priadzami umožňujú výmenu vzduchu, no pocit tepla neurčuje iba väzba. Viac vrstiev zachytí medzi sebou vzduch a spomalí jeho pohyb; hrubšia priadza, objemný povrch a priliehavý strih môžu ľahkú látku zmeniť na teplejšiu. Vlhkosť, vietor, spodná vrstva a aktivita človeka ovplyvnia komfort rovnako ako označenie na obale.",
                    "Priedušnosť navyše nie je synonymom savosti ani rýchlosti schnutia. Bavlnená priadza môže prijať vodu a viac vrstiev ju zadrží, hoci suchý výrobok pôsobí vzdušne. Polyesterová zmes môže vyschnúť rýchlejšie, ale správanie zmení povrchová úprava. Preto sa pri deke či osuške nepýtajte iba na vzdušnosť, ale aj na počet vrstiev, zloženie a podmienky použitia.",
                ],
            },
            {
                "heading": "Prečo sa mušelín po prvom praní zvlní alebo zrazí",
                "paragraphs": [
                    "Pri výrobe sú priadze napínané, látka vedená strojmi a povrch stabilizovaný dokončením. Voda, teplo a mechanika umožnia časti napätia uvoľniť sa. Otvorená bavlnená konštrukcia môže zmeniť dĺžku aj šírku a súčasne zvýrazniť typické zvlnenie. Viac vrstiev nemusia reagovať rovnakou rýchlosťou, najmä ak sa líši smer priadze alebo hustota.",
                    f"Rozmerová zmena nie je iba dôsledkom vysokej teploty. Ovplyvňuje ju predzrazenie, napätie, typ priadze, cyklus, odstreďovanie a sušenie. Podrobnejší mechanizmus vysvetľuje návod <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie zráža po praní</a>. Pri metrovom textile počítajte s odporúčaným predpraním pred strihaním; pri hotovom výrobku dodržte etiketu a nemerte rozmer, kým je kus nerovnomerne mokrý.",
                ],
                "callout": {
                    "title": "Ako odmerať zmenu bez skreslenia",
                    "items": [
                        "Pred prvým praním odmerajte suchý kus na rovnej ploche medzi rovnakými bodmi.",
                        "Zapíšte program, teplotu, odstreďovanie a spôsob sušenia.",
                        "Po praní nechajte kus úplné vyschnúť a prirodzene ho urovnajte bez naťahovania.",
                        "Porovnávajte dĺžku aj šírku; jedna hodnota nepopíše zmenu vrstiev ani reliéfu.",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Ako prať mušelínové plienky a detský textil",
                "paragraphs": [
                    "Najprv rozlíšte viacúčelovú plienku, osušku, zavinovačku a výrobok určený na priamy kontakt s pokožkou. Hygienický cieľ, povolená teplota a frekvencia prania sa môžu líšiť. Pred vložením do bubna odstráňte pevné nečistoty, plienku rozložte a neprepchávajte ju do sieťky s množstvom vrstiev, ktoré by obmedzili oplach. Dodržte pokyn výrobcu, najmä pri farebnej potlači.",
                    "Citlivá pokožka nie je dôvodom na svojvoľné znižovanie hygieny ani na nadmerné dávkovanie. Použite prostriedok určený pre daný textil a dávkujte podľa vody a náplne. Dôkladne vysušte každú vrstvu. Ak textil po praní zapácha, je slizký alebo dráždi, skontrolujte pranie, oplach a stav práčky; pach iba neprekrývajte ďalším voňavým produktom.",
                ],
            },
            {
                "heading": "Ako prať mušelínové šaty, blúzku a košeľu",
                "paragraphs": [
                    "Odev skontrolujte proti svetlu: nájdete tak vytiahnuté nite, rednúce miesta pri šve a rozdiely medzi jednou a dvoma vrstvami. Zapnite jemné viazanie, no sťahujúce šnúrky nezaviažte tak pevne, aby v mokrom stave vytvorili ryhu. Kus obráťte naruby a perte s podobne ľahkými vecami. Ťažké rifle a uteráky zvyšujú tlak aj mechanické trenie.",
                    "Po vypratí odev jemne vyrovnajte vo švoch a nechajte schnúť tak, aby ramená neniesli celú hmotnosť vody. Úzky vešiak môže na vlhkej jemnej látke vytlačiť hrany. Ak etiketa povoľuje žehlenie, pracujte z rubu a najprv skúste nižší stupeň. Pri prirodzene zvlnenom modeli zvážte len paru bez silného tlaku, aby ste neodstránili zamýšľanú textúru.",
                ],
            },
            {
                "heading": "Mušelínová deka, obliečka a prehoz: kontrola vnútornej vlhkosti",
                "paragraphs": [
                    "Objemný viacvrstvový kus môže po nasiaknutí prekročiť bezpečnú hmotnosť malej práčky. Kapacita uvedená v kilogramoch sa vzťahuje na podmienky výrobcu spotrebiča; jeden mokrý prehoz sa musí v bubne voľne pohybovať. Ak sa natlačí k dvierkam, oplach je nerovnomerný a odstreďovanie môže byť nevyvážené. Veľký kus preto patrí do primeranej kapacity alebo profesionálneho zariadenia.",
                    "Pri sušení prehoz niekoľkokrát otočte a zmeňte miesto preloženia. Prejdite rukou po okrajoch, švoch a miestach s viacerými vrstvami; chladnejší pocit môže signalizovať zvyškovú vlhkosť. Pred uložením nechajte textil ešte doschnúť v prúdení vzduchu. Vlhký mušelín uzavretý v skrini môže zatuchnúť aj vtedy, keď bol prací cyklus správny.",
                ],
            },
            {
                "heading": "Zatrhnutá niť a rozpojená vrstva: čo sa dá opraviť",
                "paragraphs": [
                    "Vytiahnutú slučku neodstrihujte hneď pri povrchu. Mohli by ste vytvoriť dva voľné konce a otvor zväčšiť. Kus položte bez napätia, sledujte smer nite a pri hodnotnom výrobku ju nechajte zatiahnuť krajčírovi na rub. Ak je pretrhnutý spoj medzi vrstvami, lokálna oprava musí vrstvy zachytiť bez tuhého uzla, ktorý by pri praní vytváral nový tlak.",
                    f"Rozširujúce sa rednúce miesto pri šve môže byť posun nití, nie iba prasknutá šijacia niť. Rozdiel vysvetľuje článok o <a href=\"{ARTICLE_SEAMS}\">pevnosti šva a posune nití</a>. Pri opakovanom zachytávaní odstráňte z pracej dávky odkryté zipsy a háčiky a skontrolujte poškodenie bubna. Opravený kus perte v ochrannom vrecku iba vtedy, ak sa v ňom môže dostatočne opláchnuť.",
                ],
            },
            {
                "heading": "Žehliť, naparovať alebo ponechať prirodzené zvlnenie",
                "paragraphs": [
                    "Hladký odevný mušelín a krčivá dvojitá gázovina nemajú rovnaký cieľ. Pri prvom môže byť jemné žehlenie z rubu žiaduce, pri druhom silný tlak sploští objem a dočasne zmení rozmery. Vždy sa riaďte symbolom žehličky a zložením. Syntetická alebo viskózová zmes môže mať nižšiu tepelnú hranicu než bežná bavlna.",
                    f"Ak chcete iba uvoľniť lokálny záhyb, vyskúšajte paru s odstupom a bez ťahania mokrého miesta. Najprv testujte na rubovej časti. Podrobné rozhodovanie podľa materiálu nájdete v návode <a href=\"{ARTICLE_IRONING}\">ako žehliť oblečenie</a>. Ak sa reliéf po žehlení vracia pri ďalšom praní, ide skôr o konštrukčnú vlastnosť než o nedostatočne vyžehlenie.",
                ],
            },
            {
                "heading": "Ako vybrať kvalitný mušelín bez jednoduchých skratiek",
                "paragraphs": [
                    "Pozrite si povrch proti svetlu, rovnomernosť priadzí, miesta spojenia vrstiev, okraje a švy. Jemnejší omak nie je automaticky dôkazom dlhšej životnosti; môže ho vytvárať pranie, mechanické zjemnenie alebo chemická úprava. Pri detskom a posteľnom textile sledujte, či sa neuvoľňujú vlákna, či spojovacie body nevytvárajú tvrdé hrčky a či výrobca uvádza zrozumiteľné zloženie a starostlivosť.",
                    "Ak porovnávate dva výrobky, pýtajte sa na počet vrstiev, rozmer po údržbe, prípadné predzrazenie a určené použitie. Nezamieňajte počet vrstiev s počtom nití a ani jedno číslo nepoužívajte ako samostatné hodnotenie. Dobrý kus je taký, ktorého konštrukcia zodpovedá funkcii: ľahká šatka, savá plienka a zimnejší prehoz potrebujú rozdielne vlastnosti.",
                ],
            },
        ],
        "table2_heading": "Diagnostika mušelínu po praní",
        "table2_intro": "Príznak má zvyčajne viac možných príčin. Najprv porovnajte etiketu, použitý cyklus a stav jednotlivých vrstiev; neopakujte agresívny postup naslepo.",
        "table2_headers": ["Príznak", "Možné vysvetlenie", "Čo overiť", "Bezpečný ďalší krok"],
        "table2_rows": [
            ("Kus je menší a viac zvlnený", "Uvoľnenie napätia, zrazenie vlákna alebo úpravy, teplo pri sušení.", "Rozmer pred praním, etiketu, program a sušičku.", "Po úplnom vyschnutí znovu odmerať; neťahať nasilu."),
            ("Vrstvy tvoria bubliny", "Rozdielna rozmerová zmena alebo poškodené spojovacie body.", "Či je problém lokálny, pri šve alebo v celej ploche.", "Stabilizovať poškodenie a pri hodnotnom kuse zvoliť opravu."),
            ("Povrch je tuhý alebo klzký", "Nadbytok prostriedku, nedostatočný oplach, tvrdá voda alebo úprava.", "Dávku, naplnenie, prívod vody a pokyn k oplachu.", "Pri povolenej starostlivosti zopakovať samotný oplach; nepridávať ďalší gél."),
            ("Po usušení ostal zatuchnutý pach", "Vlhkosť medzi vrstvami, dlhé ležanie v bubne alebo znečistená práčka.", "Okraje, preloženia, švy a tesnenie spotrebiča.", "Textil úplne vysušiť a odstrániť zdroj pachu, nie ho prekryť."),
            ("Vznikla diera alebo vytiahnutá niť", "Zachytenie o kovanie, pretrhnutý spoj vrstiev alebo oslabené miesto.", "Smer nite, stav zipsov, bubna a okolitú hustotu.", "Niť neodstrihnúť pri povrchu; miesto opraviť pred ďalším praním."),
        ],
        "steps_heading": "Bezpečný postup prania mušelínu krok za krokom",
        "steps": [
            "Prečítajte zloženie a všetky symboly, zistite počet vrstiev a skontrolujte ozdoby, lemy a spojovacie body.",
            "Odstráňte voľné nečistoty, lokálne ošetrite škvrnu po skúške farby a zatvorte prvky, ktoré sa môžu zachytiť.",
            "Oddeľte jemný kus od ťažkých uterákov, riflí, zipsov, háčikov a textílií, ktoré silno púšťajú vlákna.",
            "Zvoľte program a teplotu podľa etikety, nepreplňte bubon a použite presnú dávku kompatibilného prostriedku.",
            "Po cykle textil vyberte, podoprite jeho mokrú hmotnosť, jemne vyrovnajte švy a nežmýkajte ho krútením.",
            "Sušte s prúdením vzduchu, viacvrstvový kus otáčajte a pred uložením overte vnútornú vlhkosť.",
        ],
        "remember": [
            "Je to jedna tkaná vrstva, dvojitá gázovina alebo objemný viacvrstvový výrobok?",
            "Aké vlákna, farbivá, potlače, výšivky a spojovacie nite obsahuje hotový kus?",
            "Povoľuje etiketa zvolenú teplotu, odstreďovanie, sušičku a žehlenie?",
            "Má textil v bubne dostatok priestoru na pohyb a oplach medzi vrstvami?",
            "Je po sušení suchý aj v švoch, preloženiach a vnútorných vrstvách?",
        ],
        "mistakes": [
            "Predpokladať, že každý mušelín je stopercentná bavlna a znesie rovnakú teplotu.",
            "Prať jemnú otvorenú tkaninu so zipsami, háčikmi, suchými zipsami a ťažkými uterákmi.",
            "Naplniť bubon objemným prehozom tak, že voda a prostriedok nemôžu prechádzať vrstvami.",
            "Vytiahnuť mokré šaty za ramená alebo zavesiť ťažkú deku za jediný roh.",
            "Sploštiť prirodzený reliéf vysokou teplotou a silným tlakom bez kontroly etikety.",
            "Uložiť viacvrstvový textil po vysušení povrchu, hoci medzi vrstvami ostala vlhkosť.",
        ],
        "expert_heading": "Odbornejší pohľad: otvorená väzba, vrstvy a rozmerová zmena",
        "expert": [
            "Plátnová väzba pravidelne strieda previazanie osnovy a útku, no jej vzhľad a otvorenosť sa menia priadzou a dostavou. CottonWorks vysvetľuje základné tkané konštrukcie; z tejto schémy nemožno odvodiť konkrétnu pevnosť, savosť ani rozmerovú stabilitu neznámeho výrobku. Historický a obchodný pojem mušelín je širší než jedna presná dostava, preto treba opis doplniť reálnym zložením a vrstvením.",
            "ISO 7211-2:2024 a ASTM D3775-17(2023) opisujú určovanie počtu nití na jednotku dĺžky v tkanine. Ide o meranie konštrukcie, nie hodnotenie mäkkosti alebo bezpečnosti pre dieťa. Dvojitá gázovina navyše obsahuje dve plochy a spojovacie body, takže jedno číslo bez vysvetlenia vrstvy môže byť zavádzajúce. Podobne gramáž nevysvetľuje, koľko hmotnosti tvorí priadza a koľko konečná úprava.",
            "AATCC TM135-2025 poskytuje normalizované postupy merania rozmerovej zmeny po domácom praní. Norma pomáha porovnávať vzorky pri definovaných podmienkach, ale nepredpovedá každú kombináciu práčky, prostriedku, náplne a sušenia. Spotrebiteľský záver preto musí zostať praktický: postupujte podľa etikety, merajte za porovnateľných podmienok a neoznačujte prirodzené zvlnenie automaticky za zrazenie.",
        ],
        "source_intro": "Zdroje podporujú vysvetlenie plátnovej väzby, dostavy, rozmerovej zmeny a označovania. Nepodporujú tvrdenie, že všetok tovar predávaný ako mušelín má rovnaké vlákno, vrstvy alebo povolenú teplotu.",
        "sources": [
            ("CottonWorks: základné tkané konštrukcie", COTTONWORKS_WOVEN),
            ("AATCC TM135-2025: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
            ("ISO 7211-2:2024: počet nití na jednotku dĺžky", ISO_WOVEN_COUNT),
            ("ASTM D3775-17(2023): dostava tkaniny", ASTM_WOVEN_COUNT),
            ("EÚ 1007/2011: označovanie vláknového zloženia", EU_FIBRE_LABEL),
            ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ],
        "product_intro": "Pri bežnom prateľnom mušelíne rozhoduje zloženie, farba, vrstvy a etiketa. Jemnosť nevytvárajte nadmernou dávkou; dôležitejší je vhodný prostriedok, voľný bubon a dobrý oplach.",
        "product_text": "Hypoalergénny prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Pri mušelíne ho použite iba vtedy, keď etiketa hotového výrobku povoľuje bežný prací gél, a dávku prispôsobte vode a náplni.",
        "product_limit": "Produkt nie je automatickým riešením pre hodváb, vlnu, historický textil, špeciálne farbenie ani kus určený na profesionálne čistenie. Nenahrádza pokyn výrobcu a nezabráni konštrukčnému zrazeniu nesprávnym sušením.",
        "category_intro": "Výber pracieho gélu začnite etiketou mušelínu a až potom porovnávajte varianty pre bežnú bielizeň. Pri viacvrstvovom kuse je rovnako dôležitá presná dávka a úplné vypláchnutie.",
        "category_text": "V kategórii nájdete pracie gély pre rôzne potreby domácej bielizne. Zvoľte iba produkt kompatibilný so zložením, farbou a symbolmi konkrétneho mušelínového výrobku; odporúčané dávkovanie nepridávajte odhadom.",
        "related": [
            ("Ako predchádzať zatrhávaniu textilu", ARTICLE_SNAGGING),
            ("Čo je bavlna", ARTICLE_COTTON),
            ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
            ("Čo znamená gramáž látky", ARTICLE_GSM),
            ("Ako predchádzať zatrhávaniu", ARTICLE_SNAGGING),
            ("Ako sušiť oblečenie na vzduchu", ARTICLE_DRYING),
        ],
        "faq_title": "mušelín a jeho starostlivosť",
        "faq": [
            ("Je mušelín vždy zo stopercentnej bavlny?", "Nie. Historicky a veľmi často je bavlnený, ale obchodný názov sa používa aj na viskózové, syntetické a zmesové textílie. Rozhoduje údaj o zložení."),
            ("Na koľko stupňov prať mušelín?", "Univerzálna teplota neexistuje. Riaďte sa symbolom vaničky na hotovom výrobku; vrstva, farba, potlač alebo ozdoba môže mať nižší limit než samotné vlákno."),
            ("Prečo sa mušelín po praní pokrčí?", "Otvorená konštrukcia a rozdielne napätie priadzí sa po navlhčení uvoľnia. Pri krčivej dvojitej gázovine je zvlnenie často zámerné, no veľkú zmenu rozmeru treba odmerať."),
            ("Môže ísť mušelín do sušičky?", "Iba ak to povoľuje symbol sušenia. Teplo a prevaľovanie môžu zvýšiť zrazenie, opotrebovanie spojov vrstiev alebo zmenu reliéfu."),
            ("Treba mušelín žehliť?", "Nie vždy. Hladký typ možno žehliť podľa etikety, ale silný tlak môže sploštiť zámerné zvlnenie dvojitej gázoviny. Najprv testujte z rubu."),
            ("Prečo viacvrstvový mušelín po praní zapácha?", "Najčastejšie ostala vlhkosť medzi vrstvami alebo textil dlho ležal v bubne. Skontrolujte aj dávku, oplach a čistotu práčky; pach neprekrývajte ďalšou vôňou."),
        ],
    },
    {
        "title": "Čo je keper alebo twill: šikmá väzba, odolnosť a pranie",
        "link": "co-je-keper-alebo-twill-sikma-vazba-odolnost-a-pranie",
        "meta": "Čo je keper alebo twill, ako vzniká šikmé rebrovanie, čo väzba hovorí o odolnosti a ako prať keprové nohavice, košele či pracovné odevy.",
        "short": "Keper alebo twill je rodina tkanín so šikmým efektom, nie jedno vlákno. Zistite, čo znamená 2/1 a 3/1, ako posudzovať odolnosť a ako sa o keper starať.",
        "name": "keper",
        "genitive": "kepra",
        "construction_summary": "tkanú väzbu s pravidelným posúvaním preväzných bodov, ktoré na povrchu vytvára diagonálny smer",
        "label_details": "smer a výraznosť diagonály, farebný kontrast osnovy a útku, podiel elastanu, záhyby, výstuže a pracové reflexné prvky",
        "surface_object": "dlhšie preväzby a vystupujúce diagonálne rebro",
        "residue_place": "hustej tkanine, švoch, vreckách a viacnásobne prešitých lemoch",
        "drying_advice": "Nohavice zaveste rovnomerne za pevný pás alebo ich rozložte; záhyby a švy vyrovnajte bez násilného naťahovania diagonály.",
        "heat_risk": "zrazenie vlákna, blednutie vystupujúceho rebra, lesk, trvalé záhyby a pružnosť elastanu",
        "stain_risk": "farbu na vrcholoch rebra, smer povrchu a štruktúru dlhších preväzieb",
        "failure_sign": "pri švoch oddeľujú nite, objaví sa trhlina, ošúchané rebro alebo lokálne lesklé miesto",
        "answer": "Keper, po anglicky twill, je tkaná väzba, pri ktorej sa preväzné body postupne posúvajú a vytvárajú šikmé rebrovanie. Označenia 2/1 alebo 3/1 opisujú, ponad a popod koľko nití sa priadza v základnom opakovaní vedie; samy neurčujú vlákno ani celkovú kvalitu. Keper môže byť bavlnený, polyesterový, vlnený aj zmesový a patria doň denim, chino či niektoré gabardény. Diagonála môže zlepšiť splývavosť a v určitých konštrukciách odolnosť proti ďalšiemu trhaniu, nie však každú vlastnosť naraz. Pri praní sa riaďte etiketou hotového odevu, perte podobné farby, chráňte povrch pred trením a nevyvodzujte povolenú teplotu iba zo šikmého vzoru.",
        "intro": "Keprové nohavice môžu byť tenké a mäkké, iné tuhé a pracovné; oboje pritom ukazuje podobnú diagonálu. Rozdiel vytvára priadza, hustota, pomer previazania, smer, farbenie a dokončenie. Preto veta „keper je odolný“ potrebuje doplniť, voči akému namáhaniu, akou metódou a v ktorom smere. Pevnosť v ťahu, pokračovanie trhliny, oder, posun nití a pevnosť šva sú rozdielne vlastnosti. Tento návod vysvetľuje väzbu bez marketingových skratiek a prenáša ju do starostlivosti o nohavice, košele, posteľný aj pracovný textil.",
        "quick": [
            "<strong>Keper a twill znamenajú rovnakú rodinu väzieb:</strong> typickým znakom je posúvanejšia diagonála na povrchu.",
            "<strong>Nie je to vlákno:</strong> keper môže byť z bavlny, polyesteru, vlny, viskózy aj zo zmesi.",
            "<strong>3/1 nie je známka kvality:</strong> pomer opisuje opakovanie väzby, nie automaticky pevnosť, hrúbku ani cenu.",
            "<strong>Odolnosť treba pomenovať:</strong> trhanie, ťah, oder a posun nití sa skúšajú odlišne.",
            "<strong>Starostlivosť určuje hotový odev:</strong> vlákno, farba, elastan, záhyby a výstuhy majú prednosť pred názvom väzby.",
        ],
        "overview_heading": "Ako vzniká diagonála keprovej väzby",
        "overview": [
            "Pri tkaní sa osnova vedie pozdĺžne a útok prechádza naprieč. V plátnovej väzbe sa ich previazanie pravidelne strieda po jednej niti. Keper posúva miesto, kde priadza vystúpi nad alebo pod druhú sústavu, v každom ďalšom rade. Oko tieto posunuté body spája do šikmej línie. Diagonála môže smerovať doprava alebo doľava a jej uhol sa mení pomerom hustoty osnovy a útku, priadzou a návrhom.",
            "Zápis 2/1 zjednodušene znamená, že jedna sústava priadzí v opakovaní prechádza ponad dve a popod jednu niť druhej sústavy. Pri 3/1 je preväzba dlhšia. Presný vzhľad závisí od toho, či na líci dominuje osnova alebo útok. Dlhšie voľné úseky priadze, nazývané flotáže, umožnia iný omak a zakrytie povrchu, ale môžu byť citlivejšie na zachytenie a oder.",
            "Denim je známy osnovný keper s odlišne farbenou osnovou a útkom, ale nie každý keper je denim. Chino, drill, gabardén a serža označujú užšie typy alebo obchodné skupiny s vlastnou priadzou, hustotou a použitím. Keper je preto vhodné chápať ako konštrukčnú rodinu. Pri kúpe sa pýtajte aj na zloženie, hmotnosť, povrchovú úpravu, stálofarebnosť a určenie hotového výrobku.",
        ],
        "table1_heading": "Najčastejšie keprové konštrukcie a čo z nich možno vyčítať",
        "table1_intro": "Tabuľka vysvetľuje typické tendencie. Rovnaký zápis väzby môže pri inom vlákne, priadzi a hustote vytvoriť odlišný výrobok.",
        "table1_headers": ["Označenie", "Konštrukčný znak", "Bežný príklad", "Čo označenie negarantuje"],
        "table1_rows": [
            ("2/1 keper", "Kratšie opakovanie s dvoma preväzbami v jednom smere a jednou v druhom.", "Ľahšie nohavice, košele, podšívky a zmesové tkaniny.", "Konkrétnu pevnosť, zloženie, gramáž ani farebnú stálosť."),
            ("3/1 keper", "Dlhšia flotáž a výraznejšia prevaha jednej sústavy na líci.", "Denim, pracovné tkaniny a niektoré pevné nohavicoviny.", "Odolnosť voči každému typu oderu, trhnutia alebo prepichnutia."),
            ("Lomene a rybinové kepre", "Smer diagonály sa pravidelne obracia a vytvára cikcak alebo rybiu kosť.", "Kabáty, obleky, dekorácie a dizajnové textílie.", "Rovnakú starostlivosť; vlna, zmes aj lepený odev majú iné limity."),
            ("Osnovný a útkový keper", "Na povrchu preberá väčšiu plochu osnova alebo útok.", "Farebný kontrast denimu alebo jednoliaty keprový povrch.", "Rovnaké správanie v oboch smeroch a pri každom šve."),
        ],
        "sections": [
            {
                "heading": "Keper verzus plátnová a atlasová väzba",
                "paragraphs": [
                    "Plátnová väzba má časté preväzné body, krátke flotáže a spravidla jednoduchý rovnomerný povrch. Keper posúva preväzby do diagonály a umožňuje dlhšie flotáže. Atlasová alebo saténová väzba rozkladá preväzné body tak, aby boli menej zreteľné a povrch pôsobil hladšie. Rozdiel väzby mení omak a vzhľad, no bez priadze a hustoty neurčuje, ktorá látka vydrží dlhšie.",
                    "Pri domácej kontrole si pomôžte lupou a šikmým svetlom. Keper ukáže súvislé diagonálne rebro, plátno skôr pravidelnú mriežku a satén dlhšie hladké plochy. Kartáčovaný, brúsený alebo povrstvený povrch však môže väzbu zakryť. Pozorovanie preto nie je spoľahlivou identifikáciou vlákna ani zárukou, že látku možno prať doma.",
                ],
            },
            {
                "heading": "Znamená keprová väzba vyššiu pevnosť?",
                "paragraphs": [
                    "Dlhšie flotáže znamenajú menej miest, kde sa priadze v danom úseku vzájomne zovierajú. Pri určitom 3/1 kepre to môže umožniť priadzam pri pokračovaní trhliny preskupiť sa a zdieľať zaťaženie. Nie je to však univerzálny zákon. Jemnejšie vlákno, nižšia hustota, slabá priadza alebo chemicky poškodený povrch môžu výhodu zmeniť alebo obrátiť.",
                    f"Pevnosť v ťahu sa meria inak než sila potrebná na pokračovanie už založenej trhliny. Oder navyše hodnotí opakované povrchové namáhanie a šev pridáva ihlové otvory, niť a konštrukciu spoja. Podrobnejšie hranice sú v článku <a href=\"{ARTICLE_TENSILE}\">pevnosť textilu v ťahu a proti roztrhnutiu</a>. Bez názvu skúšky, jednotky a smeru nepoužívajte jedno číslo ako súhrnnú odolnosť.",
                ],
                "callout": {
                    "title": "Štyri odlišné otázky o odolnosti",
                    "items": [
                        "Pevnosť v ťahu: akú silu vzorka unesie pri kontrolovanom ťahaní.",
                        "Pokračovanie trhliny: akú silu treba na rast už pripraveného rezu alebo natrhnutia.",
                        "Oder: ako sa povrch mení pri opakovanom trení za definovaných podmienok.",
                        "Pevnosť a posun pri šve: ako spolupracuje látka, steh, niť, prídavok a smer dielu.",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Smer kepra: prečo sa látka nespráva rovnako v osnove a útku",
                "paragraphs": [
                    "Osnovné priadze bývajú pri tkaní viac napínané a môžu mať iný zákrut, pevnosť alebo farbu než útok. Väzba rozdeľuje flotáže a preväzné body asymetricky. Preto sa roztrhnutie, zrazenie aj ohyb môžu líšiť pozdĺž a naprieč. Odevný diel vystrihnutý mimo správneho smeru môže po praní visieť šikmo, hoci samotná látka nemá viditeľnú chybu.",
                    "Pri nohaviciach sledujte, či sa bočný šev po prvých praniach otáča dopredu, či sa záhyb posúva a či sa obe nohavice menia rovnako. Príčinou môže byť napätie priadze, strih, šitie aj rozmerová zmena. Nevyrovnávajte problém vysokou teplotou nasilu. Nový kus vypraný podľa etikety zdokumentujte a pri výraznej deformácii riešte s predajcom.",
                ],
            },
            {
                "heading": "Denim je keper, ale každý keper nie je denim",
                "paragraphs": [
                    "Klasický modrý denim používa keprovú väzbu, farebnú osnovu a svetlejší útok, preto sa líc a rub odlišujú. Pri nosení a praní sa farba z vystupujúcich osnovných priadzí odiera a vytvára typické zosvetlenie. Jednofarebný bavlnený keper na chino nohavice môže mať podobnú diagonálu, ale iné farbenie, hmotnosť a dokončenie, preto nebude starnúť rovnako.",
                    "Tmavé keprové nohavice perte naruby s podobnými farbami a bez zbytočne dlhého drsného cyklu. Pred prvým praním otestujte uvoľňovanie farby na skrytom mieste. Ak odev obsahuje elastan, vyhnite sa neoverenému vysokému teplu. Cieľom nie je zastaviť každú prirodzenú zmenu farby, ale obmedziť nerovnomerné blednutie a poškodenie, ktoré nesúvisí s bežným nosením.",
                ],
            },
            {
                "heading": "Ako prať keprové nohavice a chino",
                "paragraphs": [
                    "Vyprázdnite vrecká, zatvorte zips a gombíky podľa konštrukcie a nohavice obráťte naruby. Mastné okraje vreciek a pása lokálne ošetrite bez silného drhnutia diagonály. Ak majú nohavice ostrý záhyb, skontrolujte, či je lisovaný, šitý alebo iba vyžehlený; po praní sa každý typ obnovuje inak. Program a teplotu vyberte podľa etikety, nie podľa hrúbky na dotyk.",
                    "Po cykle zarovnajte pás, vnútorné švy a spodný lem. Nohavice zaveste tak, aby sa ich hmotnosť rozložila, alebo ich sušte naplocho, ak je mokrá zmes citlivá na vyťahanie. Pri žehlení tmavého kepra pracujte z rubu alebo cez ochrannú tkaninu. Silný tlak na vystupujúce rebro môže vytvoriť lesklé plochy, najmä pri syntetickej zmesi.",
                ],
            },
            {
                "heading": "Pracovný keper: nečistota, reflexné prvky a ochranná funkcia",
                "paragraphs": [
                    "Pracovný odev môže používať pevný bavlnený alebo polyesterovo-bavlnený keper, no reflexné pásky, nehorľavá úprava, antistatická vlastnosť a chemická ochrana majú vlastné obmedzenia. Bežná rada pre nohavice sa na certifikovaný ochranný odev nesmie preniesť. Kontaminácia olejom, rozpúšťadlom alebo nebezpečnou látkou môže vyžadovať firemný dekontaminačný postup namiesto domácej práčky.",
                    "Pred praním si prečítajte návod k ochrannému odevu, maximálny počet cyklov a podmienky obnovy úpravy. Aviváž, bielidlo, alkalita alebo vysoké teplo môžu ovplyvniť funkciu aj vtedy, keď tkanina vizuálne vyzerá neporušene. Po údržbe kontrolujte švy, pásky a označenia. Poškodený ochranný prvok nie je vhodné nahrádzať neoverenou domácou záplatou.",
                ],
            },
            {
                "heading": "Prečo sa keper leskne na kolenách, sedacej časti a hranách",
                "paragraphs": [
                    "Opakovaný tlak a trenie splošťujú vystupujúce priadze, odstraňujú jemné vlákna a menia smer odrazu svetla. Na tmavom hladšom kepre sa preto objaví lesk skôr, než vznikne diera. Horúca žehlička a silný tlak tento efekt urýchlia. Lesklé miesto nie je vždy mastná škvrna, a preto ho netreba automaticky opakovane odmasťovať agresívnym prostriedkom.",
                    "Najprv odev vyperte podľa etikety a po vysušení porovnajte povrch pri rozptýlenom svetle. Ak je zmena mechanická, pranie ju nevráti. Pri povolenom žehlení použite rub, ochrannú tkaninu a minimum tlaku potrebné na vyrovnanie. Pri obleku alebo vlnenom kepre môže byť vhodnejšia odborná para a tvarovanie než domáce plošné žehlenie.",
                ],
            },
            {
                "heading": "Trhlina, oder alebo posun nití pri šve",
                "paragraphs": [
                    "Trhlina prerúša priadze a môže sa ďalej šíriť. Oder postupne stenčuje alebo odstraňuje povrchové vlákna. Pri posune nití sa tkanina pri šve rozostúpi bez okamžitého pretrhnutia všetkých priadzí. Tieto prejavy vyzerajú odlišne a potrebujú odlišnú opravu. Samotné prešitie pôvodnej línie nemusí pomôcť, ak je okolitá látka oslabená.",
                    f"Pred ďalším praním miesto stabilizujte. Pri rozstrapkanej hrane odstráňte zdroj trenia, pri posune zväčšite plochu opravy a pri trhline spevnite jej konce bez vytvorenia tuhého bodu. Návod <a href=\"{ARTICLE_SEAMS}\">prečo oblečenie praská pri švoch</a> pomôže odlišiť chybu stehu od správania tkaniny. Nosný, ochranný alebo hodnotný kus zverte odbornej oprave.",
                ],
            },
            {
                "heading": "Ako vybrať keprovú tkaninu pre konkrétne použitie",
                "paragraphs": [
                    "Na letnú košeľu hľadajte jemnejšiu priadzu, nižšiu hmotnosť a dobrý omak; na pracovné nohavice potrebujete rovnováhu oderu, pevnosti švov, pohodlia a opraviteľnosti. Na oblek je dôležitý pád, tvarovanie a lesk, pri pošťahovej tkanine stálosť povrchu a spôsob čistenia. Jeden „silný“ keper nemôže byť najlepší vo všetkých týchto úlohách.",
                    "Pri porovnávaní si vypýtajte zloženie, gramáž, zápis väzby, dostavu, povrchovú úpravu a relevantné výsledky skúšok. Skontrolujte látku v oboch smeroch, ohnite ju, prezrite proti svetlu a jemne trite bielou handričkou iba ako orientačnú kontrolu farby. Domáce pozorovanie nenahrádza laboratórium, ale odhalí nerovnomernú väzbu, voľné nite a nevhodný povrch skôr, než sa z látky ušije výrobok.",
                ],
            },
        ],
        "table2_heading": "Čo prezrádza zmena keprového odevu po praní",
        "table2_intro": "Jeden príznak nemusí mať jedinú príčinu. Hodnoťte smer diagonály, zloženie, švy, farbu a použitý cyklus spolu.",
        "table2_headers": ["Príznak", "Možné príčiny", "Rozlišovacia kontrola", "Ďalší krok"],
        "table2_rows": [
            ("Bočný šev sa otáča", "Smer dielu, napätie priadze, strih, šitie alebo nerovnomerné zrazenie.", "Položiť suchý odev bez naťahovania a porovnať obe strany.", "Nový kus zdokumentovať; neopravovať vysokou teplotou nasilu."),
            ("Hrany a kolená sa lesknú", "Sploštený povrch trením, tlakom alebo žehličkou.", "Porovnať pri rozptýlenom svetle po bežnom vyčistení.", "Obmedziť tlak a žehliť z rubu; mechanickú zmenu nemožno vyprať."),
            ("Pri šve vznikajú medzery", "Posun nití, príliš malý prídavok alebo lokálne preťaženie.", "Skontrolovať, či sú priadze celé a iba odsunuté.", "Miesto nezaťažovať a opravu rozložiť do väčšej plochy."),
            ("Tmavá farba je pruhovaná", "Oder vystupujúceho rebra, preplnený bubon, zlé triedenie alebo prenos farby.", "Rozlíšiť zosvetlenie vlákna od cudzieho povlaku.", "Prať naruby s podobnými farbami; neprekrývať nerovnosť farbivom bez testu."),
            ("Odev je tuhý a ťažko sa ohýba", "Zvyšky prostriedku, tvrdá voda, presušenie alebo zmena úpravy.", "Dávku, oplach, etiketu a rovnomernosť po celej ploche.", "Pri povolení doplnkový oplach; nevynucovať mäkkosť avivážou."),
        ],
        "steps_heading": "Bezpečný postup prania keprového odevu",
        "steps": [
            "Zistite zloženie, symboly, podiel elastanu, farebnú úpravu a všetky funkčné alebo lepené prvky hotového odevu.",
            "Vyprázdnite vrecká, zatvorte kovanie, tmavý odev obráťte naruby a lokálne ošetrite škvrny bez drhnutia rebra.",
            "Triedite podľa farby, hmotnosti a citlivosti; jemný keper oddeľte od hrubých zipsov, uterákov a veľmi špinavého pracovného textilu.",
            "Použite povolený program, teplotu a otáčky, nechajte bubon voľný a dávkujte kompatibilný prostriedok podľa vody a náplne.",
            "Po cykle zarovnajte švy, pás a záhyby bez ťahania diagonály; sušte spôsobom povoleným na etikete.",
            "Po vysušení skontrolujte farbu, lesk, švy a trhliny; pri žehlení postupujte z rubu a chráňte povrch pred silným tlakom.",
        ],
        "remember": [
            "Je označenie keper doplnené presným vláknovým zložením a symbolmi?",
            "Obsahuje odev elastan, podšívku, lisovaný záhyb, reflexné alebo ochranné prvky?",
            "Porovnávate rovnaký typ odolnosti, rovnakú metódu, smer a jednotku?",
            "Je tmavý odev obrátený naruby a oddelený od drsných či silno púšťajúcich kusov?",
            "Sú švy a najviac namáhané hrany stabilné pred vložením do bubna?",
        ],
        "mistakes": [
            "Považovať keper za druh bavlny a vybrať teplotu bez kontroly zmesi a etikety.",
            "Tvrdiť, že 3/1 je vždy pevnejší než 2/1 bez metódy, priadze, hustoty a smeru.",
            "Prať tmavé chino alebo denim lícom von s drsnými kusmi a preplneným bubnom.",
            "Odmasťovať lesklé kolená ako škvrnu, hoci ide o mechanicky sploštený povrch.",
            "Použiť bežnú starostlivosť na certifikovaný pracovný odev s funkčnou úpravou.",
            "Prežehliť tmavé rebro vysokou teplotou a silným tlakom priamo z líca.",
        ],
        "expert_heading": "Odbornejší pohľad: flotáž, dostava a skúška trhania",
        "expert": [
            "CottonWorks opisuje keper ako základnú tkanú konštrukciu s diagonálnymi líniami a samostatne vysvetľuje 2/1 a 3/1 varianty. Dlhšia flotáž mení pohyblivosť priadze a počet preväzných bodov, ale jej účinok závisí od priadze a hustoty. Z názvu twill preto nemožno odvodiť laboratórnu hodnotu ani garantovanú životnosť hotového odevu.",
            "ASTM D1424-25 opisuje kyvadlovú metódu sily potrebnej na pokračovanie jednej trhliny po reze v tkanine. Výsledok nie je pevnosť v ťahu, odolnosť proti prepichnutiu ani oderu. Nie každá tkanina a každý smer sú pre metódu rovnako vhodné. Pri porovnávaní kepra preto musí dodávateľ uviesť metódu, orientáciu vzorky, jednotku a podmienky, inak je číslo bez odbornej hranice.",
            "ISO 7211-2:2024 a ASTM D3775-17(2023) určujú dostavu tkaniny. Dostava spolu s hrúbkou priadze ovplyvňuje zakrytie, hmotnosť a pohyb priadzí, no ani vysoký počet nití nie je samostatným hodnotením kvality. EÚ označovanie vlákien a symboly GINETEX dopĺňajú iný typ informácie: z čoho je hotový kus a ako ho možno ošetrovať. Odborné rozhodnutie spája väzbu, priadzu, zloženie, dokončenie, skúšku a použitie.",
        ],
        "source_intro": "Zdroje podporujú konštrukčné vysvetlenie kepra, meranie dostavy a presne vymedzenú skúšku pokračovania trhliny. Nepodporujú tvrdenie, že každá diagonálna tkanina je automaticky odolnejšia pri každom namáhaní.",
        "sources": [
            ("CottonWorks: twill alebo keprová väzba", COTTONWORKS_TWILL),
            ("CottonWorks: základné tkané konštrukcie", COTTONWORKS_WOVEN),
            ("ASTM D1424-25: kyvadlová skúška trhania", ASTM_TEAR),
            ("ISO 7211-2:2024: počet nití na jednotku dĺžky", ISO_WOVEN_COUNT),
            ("EÚ 1007/2011: označovanie vláknového zloženia", EU_FIBRE_LABEL),
            ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ],
        "product_intro": "Pri bavlnenom alebo zmesovom keprovom odeve určenom na bežné domáce pranie môže byť vhodný prací gél. Najprv však overte farbu, elastan, funkčnú úpravu a symboly celého odevu.",
        "product_text": "Hypoalergénny prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Na kompatibilný keper ho dávkujte podľa tvrdosti vody, veľkosti a znečistenia náplne; tmavý kus perte naruby.",
        "product_limit": "Produkt nie je automaticky vhodný pre vlnený oblek, odev určený na profesionálne čistenie ani certifikovaný ochranný textil. Neobnovuje oder, pretrhnutú priadzu, reflexný prvok ani funkčnú úpravu.",
        "category_intro": "Pri porovnávaní pracích gélov zohľadnite farbu, vláknovú zmes, pracovné znečistenie a pokyn k ochranným prvkom. Hrubý keper nie je dôvod na automatické zvýšenie dávky.",
        "category_text": "Kategória obsahuje pracie gély pre bežnú domácu bielizeň. Zvoľte variant kompatibilný s etiketou konkrétneho keprového odevu a pri funkčnom pracovnom textile dodržte osobitný návod výrobcu.",
        "related": [
            ("Pevnosť textilu v ťahu a proti roztrhnutiu", ARTICLE_TENSILE),
            ("Pevnosť šva a posun nití", ARTICLE_SEAMS),
            ("Čo je bavlna", ARTICLE_COTTON),
            ("Polyester verzus bavlna", ARTICLE_BLEND),
            ("Prečo farby blednú", ARTICLE_COLORFASTNESS),
            ("Ako žehliť oblečenie", ARTICLE_IRONING),
        ],
        "faq_title": "keper, twill a starostlivosť",
        "faq": [
            ("Je keper to isté ako twill?", "Áno, ide o slovenské a anglické pomenovanie rovnakej rodiny tkaných väzieb s posúvanými preväznými bodmi a typickou diagonálou."),
            ("Je každý keper z bavlny?", "Nie. Keprovú väzbu možno utkať z bavlny, vlny, polyesteru, viskózy aj zmesí. Presné zloženie uvádza etiketa."),
            ("Čo znamená keper 3/1?", "Opisuje základné opakovanie väzby s dlhšou preväzbou jednej sústavy priadzí. Nie je to univerzálna trieda pevnosti ani kvality."),
            ("Je keper odolnejší než plátnová tkanina?", "V niektorých konštrukciách a pri niektorých skúškach môže mať výhodu, no rozhoduje priadza, hustota, smer a metóda. Bez týchto údajov sa nedá urobiť všeobecný rebríček."),
            ("Ako prať keprové nohavice?", "Podľa etikety, s podobnými farbami a hmotnosťou. Tmavý odev obráťte naruby, nepreplňte bubon a po praní vyrovnajte švy bez násilného ťahania."),
            ("Prečo sa keprové nohavice lesknú?", "Tlak, trenie a horúca žehlička sploštia vystupujúce priadze a zmenia odraz svetla. Mechanický lesk pranie zvyčajne neodstráni."),
        ],
    },
    {
        "title": "Čo je velúr: vlasový povrch, rozdiel od zamatu a starostlivosť",
        "link": "co-je-velur-vlasovy-povrch-rozdiel-od-zamatu-a-starostlivost",
        "meta": "Čo je velúr, čím sa líši od zamatu, semišu a froté, prečo mení odtieň podľa smeru vlasu a ako prať velúrové oblečenie bez otlačenia.",
        "short": "Velúr je mäkký vlasový textil s viacerými konštrukciami a zloženiami. Naučte sa odlišiť ho od zamatu, chrániť smer vlasu a riešiť otlačenie, žmolky aj škvrny.",
        "name": "velúr",
        "genitive": "velúru",
        "construction_summary": "textilný povrch s krátkym mäkkým vlasom, ktorý býva v odevnej praxi často pletený, ale obchodné použitie názvu nie je na všetkých trhoch jednotné",
        "label_details": "smer vlasu, pletený alebo tkaný základ, podšívku, výplň, elastan, potlač, embosovanie a zákaz kefovania",
        "surface_object": "vlasové slučky a jemné vlákna na líci",
        "residue_place": "hustom vlase, rubovom úplete, švoch a preložených lemoch",
        "drying_advice": "Odev sušte bez štipcov na líci a bez dlhého pritlačenia vlasu k tyči; tvar podoprite a po vyschnutí upravte povrch iba spôsobom povoleným výrobcom.",
        "heat_risk": "smer a výšku vlasu, lesk, elastický základ, embosovanie, lepidlá a rozmer odevu",
        "stain_risk": "smer vlasu, odraz svetla a pevnosť vlasových vlákien v základe",
        "failure_sign": "vlas uvoľňuje v chumáčoch, vznikajú holé miesta, vrstvy sa oddeľujú alebo farba migruje pri jemnom teste",
        "answer": "Velúr je textília s mäkkým vlasovým povrchom, často vytvorená ako pletenina, kým klasický zamat je typicky tkaná vlasová látka. V obchode sa však názvy používajú nejednotne, preto rozhoduje reálna konštrukcia, zloženie a etiketa. Velúr môže byť bavlnený, polyesterový, viskózový alebo zmesový a môže obsahovať elastan. Pred praním odev obráťte naruby, oddeľte ho od zipsov a textílií púšťajúcich vlákna, použite povolený jemný cyklus a nepreplňte bubon. Vlas nedrhnite, nežmýkajte a nežehlite priamo silným tlakom. Otlačenie najprv nechajte oddýchnuť; para a kefa sú vhodné iba vtedy, keď ich výrobca povoľuje.",
        "intro": "Velúr mení odtieň pri pohladení, pretože vlas sa nakloní a inak odrazí svetlo. Táto vlastnosť vytvára mäkkosť a hĺbku farby, ale aj citlivosť na tlak, trenie a nesprávne sušenie. Problémom je, že rovnaký názov sa používa na teplákový pletený velúr, pošťahovú textíliu, bavlnený detský odev aj syntetický dekoračný povrch. Jedna rada preto nemôže platiť pre všetky. Najprv treba zistiť základ, vlas, vláknové zloženie a konštrukciu hotového výrobku; až potom sa rozhoduje o vode, prostriedku, pare alebo profesionálnom čistení.",
        "quick": [
            "<strong>Velúr nie je semiš:</strong> velúr je textília, kým semiš je brúsená koža alebo jej napodobenina s iným podkladom.",
            "<strong>Velúr a zamat sa prekrývajú iba v bežnej reči:</strong> klasický zamat je spravidla tkaný, odevný velúr často pletený.",
            "<strong>Svetlejší fľak nemusí byť strata farby:</strong> zmenený smer vlasu odráža svetlo inak.",
            "<strong>Vlas sa chráni pred tlakom:</strong> štipce, preplnený bubon, horúca žehlička a dlhé zloženie môžu zanechať otlačenie.",
            "<strong>Najcitlivejší diel rozhoduje:</strong> podšívka, lepidlo, pena alebo výstuha môže vylúčiť domáce pranie.",
        ],
        "overview_heading": "Čo je vlasový povrch a ako velúr spoznáte",
        "overview": [
            "Vlas tvorí množstvo krátkych koncov alebo slučiek, ktoré vystupujú nad základnú textilnú plochu. Pri strihanom vlase sú slučky otvorené a ich konce vytvárajú mäkký povrch. Pri slučkovom povrchu zostávajú uzavreté. Velúrový vzhľad môže vzniknúť pletením, tkaním, brúsením alebo technikou, ktorá vlákna upevní na podklad. Od spôsobu upevnenia závisí, ako povrch znáša ohyb, ťah, pranie a oder.",
            "Smer vlasu, označovaný aj ako nap, je viditeľný pri prejdení dlaňou. V jednom smere pôsobí povrch hladšie a tmavšie, v opačnom svetlejšie alebo drsnejšie. Pri strihaní odevu musia diely smerovať rovnako, inak rukáv vyzerá farebne odlišne aj z rovnakej role. Pri starostlivosti je smer užitočný na rozlíšenie bežnej optickej zmeny od odratého alebo zlepeného vlasu.",
            "Pletený velúr býva pružnejší a používa sa na teplákové súpravy, detské oblečenie a domáce odevy. Tkaná vlasová textília môže byť stabilnejšia a častejšia pri dekorácii alebo formálnejšom odeve. Flockovaný povrch má krátke vlákna upevnené na podklad spojivom. Tieto tri konštrukcie nemožno prať podľa jednej rady, hoci na prvý dotyk pôsobia podobne.",
        ],
        "table1_heading": "Velúr, zamat, froté a semiš: praktické rozdiely",
        "table1_intro": "Názvy v obchode sa môžu prekrývať. Tabuľka pomáha položiť správne otázky, nie nahradiť etiketu alebo technický list.",
        "table1_headers": ["Materiál alebo povrch", "Typický základ", "Povrch", "Hlavná hranica pri údržbe"],
        "table1_rows": [
            ("Velúr", "Často pletený textil, existujú aj iné konštrukcie.", "Krátky mäkký vlas s viditeľným smerom.", "Overiť základ, vlákno, elastan a spôsob upevnenia vlasu."),
            ("Zamat", "Klasicky tkaná vlasová konštrukcia, obchodne aj širšie použitie.", "Hustý strihaný vlas a výrazná hra svetla.", "Často citlivý na tlak; hodnotný odev môže vyžadovať odborné čistenie."),
            ("Froté", "Tkaný alebo pletený textil so slučkami.", "Neostrihané slučky určené najmä na savosť.", "Slučky sa zachytávajú; starostlivosť o uterák nie je návodom pre velúr."),
            ("Semiš a imitácia semišu", "Brúsená koža alebo syntetický/mikrovláknový podklad.", "Jemný brúsený povrch bez klasickej textilnej vlasovej konštrukcie.", "Koža, laminát a mikrovlákno potrebujú rozdielne čistenie."),
            ("Flock", "Textilný alebo netextilný podklad so spojivom.", "Krátke vlákna uložené na povrch.", "Voda, trenie a rozpúšťadlo môžu ovplyvniť lepidlo."),
        ],
        "sections": [
            {
                "heading": "Velúr verzus zamat: rozdiel nie je iba v jazyku",
                "paragraphs": [
                    "Woolmark pri svojej materiálovej inovácii rozlišuje tkaný velvet a pletený velour, čo zodpovedá bežnému technickému rozdeleniu. Maloobchodné názvy však nemusia byť dôsledné a v slovenčine sa obe slová niekedy používajú pre mäkký vlasový vzhľad. Preto sa pri odevnom čistení nepýtajte iba „je to velúr alebo zamat?“, ale aj „je základ pletený, tkaný alebo lepený?“.",
                    f"Pri tkanom vlasovom odeve venujte osobitnú pozornosť tlaku a vytiahnutým vláknam; praktické mechanické riziká rozoberá návod <a href=\"{ARTICLE_SNAGGING}\">ako predchádzať zatrhávaniu textilu</a>. Tento text sa sústreďuje na velúr ako širšiu skupinu, najmä na prateľné pletené odevy. Ak etiketa uvádza profesionálne čistenie, klasifikácia z internetu toto obmedzenie neruší.",
                ],
            },
            {
                "heading": "Prečo velúr mení odtieň po pohladení",
                "paragraphs": [
                    "Naklonené vlákna odrážajú svetlo pod iným uhlom a časť svetla zachytávajú medzi sebou. Preto sa ten istý farebný povrch javí tmavší alebo svetlejší bez zmeny pigmentu. Pri hodnotení fľaku prejdite po suchom povrchu mäkkou dlaňou v povolenom smere a pozrite sa z viacerých uhlov. Ak sa odtieň mení spolu so smerom, môže ísť o normálny nap.",
                    "Trvalé zosvetlenie ostáva viditeľné aj po urovnaní vlasu a môže súvisieť s oderom, stratou vlákien alebo farbiva. Zlepený povrch pôsobí tvrdšie a jednotlivé chumáčiky sa neoddeľujú. Mastná škvrna môže byť tmavšia a meniť omak. Diagnostika pred čistením je dôležitá, pretože agresívne drhnutie optické otlačenie premení na skutočné poškodenie vlasu.",
                ],
                "callout": {
                    "title": "Rýchla kontrola svetlého miesta",
                    "items": [
                        "Nechajte povrch úplne vyschnúť a pozorujte ho pri rozptýlenom svetle z dvoch smerov.",
                        "Jemne zmeňte smer vlasu dlaňou; nepoužívajte tvrdú kefu bez povolenia výrobcu.",
                        "Porovnajte omak, hustotu a výšku vlasu so susednou plochou, nie iba farbu.",
                        "Ak chýbajú vlákna alebo je podklad obnažený, ďalšie mokré čistenie zastavte.",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Pletený velúr: pružnosť, rast a skrútenie",
                "paragraphs": [
                    "Pletený základ sa prispôsobuje pohybu a môže obsahovať elastan. Vlas pridáva hmotnosť a trenie, preto sa tepláky môžu pri dlhom nosení vytiahnuť na kolenách alebo sedacej časti. Po namočení sa hmotnosť zvýši a úzky vešiak vytvorí body napätia. Pranie môže časť dočasnej deformácie uvoľniť, neobnoví však unavený elastan ani poškodené očká.",
                    "Pred praním obráťte odev naruby, zatvorte zips a oddeľte ho od háčikov. Jemnejší cyklus a primerané otáčky znižujú trenie, ale musia byť povolené etiketou. Po praní zarovnajte bočné švy, pás a manžety, kým je kus vlhký, bez naťahovania do väčšieho rozmeru. Ak sa švy nového odevu výrazne skrútia, môže ísť o strih alebo stabilitu základu.",
                ],
            },
            {
                "heading": "Bavlnený, polyesterový a viskózový velúr",
                "paragraphs": [
                    "Bavlnený velúr prijíma vlhkosť a môže byť príjemný na domáce oblečenie, no jeho rozmer a farba reagujú na dokončenie a sušenie. Polyesterový variant zvykne rýchlejšie schnúť, ale môže zadržiavať mastné nečistoty, tvoriť statický náboj a byť citlivý na vysokú teplotu. Viskóza pridáva lesk a splývavosť, pri navlhčení však potrebuje opatrnú manipuláciu.",
                    f"Pri zmesi sa nespoliehajte na najväčší percentuálny podiel. Menšie množstvo elastanu, vlas z jedného vlákna a základ z druhého alebo citlivá podšívka môžu určiť bezpečný limit. Vlastnosti vlákien podrobnejšie vysvetľujú články <a href=\"{ARTICLE_COTTON}\">čo je bavlna</a>, <a href=\"{ARTICLE_POLYESTER}\">čo je polyester</a> a <a href=\"{ARTICLE_VISCOSE}\">čo je viskóza</a>.",
                ],
            },
            {
                "heading": "Ako prať velúrové tepláky, mikinu a pyžamo",
                "paragraphs": [
                    "Odev obráťte naruby, aby sa lícny vlas menej triel o bubon a ostatnú bielizeň. Zips zatvorte a prekryte, šnúrky zabezpečte a skontrolujte suché zipsy na iných kusoch. Perte s podobnou farbou a hmotnosťou; bavlnené uteráky môžu uvoľňovať vlákna, ktoré sa vo vlasovom povrchu zachytia. Nepoužívajte vyššiu teplotu iba preto, že ide o domáci odev.",
                    "Dávkujte prostriedok presne a nepreplňte bubon, aby sa z hustého povrchu vypláchli nečistoty. Po cykle kus vyberte bez žmýkania, zarovnajte tvar a sušte podľa etikety. Ak je povolená sušička, zvoľte určený režim a nenechávajte odev zbytočne presušiť. Vysoké teplo môže ovplyvniť syntetický vlas a elastan skôr, než sa zmení bavlnená časť.",
                ],
            },
            {
                "heading": "Ako čistiť velúrové pošťahové kreslo alebo čelo postele",
                "paragraphs": [
                    "Najprv nájdite štítok nábytku a pokyn výrobcu poťahu. Pošťahový velúr môže byť nalepený, podložený penou, neodnímateľný alebo ošetrený proti škvrnám. Veľké množstvo vody môže preniknúť do výplne, vytvoriť mapu, uvoľniť farbu alebo predĺžiť schnutie. Univerzálny prací postup pre mikinu sa na nábytok nevzťahuje.",
                    "Voľný prach odstráňte nízkym výkonom a vhodným hladkým nadstavcom v smere vlasu bez pritlačenia. Škvrnu odsávajte bielou handričkou a kompatibilný prostriedok otestujte na skrytom mieste vrátane zaschnutia. Nezmáčajte iba stred fľaku; ostrý okraj mokrej mapy môže ostať viditeľný. Pri neznámom kóde, hodnotnom nábytku alebo veľkom znečistení zvoľte profesionálne čistenie.",
                ],
            },
            {
                "heading": "Ako uvoľniť otlačený vlas bez poškodenia",
                "paragraphs": [
                    "Najprv odstráňte tlak a nechajte textíliu niekoľko hodín voľne odpočívať. Ľahké otlačenie sa môže čiastočne zdvihnúť samo. Ak výrobca povoľuje paru, použite ju s odstupom z rubu alebo bez priameho dotyku a vlas nedrvte žehličkou. Po vychladnutí ho upravte iba mäkkou kefou určenou pre daný povrch a v smere, ktorý nevyťahuje vlákna.",
                    "Nie každý otlak sa dá odstrániť. Ak sú vlákna zlomené, zlepené teplom alebo vytrhnuté zo základu, para ich neobnoví. Syntetický vlas sa môže pri vysokej teplote trvalo zdeformovať a flockovaný povrch môže reagovať na vlhkosť spojiva. Pred zásahom preto identifikujte konštrukciu a testujte na neviditeľnom mieste.",
                ],
            },
            {
                "heading": "Žmolky, matovanie a strata vlasu nie sú to isté",
                "paragraphs": [
                    "Žmolok je spletený zhluk vlákien držiaci na povrchu. Matovanie je zľahnutie a zapletenie vlasu do súvislejšej plochy. Strata vlasu znamená, že vlákna alebo slučky opustili základ a vzniká redšie či holé miesto. Odžmolkovač môže odstrániť voľný zhluk, ale na vlasovom textile môže súčasne zrezať zdravý povrch. Bez skúšky ho nepoužívajte.",
                    f"Pri matovaní najprv odstráňte zvyšky prostriedku a nechajte kus vyschnúť. Kefu alebo paru použite iba pri povolení. Ak vlas vypadá v chumáčoch, ďalšie mechanické čistenie zastavte. Všeobecný mechanizmus tvorby zhlukov vysvetľuje článok <a href=\"{ARTICLE_PILLING}\">prečo oblečenie žmolkuje</a>, ale vlasový povrch vyžaduje jemnejšiu diagnostiku než hladké tričko.",
                ],
            },
            {
                "heading": "Skladovanie velúru bez hrán, otlačení a prachu",
                "paragraphs": [
                    "Odev pred uložením úplne vysušte a odstráňte prach, ktorý by sa pod tlakom vtlačil do vlasu. Ťažký pletený velúr skladajte voľne alebo ho zaveste na široký podopretý vešiak podľa tvaru. Nevytvárajte ostrý sklad cez lícnu viditeľnú plochu a neukladajte naň ťažké predmety. Plastový nepriedušný obal na zvyškovo vlhkom textile podporí zatuchnutie.",
                    "Pošťah chráňte pred dlhodobým bodovým tlakom nôh, ostrých hrán a predmetov uložených na sedadle. Pravidelne odstraňujte voľný prach v súlade s návodom. Pri presúvaní nábytku povrch neobaľujte hrubou textíliou priamo proti vlasu; vložte hladkú ochrannú vrstvu. Prevencia je spoľahlivejšia než neskoršie naparovanie hlbokého otlačenia.",
                ],
            },
            {
                "heading": "Ako posudzovať kvalitu velúru",
                "paragraphs": [
                    "Skontrolujte rovnomernosť smeru a výšky vlasu, viditeľné pruhy, holé miesta, okraje a rubový základ. Jemne prejdite dlaňou v oboch smeroch a sledujte, či sa povrch vracia bez uvoľňovania chumáčov. Vyšší alebo hustejší vlas nie je automaticky lepší; pri častom sedení môže byť rozhodujúca schopnosť udržať vlas, odolnosť základu a jednoduché čistenie.",
                    "Ak výrobca uvádza skúšku oderu alebo vlasu, požadujte metódu a podmienky. Výsledok na tkanom pošťahovom velvete nemožno automaticky preniesť na pletenú mikinu. Sledujte aj stálofarebnosť pri trení, rozmerovú zmenu, žmolkovanie, pevnosť švov a návod na údržbu. Kvalita je vhodnosť pre konkrétne použitie, nie jediné najvyššie číslo.",
                ],
            },
        ],
        "table2_heading": "Diagnostika velúrového povrchu",
        "table2_intro": "Pred čistením rozlíšte optickú zmenu smeru, nečistotu, zlepenie a skutočnú stratu vlasu. Nesprávny zásah môže mierny problém zafixovať.",
        "table2_headers": ["Prejav", "Pravdepodobné vysvetlenie", "Ako ho rozlíšiť", "Bezpečný postup"],
        "table2_rows": [
            ("Svetlá plocha bez zmeny omaku", "Vlas je otočený iným smerom.", "Odtieň sa mení pri pohľade a jemnom urovnaní.", "Nechať odpočinúť; upraviť iba povolenou mäkkou metódou."),
            ("Tvrdé zlepené chumáčiky", "Zvyšok prostriedku, mastnota, teplo alebo nevhodná chémia.", "Porovnať omak a skontrolovať, či sa povrch po vyschnutí oddeľuje.", "Nedrhnúť; identifikovať príčinu a testovať kompatibilné čistenie."),
            ("Holé miesto s viditeľným podkladom", "Vlas je odretý, vytrhnutý alebo sa uvoľnilo spojivo.", "Smerovanie povrchu hustotu neobnoví.", "Zastaviť mechanické namáhanie; zvážiť odbornú opravu alebo reklamáciu."),
            ("Drobné guľôčky na povrchu", "Žmolky z vlastných alebo prenesených vlákien.", "Skontrolovať, či zhluk drží na vlase a či chýba materiál pod ním.", "Oddeľiť zdroj vlákien; odžmolkovač použiť iba po bezpečnom teste."),
            ("Zatuchnutý pach v hrubom odeve", "Nedostatočné vysušenie vlasu, podšívky alebo švov.", "Skontrolovať chladné vlhké miesta a čas medzi praním a sušením.", "Vysušiť s prúdením vzduchu a odstrániť príčinu, nie pach prekryť."),
        ],
        "steps_heading": "Bezpečný postup prania prateľného velúrového odevu",
        "steps": [
            "Zistite, či je základ pletený, tkaný alebo lepený, prečítajte zloženie, symboly a pokyny k vlasu, podšívke a ozdobám.",
            "Skontrolujte uvoľnený vlas, švy a škvrny; lokálny prostriedok otestujte na skrytom mieste vrátane úplného zaschnutia.",
            "Odev obráťte naruby, zatvorte zachytávajúce prvky a oddeľte ho od zipsov, háčikov, uterákov a textílií púšťajúcich vlákna.",
            "Použite povolený program, teplotu a otáčky, nechajte voľný bubon a dávkujte kompatibilný prostriedok presne.",
            "Po cykle kus nežmýkajte, podoprite jeho tvar a sušte bez štipcov alebo tyče pritlačenej k viditeľnému vlasu.",
            "Povrch upravujte až po vyschnutí a iba povolenou parou či kefou; pred uložením odstráňte tlak a overte suché švy.",
        ],
        "remember": [
            "Je povrch pletený, tkaný, brúsený alebo flockovaný a čo uvádza etiketa?",
            "Je svetlejšie miesto iba otočený vlas, zlepenie, mastnota alebo skutočná strata vlákien?",
            "Obsahuje odev elastan, podšívku, výstuhu, penu alebo lepidlo s nižšou hranicou?",
            "Je velúr oddelený od zipsov, háčikov, suchých zipsov a textílií púšťajúcich vlákna?",
            "Povoľuje výrobca prímú paru, kefovanie, sušičku alebo žehlenie?",
        ],
        "mistakes": [
            "Použiť rovnaký postup na pletenú velúrovú mikinu, tkaný zamat a lepený pošťah.",
            "Drhnúť svetlý otlak ako škvrnu bez kontroly smeru a hustoty vlasu.",
            "Prať lícnym povrchom von s uterákmi, odkrytými zipsami a preplneným bubnom.",
            "Použiť odžmolkovač na vlas bez testu a zrezať spolu so zhlukom aj zdravý povrch.",
            "Pritlačiť horúcu žehličku priamo na vlas alebo syntetický embosovaný vzor.",
            "Zložiť vlhký odev pod ťažkú kopu a vytvoriť súčasne zatuchnutie aj ostré otlačenie.",
        ],
        "expert_heading": "Odbornejší pohľad: retencia vlasu, matovanie a hranice testov",
        "expert": [
            "ASTM D4685/D4685M-25 hodnotí vybrané aspekty odolnosti vlasových tkanín proti opotrebovaniu a stratu vlasových chumáčov pri definovanom odieraní. Rozsah je dôležitý: metóda sa týka najmä tkaných vlasových textílií a nie je univerzálnym skóre pre každý pletený velúr, flock alebo hotový nábytok. Výsledok navyše ovplyvňuje predchádzajúce pranie, čistenie a povrchová úprava.",
            "ISO 12945-4:2020 opisuje vizuálne hodnotenie žmolkovania, rozstrapatenia a matovania textílií. Ide o hodnotiaci postup naviazaný na pripravené vzorky, nie predpoveď presného počtu praní spotrebiteľského odevu. Pri vlasovom povrchu treba navyše oddeliť zmenu smeru od straty vlákien. Fotografia pri jednom osvetlení môže rozdiel zveličiť alebo skryť, preto sa vzhľad posudzuje kontrolovane.",
            "Woolmark uvádza tkaný velvet a pletený velour ako odlišné konštrukčné cesty vo svojej inovácii z vlny. Tento príklad podporuje rozlíšenie základu, no nie tvrdenie, že každý produkt na trhu dodržiava rovnaké pomenovanie. Označenie vlákien podľa EÚ a symboly GINETEX preto zostávajú rozhodujúce pre konkrétny kus; názov povrchu je iba jedna časť popisu.",
        ],
        "source_intro": "Zdroje podporujú rozlíšenie vybraných tkaných a pletených vlasových konštrukcií a presne vymedzené hodnotenie opotrebovania. Nepodporujú jeden univerzálny postup pre všetok tovar označený ako velúr.",
        "sources": [
            ("Woolmark: príklad tkaného velvetu a pleteného velúru", WOOLMARK_VELVET),
            ("ASTM D4685/D4685M-25: oder vlasových tkanín", ASTM_PILE),
            ("ISO 12945-4:2020: vizuálne hodnotenie žmolkovania a matovania", ISO_PILLING),
            ("AATCC TM135-2025: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
            ("EÚ 1007/2011: označovanie vláknového zloženia", EU_FIBRE_LABEL),
            ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ],
        "product_intro": "Bežný prací gél prichádza do úvahy iba pri velúrovom odeve, ktorý etiketa povoľuje prať vo vode a ktorého vlas, základ, farba aj elastické diely sú s produktom kompatibilné.",
        "product_text": "Hypoalergénny prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Pri prateľnej velúrovej mikine alebo teplákoch ho použite v presnej dávke, v nepreplnenom bubne a iba podľa etikety.",
        "product_limit": "Produkt nie je určený ako univerzálny čistič na pošťah, tkaný zamat, vlnu, hodváb, flock alebo profesionálne čistený odev. Neobnoví vytrhnutý vlas, tepelné zlepenie ani poškodené spojivo.",
        "category_intro": "Pri velúre je dôležitejšia kompatibilita s vlasom a základom než intenzita vône. Vyberajte podľa zloženia, farby a pokynov a nepoužívajte nadbytok, ktorý sa zachytí v hustom povrchu.",
        "category_text": "V kategórii nájdete pracie gély pre bežnú domácu bielizeň. Na velúr vyberte iba variant povolený etiketou konkrétneho odevu; pri pošťahu alebo citlivej vlasovej textílii postupujte podľa výrobcu.",
        "related": [
            ("Ako predchádzať zatrhávaniu textilu", ARTICLE_SNAGGING),
            ("Prečo oblečenie žmolkuje", ARTICLE_PILLING),
            ("Ako predchádzať zatrhávaniu", ARTICLE_SNAGGING),
            ("Čo je polyester", ARTICLE_POLYESTER),
            ("Čo je viskóza", ARTICLE_VISCOSE),
            ("Ako žehliť oblečenie", ARTICLE_IRONING),
        ],
        "faq_title": "velúr a vlasový povrch",
        "faq": [
            ("Je velúr to isté ako zamat?", "Nie úplne. Klasický zamat je typicky tkaná vlasová látka, kým odevný velúr býva často pletený. Obchodné názvy sa však prekrývajú, preto overte konštrukciu a etiketu."),
            ("Môžem velúr prať v práčke?", "Iba ak to povoľuje etiketa hotového výrobku. Pletená mikina, tkaný odev, flock a pošťah môžu mať úplne odlišné obmedzenia."),
            ("Prečo má velúr svetlé fľaky?", "Môže ísť iba o otočený vlas, ale aj o zlepenie, oder alebo stratu farby. Suchý povrch pozorujte z viacerých uhlov a porovnajte omak a hustotu."),
            ("Dá sa velúr žehliť?", "Priamy silný tlak na vlas sa neodporúča. Ak etiketa povoľuje teplo alebo paru, pracujte z rubu, s odstupom alebo na vhodnej podložke a najprv testujte."),
            ("Ako odstrániť žmolky z velúru?", "Najprv odlište žmolok od zdravého vlasu a matovania. Odžmolkovač môže vlas zrezať, preto ho použite iba po povolení a bezpečnom teste."),
            ("Ako skladovať velúrové oblečenie?", "Úplne suché, bez bodového tlaku a ostrých záhybov na viditeľnej ploche. Ťažký pletený kus podoprite a neuzatvárajte zvyškovú vlhkosť do plastu."),
        ],
    },
    {
        "title": "Čo je seersucker: zvlnená tkanina, priedušnosť a pranie bez žehlenia",
        "link": "co-je-seersucker-zvlnena-tkanina-priedusnost-a-pranie-bez-zehlenia",
        "meta": "Čo je seersucker, ako vzniká jeho zvlnenie, čím sa líši pravá tkaná štruktúra od razeného reliéfu a ako prať košele, šaty či obliečky.",
        "short": "Seersucker je zvlnená tkanina, ktorej reliéf môže vznikať rozdielnym napätím pri tkaní alebo dodatočnou úpravou. Zistite, ako ho prať a kedy ho radšej nežehliť.",
        "name": "seersucker",
        "genitive": "seersuckeru",
        "construction_summary": "tkaninu so striedaním hladších a zvlnených pruhov alebo plôch, pri ktorej môže reliéf vzniknúť už tkaním alebo až mechanickým či chemickým dokončením",
        "label_details": "spôsob vytvorenia reliéfu, zloženie pruhov, elastické nite, farbenie, pokyn k žehleniu a tvarové výstuhy",
        "surface_object": "vystupujúce zvlnené pruhy a voľnejšie úseky priadze",
        "residue_place": "prehĺbeninách reliéfu, švoch, golieri, manžetách a viacvrstvových lemoch",
        "drying_advice": "Košeľu alebo šaty vyrovnajte vo švoch, nie v samotnom reliéfe; sušte voľne bez napnutia, ktoré by zvlnené pruhy roztiahlo do hladka.",
        "heat_risk": "výšku reliéfu, rozmer, farbu, živicu alebo tepelne fixovanú syntetickú časť, elastan a lepené výstuhy",
        "stain_risk": "vrcholy reliéfu, farebné pruhy a rozdielne napätie hladkej a zvlnenej plochy",
        "failure_sign": "reliéf mizne iba na niektorých miestach, pruhy sa sťahujú nerovnomerne, živica sa drobí alebo sa pri šve otvára tkanina",
        "answer": "Seersucker je ľahká tkanina s pravidelne zvlneným alebo zvrásneným povrchom, ktorý drží časť plochy ďalej od pokožky. Pri tradičnej konštrukcii vzniká efekt rozdielnym napätím dvoch skupín osnovných priadzí počas tkania; podobný vzhľad sa však dá vytvoriť aj razením alebo úpravou. Názov preto nie je zárukou bavlny ani trvalosti reliéfu. Košeľu, šaty alebo obliečky perte podľa etikety, s podobne ľahkými kusmi, v nepreplnenom bubne a s presnou dávkou prostriedku. Po praní vyrovnajte švy, nie zvlnenie. Žehlenie nie je automaticky zakázané, ale silný tlak na líci môže reliéf dočasne alebo trvalo sploštiť; symbol žehličky má prednosť.",
        "intro": "Seersucker sa často predáva ako letná tkanina, ktorú netreba žehliť. Obe tvrdenia potrebujú hranice. Zvlnený povrch zmenšuje súvislý kontakt s pokožkou a môže podporiť prúdenie vzduchu, no komfort ovplyvňuje zloženie, hustota, strih, vlhkosť a počasie. A reliéf môže byť vytkaný, tepelne vyrazený alebo chemicky stabilizovaný, takže jeho reakcia na vodu a teplo nie je jednotná. Praktický návod preto musí odlišiť konštrukciu od vzhľadu a vysvetliť, kedy prirodzené zvlnenie chrániť a kedy je lokálny záhyb skutočnou chybou.",
        "quick": [
            "<strong>Zvlnenie môže byť vytkané alebo dokončené:</strong> dva podobné povrchy nemusia mať rovnakú trvácnosť.",
            "<strong>Seersucker nie je vlákno:</strong> býva bavlnený, ale existujú syntetické, viskózové aj zmesové varianty.",
            "<strong>Letný komfort nemá jednu príčinu:</strong> reliéf znižuje kontakt, kým priadza, hustota a strih riadia vlhkosť a prúdenie.",
            "<strong>Nežehliť nie je univerzálny symbol:</strong> postup určuje etiketa, no silné plošné žehlenie môže efekt znehodnotiť.",
            "<strong>Po praní vyrovnajte švy:</strong> zvlnenú plochu nenaťahujte do hladka a nehodnoťte rozmer, kým je kus mokrý.",
        ],
        "overview_heading": "Ako vzniká zvlnenie seersuckeru",
        "overview": [
            "Pri klasickom tkanom seersuckeri pracujú v osnove najmenej dve skupiny priadzí s rozdielnym napätím. Pevnejšie napnutá skupina vytvára hladší pruh, kým voľnejšia skupina po uvoľnení vystúpi a zvlní sa. Efekt je zabudovaný do geometrie tkaniny a nemusí sa spoliehať iba na povrchovú živicu. Vyžaduje však presné riadenie priadzí, napätia a dokončenia, aby sa pruhy po praní nesťahovali nerovnomerne.",
            "Podobný reliéf možno vytvoriť mechanickým razením. CottonWorks upozorňuje na razený „falošný seersucker“: termoplastické vlákna možno tepelne fixovať, zatiaľ čo celulózové materiály potrebujú pre trvácnejší razený efekt živicovú úpravu. To neznamená, že razený variant je automaticky nekvalitný. Znamená to, že vzhľad sám neprezradí mechanizmus ani reakciu na žehlenie.",
            "Niektoré látky získajú zvlnenie rozdielnym zmrštením priadzí alebo chemickým ošetrením vybraných pruhov. Obchodný popis by mal vysvetliť zloženie a starostlivosť, no v praxi to nebýva podrobné. Spotrebiteľ preto vychádza zo štítku, pokynu výrobcu a správania skúšobného miesta. Domácim rozťahovaním reliéfu nemožno spoľahlivo určiť, či je vytkaný alebo dodatočne fixovaný.",
        ],
        "table1_heading": "Spôsoby vytvorenia seersuckerového reliéfu",
        "table1_intro": "Tabuľka ukazuje konštrukčné rozdiely a praktické otázky. Bez technického listu nemožno mechanizmus potvrdiť iba pohľadom.",
        "table1_headers": ["Typ efektu", "Ako približne vzniká", "Typická vlastnosť", "Riziko pri starostlivosti"],
        "table1_rows": [
            ("Tkaný slack-tension seersucker", "Skupiny osnovných priadzí sa tkajú s rozdielnym napätím.", "Reliéf je súčasťou geometrie tkaniny.", "Nerovnomerné zrazenie, deformácia pruhov a sploštenie tlakom."),
            ("Tepelne razený syntetický povrch", "Reliéf sa vytlačí valcom a termoplastické vlákno sa tepelne fixuje.", "Pravidelný opakovaný vzor aj bez dvoch osnovných napätí.", "Nevhodné teplo môže povrch znovu zmeniť alebo lesknúť."),
            ("Živicovo stabilizovaný celulózový reliéf", "Razený alebo zvrásnený efekt sa stabilizuje chemickou úpravou.", "Vzhľad podobný tradičnému zvlneniu.", "Opotrebovanie úpravy, citlivosť na teplotu a zmena omaku."),
            ("Rozdielne zmrštené pruhy", "Vybrané priadze alebo plochy sa pri dokončení zmenia rozdielne.", "Reliéf sa zvýrazní po mokrom spracovaní.", "Ďalšie pranie môže meniť rozmery, ak stabilizácia nie je dokončená."),
        ],
        "sections": [
            {
                "heading": "Prečo sa seersucker nosí v lete",
                "paragraphs": [
                    "Vystupujúce pruhy sa dotýkajú pokožky na menšej súvislej ploche než hladká priliehavá tkanina. Medzi prehĺbeniami ostáva priestor pre vzduch a textília sa menej plošne prilepí pri potení. Tento geometrický efekt vysvetľuje letnú povesť seersuckeru, nie však automatické ochladenie za každých podmienok. Hrubý, hustý alebo nepriedušne dokončený variant môže byť teplejší než hladká ľahká tkanina.",
                    "Bavlnená priadza prijíma vlhkosť, syntetická zmes môže schnúť rýchlejšie a viskóza môže zmeniť splývavosť. Strih, spodná vrstva a vietor ovplyvňujú prúdenie. Preto pri výbere letnej košele hodnotíte reliéf, zloženie, hmotnosť, hustotu a voľnosť strihu spolu. Samotný názov seersucker nie je normované meranie priedušnosti ani tepelného komfortu.",
                ],
            },
            {
                "heading": "Vytkaný seersucker verzus razená napodobenina",
                "paragraphs": [
                    "Pri vytkanom variante reliéf nadväzuje na skupiny osnovných nití a smeruje pozdĺž tkaniny. Na rube možno pozorovať zodpovedajúce napätie a pruhy, hoci dokončenie vzhľad zjemní. Razený vzor môže byť pravidelnejší, opakovať sa aj iným smerom alebo byť viazaný na povrchovú úpravu. Tieto znaky sú orientačné; moderná výroba dokáže vytvoriť veľmi presvedčivý reliéf.",
                    "Rozhodujúcou praktickou informáciou nie je nálepka „pravý“ alebo „falošný“, ale trvácnosť pri určenej starostlivosti. Vytkaný efekt sa môže pri silnom tlaku dočasne sploštiť a po navlhčení vrátiť; nekvalitne stabilizovaná tkanina sa však môže sťahovať nerovnomerne. Kvalitný razený syntetický povrch zas môže tvar držať dobre, no vyžaduje dodržanie tepelnej hranice.",
                ],
                "callout": {
                    "title": "Čo sa opýtať pred kúpou",
                    "items": [
                        "Je reliéf vytvorený rozdielnym napätím pri tkaní alebo dodatočným dokončením?",
                        "Aké je presné vláknové zloženie a obsahuje tkanina elastan alebo živicovú úpravu?",
                        "Aká rozmerová zmena sa očakáva pri postupe uvedenom na etikete?",
                        "Povoľuje výrobca sušičku, paru a lokálne žehlenie goliera, manžiet alebo švov?",
                    ],
                    "background": "#f7fbf8",
                    "border": "#dbe5de",
                },
            },
            {
                "heading": "Musí sa seersucker žehliť?",
                "paragraphs": [
                    "Prirodzený zvlnený povrch je súčasť vzhľadu, takže plošné vyžehlenie do hladka zvyčajne nedáva zmysel. Neznamená to, že celý odev nesmie prísť do kontaktu s teplom. Golier, manžeta, léga alebo šev môže podľa etikety potrebovať lokálne vyrovnanie. Pracujte z rubu, s menším tlakom a tak, aby ste neprešli horúcou plochou cez reliéf.",
                    f"Syntetický razený variant, bavlnená tkanina so živicou a vytkaný bavlnený seersucker reagujú na teplo rozdielne. Symbol žehličky preto nemožno nahradiť internetovou radou „nežehliť“. Ak etiketa paru povoľuje, záhyb skúste uvoľniť s odstupom a bez stlačenia. Všeobecné teplotné hranice vysvetľuje návod <a href=\"{ARTICLE_IRONING}\">ako žehliť oblečenie podľa materiálu</a>.",
                ],
            },
            {
                "heading": "Ako prať seersuckerovú košeľu",
                "paragraphs": [
                    "Golier a manžety skontrolujte na maz a kozmetiku, škvrny ošetrite lokálne bez drhnutia vrcholov reliéfu. Zapnite voľné prvky, košeľu obráťte naruby a perte s podobne ľahkými odevmi. Ťažké nohavice a uteráky môžu zvlnenie počas mokrého cyklu stlačiť a zároveň zvýšiť mechanické opotrebovanie. Program, teplotu a otáčky určí etiketa a najcitlivejšia súčasť košele.",
                    "Po vybratí uchopte košeľu za pevné body, jemne zarovnajte ramená, bočné švy, légu a golier. Neťahajte zvlnené pruhy do hladka. Zaveste ju na široký vešiak alebo sušte podľa pokynu výrobcu s dobrým prúdením vzduchu. Po vyschnutí sa rozhodnite, či treba upraviť iba konštrukčné detaily; celá plocha môže zostať prirodzene zvlnená.",
                ],
            },
            {
                "heading": "Ako prať seersuckerové šaty a sukňu",
                "paragraphs": [
                    "Odev môže kombinovať seersuckerovú vrchnú látku s hladkou podšívkou, zipsom, elastickým pásom a tvarovacou výstuhou. Každá vrstva sa za mokra mení inak. Ak podšívka nie je pre domáce pranie určená alebo sa pri skúške uvoľňuje farba, zvoľte odborné čistenie. Pri povolenom praní použite ochranné vrecko primeranej veľkosti, nie tesný uzol.",
                    "Mokré šaty nenoste za ramienko či jedinú šnúrku. Podoprite ich, urovnajte zvislé švy a nechajte reliéf prirodzene sa usadiť. Pri sušení sledujte, či sa podšívka nesťahuje inak než vrchná vrstva. Ak vzniká bublina alebo sa lem pretáča, nenaťahujte mokrý kus nasilu; po úplnom vyschnutí zdokumentujte rozdiel a zvážte odbornú úpravu.",
                ],
            },
            {
                "heading": "Seersuckerové obliečky a posteľná bielizeň",
                "paragraphs": [
                    "Zvlnená obliečka sa dotýka pokožky v premenlivej ploche a po vysušení nepotrebuje dokonale hladký vzhľad. Pri praní zapnite zips alebo gombíky, obliečku obráťte podľa odporúčania a neplňte bubon tak, aby sa veľké kusy zbalili do nepriepustnej gule. Reliéf zadrží drobné častice v prehĺbeninách, preto potrebuje priestor na pohyb a dostatok vody na oplach.",
                    "Po praní obliečku rozprestrite, zarovnajte rohy a švy a sušte s prúdením vzduchu. Neťahajte celú plochu do hladka, pretože tým meranie rozmeru skreslíte a reliéf dočasne sploštíte. Ak je povolená sušička, vyberte bielizeň po dosušení bez dlhého prevaľovania. Presušenie zvyšuje pokrčenie hladkých lemov a môže opotrebovať vrcholy zvlnenia.",
                ],
            },
            {
                "heading": "Zrazenie seersuckeru a meranie rozmeru",
                "paragraphs": [
                    "Pri reliéfnej tkanine treba odlišiť skutočné skrátenie priadzí od dočasného zvýšenia zvlnenia. Mokrá plocha sa môže javiť užšia, pretože pruhy vystúpili vyššie. Merajte až úplne suchý kus v prirodzene urovnanom stave bez napínania. Použite rovnaké body a zaznamenajte dĺžku aj šírku, pretože rozdielne osnovné skupiny môžu meniť geometriu smerovo.",
                    f"AATCC TM135 poskytuje kontrolovaný rámec pre domáce pranie a meranie rozmerov, ale konkrétny výrobok treba testovať deklarovaným postupom. Pri metrovom textile ho pred strihaním predperte, ak to dodávateľ odporúča. Podrobné príčiny zmeny nájdete v článku <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie zráža</a>. Výrazne nerovnomerné stiahnutie pruhov po dodržaní etikety môže byť kvalitatívny problém.",
                ],
            },
            {
                "heading": "Škvrny a pot v prehĺbeninách reliéfu",
                "paragraphs": [
                    "Golier, podpazušie a vrcholy zvlnenia zachytávajú maz, opaľovací krém a prach rozdielne. Škvrnu najprv odsajte, potom naneste kompatibilný prostriedok v malom množstve aj do prehĺbeniny bez roztiahnutia pruhu. Tvrdá kefa odiera vrcholy, splošťuje reliéf a môže vytvoriť svetlú mapu. Pri farebných pruhoch vždy otestujte prenos farby na bielu handričku.",
                    f"Po lokálnom ošetrení nenechajte koncentrovaný produkt zaschnúť, ak to návod výslovne nepovoľuje. Kus vyperte a dôkladne opláchnite. Ak fľak ostáva, nevkladajte ho do vysokej teploty a neopakujte naslepo stále silnejšiu chémiu. Systematický postup podľa typu nečistoty ponúka návod <a href=\"{ARTICLE_STAIN}\">ako odstrániť škvrny z oblečenia</a>.",
                ],
            },
            {
                "heading": "Keď reliéf po praní zoslabne alebo zmizne",
                "paragraphs": [
                    "Najprv nechajte kus úplne vyschnúť bez tlaku. Vytkané zvlnenie sa môže po navlhčení znovu zvýrazniť, zatiaľ čo mokrá hmotnosť ho dočasne stiahne. Ak povrch ostane hladký, skontrolujte, či nebol vyžehlený, presušený alebo napnutý. Pri razenom efekte mohla teplota a mechanika zmeniť termoplastickú fixáciu alebo postupne opotrebovať živicovú úpravu.",
                    "Domáce chemické pokusy o obnovenie reliéfu nie sú bezpečné. Neznáma živica alebo zmes môže zmeniť farbu, pevnosť a omak. Ak výrobca uvádza aktiváciu parou alebo praním, dodržte presný postup. Pri novom kuse, ktorý po povolenej starostlivosti stratí efekt nerovnomerne, odfoťte stav, etiketu a cyklus a riešte ho s predajcom skôr, než pridáte ďalšie teplo.",
                ],
            },
            {
                "heading": "Ako vybrať seersucker bez marketingových skratiek",
                "paragraphs": [
                    "Prezrite rovnomernosť pruhov, líc aj rub, rovný okraj a napojenie reliéfu pri švoch. Zvlnenie nemá vytvárať nekontrolované bubliny pri jednom okraji ani rednúce miesta na vrcholoch. Pýtajte sa na zloženie, hmotnosť, mechanizmus efektu a očakávanú rozmerovú zmenu. Nápis „bez žehlenia“ je praktická vlastnosť vzhľadu, nie dôkaz odolnosti alebo zdravotnej vhodnosti.",
                    "Na košeľu posúďte mäkkosť, priesvitnosť, tvar goliera a správanie pri ohybe. Na obliečky sledujte kvalitu švov, zapínanie, priestor na pohyb pri praní a omak reliéfu. Na detský textil kontrolujte drobné uvoľnené nite a bezpečnosť doplnkov. Najvhodnejší seersucker nie je najviac zvlnený, ale ten, ktorého konštrukcia a starostlivosť zodpovedajú zamýšľanému použitiu.",
                ],
            },
        ],
        "table2_heading": "Diagnostika seersuckeru po praní",
        "table2_intro": "Reliéf prirodzene mení rozmery a odraz svetla. Problém preto hodnotíte až po úplnom vyschnutí, bez naťahovania a s porovnaním švov aj pruhov.",
        "table2_headers": ["Prejav", "Možné vysvetlenie", "Čo skontrolovať", "Odporúčaný krok"],
        "table2_rows": [
            ("Reliéf je výraznejší", "Uvoľnenie napätia a prirodzené zvlnenie po mokrom cykle.", "Suchý rozmer, rovnomernosť pruhov a etiketu.", "Ak je zmena rovnomerná a rozmer v limite, ponechať prirodzený vzhľad."),
            ("Reliéf takmer zmizol", "Tlak, žehlenie, napnutie, teplo alebo opotrebovanie dokončenia.", "Mechanizmus efektu, použité sušenie a teplotu.", "Nechať vyschnúť bez tlaku; ďalší postup iba podľa výrobcu."),
            ("Pruhy sa sťahujú nerovnomerne", "Rozdielna zmena osnovných skupín, šev, strih alebo nestabilná úprava.", "Oba smery, napojenie pri švoch a záznam cyklu.", "Nový kus zdokumentovať; nenapínať mokrý povrch do hladka."),
            ("Vrchol reliéfu je svetlý a lesklý", "Povrchový oder alebo tlak žehličky.", "Či ostáva zmena po vyčistení a z viacerých uhlov.", "Obmedziť trenie a teplo; mechanickú stratu farby pranie nevráti."),
            ("Prehĺbeniny sú tuhé alebo zapáchajú", "Zvyšky prostriedku, slabý oplach alebo nedosušenie.", "Dávku, naplnenie, vodu a vnútornú vlhkosť.", "Pri povolení opláchnuť a úplne vysušiť; nepridávať ďalšiu vrstvu vône."),
        ],
        "steps_heading": "Bezpečný postup prania seersuckeru",
        "steps": [
            "Prečítajte vláknové zloženie, symboly a informáciu o reliéfe; skontrolujte golier, manžety, podšívku, elastan a výstuhy.",
            "Lokálne ošetrite škvrny po teste farby, zapnite zachytávajúce prvky a odev obráťte naruby, ak to konštrukcia povoľuje.",
            "Oddeľte seersucker od ťažkých a drsných kusov, nechajte bubon voľný a reliéf nestláčajte do tesného pracieho vrecka.",
            "Zvoľte povolenú teplotu, program a otáčky a použite presnú dávku kompatibilného pracieho prostriedku.",
            "Po vypratí vyrovnajte švy, rohy a konštrukčné diely, no zvlnené pruhy nenaťahujte do hladka.",
            "Sušte bez bodového tlaku a po úplnom vyschnutí upravte iba golier alebo lem v súlade so symbolom žehlenia.",
        ],
        "remember": [
            "Je reliéf vytkaný rozdielnym napätím alebo vytvorený razením či chemickou úpravou?",
            "Aké vlákna obsahuje tkanina a má syntetickú, elastickú alebo živicovú zložku?",
            "Povoľuje etiketa sušičku, paru alebo lokálne žehlenie konštrukčných detailov?",
            "Je odev oddelený od ťažkých uterákov, zipsov a kusov, ktoré by reliéf stlačili alebo odierali?",
            "Hodnotíte rozmer a vzhľad až po úplnom vyschnutí bez násilného rozťahovania?",
        ],
        "mistakes": [
            "Predpokladať, že každý seersucker je bavlnený a vytkaný tradičnou metódou.",
            "Považovať letný názov za laboratórny dôkaz priedušnosti a rovnakého komfortu.",
            "Vyžehliť celý reliéf silným tlakom z líca len preto, aby vyzeral ako hladká košeľa.",
            "Prať ľahkú košeľu s uterákmi a ťažkými nohavicami v preplnenom bubne.",
            "Merať mokrý zvlnený kus alebo ho napínať do pôvodnej veľkosti za rohy.",
            "Prekrývať zatuchnutie v prehĺbeninách ďalšou vôňou namiesto kontroly oplachu a schnutia.",
        ],
        "expert_heading": "Odbornejší pohľad: napätie osnovy, razenie a rozmerová stabilita",
        "expert": [
            "Technický bulletin Cotton Incorporated opisuje spracovanie bavlneného tkaného seersuckeru prostredníctvom konštrukcie a riadenia napätia. Je to výrobný zdroj pre konkrétnu cestu a nepokrýva každý obchodný produkt. Podporuje však základné vysvetlenie, že reliéf môže byť vytvorený geometriou dvoch osnovných skupín a že spracovanie musí kontrolovať napätie aj rozmerové zmeny.",
            "CottonWorks pri mechanickom dokončovaní výslovne uvádza razený efekt napodobňujúci seersucker. Termoplastické vlákna možno tepelne fixovať, kým celulózová látka potrebuje na trvácnejší razený vzor živicovú úpravu. Z toho vyplýva praktická hranica: rovnaký vzhľad môže mať rozdielny mechanizmus, a preto nemožno povolenú teplotu ani trvácnosť určiť pohľadom.",
            "AATCC TM135-2025 meria rozmerovú zmenu po definovaných postupoch domáceho prania. Pri seersuckeri je dôležité použiť konzistentné meracie body a prirodzené urovnanie bez vyhladenia reliéfu silou. Norma nehodnotí subjektívny letný komfort ani nezaručuje, že každá domáca práčka replikuje test. Označenie vlákien a symboly starostlivosti zostávajú rozhodujúce pre konkrétny hotový výrobok.",
        ],
        "source_intro": "Zdroje podporujú konštrukčnú cestu cez rozdielne osnovné napätie, existenciu razených napodobenín a kontrolované meranie rozmerovej zmeny. Nepodporujú tvrdenie, že každý zvlnený povrch je rovnaký alebo že sa nikdy nesmie žehliť.",
        "sources": [
            ("Cotton Incorporated: spracovanie tkaného bavlneného seersuckeru", COTTONINC_SEERSUCKER),
            ("CottonWorks: mechanické dokončovanie a razený seersucker", COTTONWORKS_FINISHING),
            ("AATCC TM135-2025: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
            ("ISO 7211-2:2024: počet nití na jednotku dĺžky", ISO_WOVEN_COUNT),
            ("EÚ 1007/2011: označovanie vláknového zloženia", EU_FIBRE_LABEL),
            ("GINETEX: význam symbolov ošetrovania", GINETEX),
        ],
        "product_intro": "Pri prateľnej seersuckerovej koŮli, šatách alebo obliečke vyberajte prostriedok podľa zloženia, farby, reliéfnej úpravy a etikety. Nadmerná dávka sa môže zachytiť v prehĺbeninách.",
        "product_text": "Hypoalergénny prací gél z marseillského mydla je určený na bežnú domácu bielizeň. Na kompatibilný seersucker ho použite v presnej dávke, pri povolenom programe a s dostatočným priestorom na oplach.",
        "product_limit": "Produkt nie je automaticky vhodný pre vlnu, hodváb, citlivú viskózu, profesionálne čistený odev ani neznámu živicovú úpravu. Neobnoví reliéf trvalo sploštený teplom alebo poškodenú priadzu.",
        "category_intro": "Pri pracích géloch porovnávajte kompatibilitu so zložením a farbou, nie schopnosť „zjemniť“ reliéf. Seersucker má zostať zvlnený a po praní dobre vypláchnutý.",
        "category_text": "V kategórii nájdete pracie gély pre bežnú domácu bielizeň. Na konkrétny seersucker vyberte iba variant, ktorý povoľuje etiketa a ktorý zodpovedá vláknovej zmesi aj povrchovej úprave.",
        "related": [
            ("Čo je bavlna", ARTICLE_COTTON),
            ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
            ("Čo znamená gramáž látky", ARTICLE_GSM),
            ("Počet nití pri obliečkach", ARTICLE_COUNT),
            ("Ako žehliť oblečenie", ARTICLE_IRONING),
            ("Ako sušiť oblečenie na vzduchu", ARTICLE_DRYING),
        ],
        "faq_title": "seersucker, pranie a žehlenie",
        "faq": [
            ("Čo je seersucker?", "Je to tkanina s pravidelne zvlneným povrchom. Efekt môže vzniknúť rozdielnym napätím osnovných priadzí pri tkaní alebo dodatočným mechanickým či chemickým dokončením."),
            ("Je seersucker vždy bavlnený?", "Nie. Bavlnený je tradičný a častý, ale existujú polyesterové, viskózové a zmesové varianty. Presné zloženie uvádza etiketa."),
            ("Treba seersucker žehliť?", "Väčšinou sa ponecháva prirodzene zvlnený. Golier alebo šev možno upraviť iba vtedy, ak to povoľuje symbol žehlenia; silný tlak na líci môže reliéf sploštiť."),
            ("Na koľko stupňov prať seersucker?", "Jedna teplota neplatí pre všetky varianty. Riaďte sa symbolom vaničky a zohľadnite vlákno, farbu, elastan, živicovú úpravu a podšívku."),
            ("Prečo sa seersucker po praní viac zvlnil?", "Voda mohla uvoľniť rozdielne napätie priadzí. Hodnoťte až suchý kus bez naťahovania; rovnomerné zvýraznenie reliéfu nemusí byť chyba."),
            ("Dá sa seersucker sušiť v sušičke?", "Iba ak to povoľuje etiketa. Teplo a prevaľovanie môžu ovplyvniť rozmer, razený efekt, živicovú úpravu alebo elastickú zložku."),
        ],
    },
]


def visible_text(markup: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(TAG_RE.sub(" ", markup))).strip()


def article_hrefs(markup: str) -> list[str]:
    return sorted(set(re.findall(r'href="([^"]+)"', markup)))


def fetch_status(url: str) -> dict[str, object]:
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Codex VEVO batch 45 link preflight"},
            timeout=45,
            allow_redirects=True,
        )
        host = urlparse(url).netloc.lower()
        allowed_automation_block = (
            (host == "www.iso.org" and response.status_code == 403)
            or (host == "eur-lex.europa.eu" and response.status_code == 202)
        )
        automation_note = None
        if host == "www.iso.org" and response.status_code == 403:
            automation_note = "Official ISO page blocks the automated link checker"
        elif host == "eur-lex.europa.eu" and response.status_code == 202:
            automation_note = "Official EUR-Lex page accepts the automated request asynchronously"
        return {
            "url": url,
            "status": response.status_code,
            "final_url": response.url,
            "allowed_automation_block": allowed_automation_block,
            "automation_note": automation_note,
            "ok": response.status_code == 200 or allowed_automation_block,
        }
    except requests.RequestException as exc:
        return {
            "url": url,
            "status": None,
            "final_url": None,
            "allowed_automation_block": False,
            "ok": False,
            "error": str(exc),
        }


def preflight_links(articles: list[dict[str, object]]) -> dict[str, object]:
    target_urls = {f"{BASE}/n/{article['link']}" for article in articles}
    outgoing_urls = {
        urljoin(BASE, href) if href.startswith("/") else href
        for article in articles
        for href in article_hrefs(str(article["long"]))
    }
    all_urls = sorted(target_urls | outgoing_urls)
    with ThreadPoolExecutor(max_workers=6) as executor:
        checks = list(executor.map(fetch_status, all_urls))
    for check in checks:
        if check["url"] in target_urls:
            check["expected_status"] = 404
            check["ok"] = check["status"] == 404
    report = {
        "batch": "batch-45",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "target_count": len(target_urls),
        "outgoing_count": len(outgoing_urls),
        "check_count": len(checks),
        "failure_count": sum(not check["ok"] for check in checks),
        "checks": checks,
    }
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    candidate_titles = [
        line.strip()
        for line in CANDIDATES.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]
    article_by_title = {str(article["title"]): article for article in ARTICLES}
    if len(article_by_title) != len(ARTICLES) or set(candidate_titles) != set(article_by_title):
        raise SystemExit("Candidate titles and article definitions do not match exactly")
    slugs = [str(article["link"]) for article in ARTICLES]
    if len(slugs) != len(set(slugs)):
        raise SystemExit("Batch contains duplicate slugs")

    rendered: list[dict[str, object]] = []
    metrics: list[dict[str, object]] = []
    for article in ARTICLES:
        body = render_article(article)
        public_text = f"{article['title']} {article['short']} {body}"
        visible = visible_text(body)
        if FORBIDDEN_PUBLIC_RE.search(public_text):
            raise SystemExit(f"Forbidden public wording in {article['title']}")
        if FIXED_PRICE_RE.search(visible_text(public_text)):
            raise SystemExit(f"Fixed price found in {article['title']}")
        metric = {
            "title": article["title"],
            "slug": article["link"],
            "words": len(WORD_RE.findall(visible)),
            "h2": len(re.findall(r"<h2\b", body, re.IGNORECASE)),
            "tables": len(re.findall(r"<table\b", body, re.IGNORECASE)),
            "responsive_tables": len(re.findall(r'<div\b[^>]*style="[^"]*overflow-x:\s*auto', body, re.IGNORECASE)),
            "styled_blocks": len(re.findall(r"<div\b[^>]*style=", body, re.IGNORECASE)),
            "action_buttons": len(re.findall(r'<a\b[^>]*style="[^"]*display:\s*inline-block', body, re.IGNORECASE)),
        }
        if metric["words"] < 2500:
            raise SystemExit(f"Article is too short: {article['title']} ({metric['words']} words)")
        if metric["h2"] < 24 or metric["tables"] < 2 or metric["responsive_tables"] != metric["tables"]:
            raise SystemExit(f"Article structure is incomplete: {article['title']} ({metric})")
        if metric["styled_blocks"] < 10 or metric["action_buttons"] < 2:
            raise SystemExit(f"Article visual blocks are incomplete: {article['title']} ({metric})")
        metrics.append(metric)
        rendered.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": body,
                "link": article["link"],
                "date_posted": PUBLISH_DATE,
                "time_posted": "13:00:00",
                "commenting": False,
                "title_tag": article["title"],
                "description": article["meta"],
            }
        )

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(rendered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = preflight_links(rendered)
    if report["failure_count"]:
        failed = [check for check in report["checks"] if not check["ok"]]
        print(json.dumps({"failed_links": failed}, ensure_ascii=False, indent=2))
        raise SystemExit("Batch 45 link preflight failed")
    print(json.dumps({"article_count": len(rendered), "metrics": metrics, "link_preflight": True}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
