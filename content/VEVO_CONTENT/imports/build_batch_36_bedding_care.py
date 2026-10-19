import html
import json
import re
import unicodedata
from pathlib import Path
from urllib.parse import urljoin

import requests


BASE = "https://www.vevo.sk"
TODAY = "2026-07-09"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-36-2026-07-09-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-36-2026-07-09-link-preflight.json")

FORBIDDEN_PUBLIC_RE = re.compile(
    r"longtail|long-tail|long tail|"
    r"\bkeyword(?:s)?\b|klucov\w*\s+slov\w*|kľúčov\w*\s+slov\w*|"
    r"\bSEO\b|search\s+intent|sub[- ]?quer(?:y|ies)|sub[- ]?query|fan[- ]?out|fanout|"
    r"cielene\s+pokr[yý]vame|cielene\s+odpoved[áa]|"
    r"\bCTA\b",
    re.IGNORECASE,
)


def slugify(value):
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower())
    return re.sub(r"-+", "-", value).strip("-")


def esc(value):
    return html.escape(str(value), quote=True)


def table(headers, rows):
    head = "".join(
        f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{esc(header)}</th>'
        for header in headers
    )
    body = "\n".join(
        "<tr>"
        + "".join(f'<td style="border: 1px solid #e5e5e5; padding: 10px;">{cell}</td>' for cell in row)
        + "</tr>"
        for row in rows
    )
    return (
        '<table style="width: 100%; border-collapse: collapse; margin: 20px 0;">\n'
        f"<thead><tr>{head}</tr></thead>\n<tbody>\n{body}\n</tbody>\n</table>"
    )


def callout(title, bullets, background="#fffaf5", border="#e6ded2"):
    items = "".join(f"<li>{item}</li>" for item in bullets)
    return f"""
<div style="border: 1px solid {border}; border-radius: 8px; padding: 18px; margin: 22px 0; background: {background};">
<h2 style="margin-top: 0;">{esc(title)}</h2>
<ul>{items}</ul>
</div>
""".strip()


def product_and_category_blocks(article):
    return """
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">Riešenie pre čisté domáce textílie</h2>
<p>Pri posteľných textíliách má produkt zmysel až vtedy, keď sedí program, veľkosť náplne a sušenie. Najprv odstráňte pot, prach a vlhkosť, až potom dolaďujte vôňu.</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
<p><strong>Kedy dáva zmysel:</strong> pri pravidelnom praní plachiet, chráničov, ľahších prehozov a posteľných textílií, kde chcete jemný prací základ a jasné dávkovanie bez zbytočne silnej parfumácie.</p>
<p><strong>Kedy najprv riešiť príčinu:</strong> ak textília zapácha po vlhkosti, bola dlho mokrá alebo je príliš objemná na bubon. Vtedy treba upraviť náplň, oplach a sušenie.</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
</div>
</div>
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">Vyberte si pranie podľa typu textilu</h2>
<ul>
<li><strong>Bežné posteľné textílie:</strong> začnite kategóriou <a href="/c/vevo-home-care/pranie/praci-gel">pracie gély</a> a dávkovaním podľa náplne.</li>
<li><strong>Citlivá pokožka:</strong> pridajte dôkladný oplach a držte sa štítku, najmä pri textíliách v priamom kontakte s pokožkou.</li>
<li><strong>Jemná vôňa po vysušení:</strong> keď je textil čistý a suchý, môžete opatrne použiť aj <a href="/c/vevo-fragrance/parfum-do-prania">parfumy do prania</a>. Nepoužívajte ich na prekrytie zatuchnutia.</li>
</ul>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Porovnať pracie gély</a></p>
</div>
""".strip()


def source_box():
    sources = [
        ("EPA: A Brief Guide to Mold, Moisture and Your Home", "https://www.epa.gov/mold/brief-guide-mold-moisture-and-your-home"),
        ("EPA: Care for Your Air", "https://www.epa.gov/indoor-air-quality-iaq/care-your-air-guide-indoor-air-quality"),
        ("CUH NHS: How to reduce the level of dust mites in your home", "https://www.cuh.nhs.uk/patient-information/dust-mites-in-your-home/"),
        ("American Lung Association: Dust Mites", "https://www.lung.org/clean-air/indoor-air/indoor-air-pollutants/dust-mites"),
    ]
    items = "".join(f'<li><a rel="noopener" href="{href}" target="_blank">{esc(label)}</a></li>' for label, href in sources)
    return f"""
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Zdroje a odborný kontext</h2>
<p>Pri posteľných textíliách sú dôležité tri veci: vlhkosť, prach a kontakt s pokožkou. Odborné zdroje odporúčajú znižovať vlhkosť, vetrať, prať posteľné textílie podľa vhodnej teploty a pri alergii riešiť aj bariérové obaly. V domácnosti to vždy prispôsobte štítku konkrétneho výrobku.</p>
<ul>{items}</ul>
</div>
""".strip()


def related_links(items):
    links = "".join(f'<li><a href="{href}">{esc(label)}</a></li>' for label, href in items)
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>{links}</ul>"


def faq(items, title):
    parts = [f"<h2>FAQ: {esc(title)}</h2>"]
    for question, answer in items:
        parts.append(f"<h3>{esc(question)}</h3><p>{answer}</p>")
    return "\n".join(parts)


COMMON_RELATED = [
    ("Ako často prať posteľné prádlo", "/n/ako-casto-prat-postelne-pradlo"),
    ("Ako správne prať obliečky", "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"),
    ("Ako osviežiť posteľ medzi praniami", "/n/ako-osviezit-postel-medzi-praniami-vetranie-pyzamo-matrac-a-jemna-vona"),
    ("Prečo je bielizeň po praní tvrdá alebo lepkavá", "/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach"),
    ("Prečo oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
]


ARTICLES = [
    {
        "title": "Ako prať chránič matraca: pot, prach, roztoče a správne sušenie",
        "short": "Chránič matraca perte podľa štítku, nepreplňte bubon a pred návratom na posteľ ho vysušte úplne do hĺbky. Najväčším rizikom je vlhkosť uzavretá medzi matracom a plachtou.",
        "answer": "Chránič matraca perte samostatne alebo s ľahkou podobnou bielizňou, podľa štítku najčastejšie na jemnom alebo bavlnenom programe. Pri pote a prachu pomáha predpierka alebo dlhší program, pri nepremokavej vrstve sa vyhnite vysokému teplu a agresívnemu žmýkaniu. Na matrac ho vráťte až úplne suchý.",
        "quick": [
            "<strong>Pot a pach:</strong> riešte čo najskôr, aby sa nezafixovali do povrchu.",
            "<strong>Nepremokavá vrstva:</strong> kontrolujte štítok, nie každý chránič znáša horúcu vodu alebo sušičku.",
            "<strong>Roztoče:</strong> ak štítok povoľuje vyššiu teplotu, má to pri alergii praktický význam.",
            "<strong>Sušenie:</strong> najprv presušiť úplne, až potom navliecť plachtu.",
            "<strong>Vôňa:</strong> jemné prevoňanie patrí až na čistý a suchý textil.",
        ],
        "intro": [
            "Chránič matraca je nenápadný kus textilu, ale v posteli robí veľa práce. Zachytáva pot, kožný maz, prach, drobné nehody a časť nečistôt, ktoré by inak išli priamo do matraca. Práve preto nestačí prať iba obliečky a plachtu. Ak chránič zostáva celé mesiace bez prania, posteľ môže pôsobiť zatuchnuto aj vtedy, keď navonok vyzerá čisto.",
            "Pri praní chrániča matraca je rozhodujúce, z čoho je vyrobený. Niektoré sú jednoduché bavlnené alebo froté, iné majú nepremokavú membránu, prešívanie alebo elastické boky. To mení teplotu, žmýkanie aj sušenie. Najhoršia chyba je správať sa k nim ako k obyčajnej plachte a potom ich dať späť na matrac ešte mierne vlhké.",
            "Dobrá rutina je jednoduchá: chránič zložiť, skontrolovať štítok, odstrániť lokálne škvrny, prať s primeranou dávkou gélu, dobre vypláchnuť a sušiť tak dlho, kým nie je suchý aj pri švoch a rohoch. Ak máte doma alergika, dieťa alebo človeka, ktorý sa v noci výrazne potí, oplatí sa prať ho pravidelnejšie.",
            "Článok rieši praktické situácie, ktoré sa pri chrániči matraca opakujú najčastejšie: ako vyprať chránič po potení, ako prať nepremokavý chránič matraca, ako často ho prať, ako ho sušiť a čo robiť, keď aj po praní cítiť vlhkosť alebo zatuchnutie.",
        ],
        "why_heading": "Prečo chránič matraca zapácha alebo zostáva po praní ťažký",
        "why": [
            "Chránič matraca zachytáva vlhkosť priamo v mieste, kde sa v noci najviac potíme. Ak sa perie zriedka, pot a kožný maz sa zmiešajú s prachom a prací program musí odstrániť viac než bežnú špinu. Krátky program alebo preplnený bubon potom nestačí.",
            "Druhým problémom je konštrukcia. Nepremokavé chrániče majú vrstvu, ktorá bráni prenikaniu tekutiny k matracu. Táto výhoda však znamená, že textil horšie presychá. Ak vlhkosť ostane pri membráne, po navlečení plachty sa rýchlo objaví zatuchnutý pach.",
            "Tretia častá príčina je priveľa pracieho prostriedku. Pri veľkej textílii človek ľahko pridá dávku navyše, ale zvyšky gélu sa potom horšie vyplachujú. Chránič môže byť tvrdší, lepkavý alebo nepríjemný na dotyk.",
            "Pri alergii treba myslieť aj na prach a roztoče. Posteľné textílie sú pre ne typickým prostredím, najmä keď je v spálni vyššia vlhkosť. Pranie pomáha, ale iba vtedy, keď rešpektuje štítok a textil sa po ňom dôkladne vysuší.",
        ],
        "decision_rows": [
            ("Bavlnený alebo froté chránič", "znáša bežnejšie pranie, ale môže držať veľa vody", "prať samostatne alebo s ľahkou bielizňou"),
            ("Nepremokavý chránič", "membrána sa môže poškodiť teplom alebo silným žmýkaním", "riadiť sa štítkom a sušiť opatrne"),
            ("Chránič po nočnom potení", "pot sa môže zafixovať do povrchu", "neodkladať pranie a použiť primeraný program"),
            ("Chránič v detskej posteli", "škvrny a vlhkosť sú častejšie", "mať náhradný kus a sušiť bez kompromisu"),
            ("Chránič pri alergii", "prach a roztoče sa držia v posteli", "ak štítok dovolí, prať dôkladnejšie a vetrať spálňu"),
        ],
        "steps": [
            "Zložte plachtu aj chránič a skontrolujte štítok výrobcu.",
            "Lokálne škvrny od potu alebo nehody navlhčite a jemne ošetrite pred praním.",
            "Chránič neperte v preplnenom bubne, potrebuje priestor na pohyb a oplach.",
            "Použite primeranú dávku pracieho gélu podľa veľkosti náplne a tvrdosti vody.",
            "Vyberte program podľa materiálu, nie podľa zvyku pri plachtách.",
            "Pri nepremokavej vrstve sa vyhnite vysokému teplu, ak ho štítok nepovoľuje.",
            "Po praní chránič rozložte, vyrovnajte rohy a nechajte ho preschnúť do hĺbky.",
            "Na matrac ho vráťte až vtedy, keď nie je vlhký pri švoch, gume ani v prešívaní.",
        ],
        "check_rows": [
            ("Pach potu", "krátke pranie a dlhé odkladanie", "prať skôr a nešetriť časom programu"),
            ("Tvrdý povrch", "priveľa pracieho prostriedku alebo slabý oplach", "znížiť dávku a pridať oplach"),
            ("Zatuchnutie po usušení", "vlhkosť ostala pri membráne", "sušiť dlhšie a vzdušnejšie"),
            ("Poškodená membrána", "príliš horúca voda alebo sušička", "čítať štítok pred každým experimentom"),
            ("Prach v posteli", "chránič sa perie menej než obliečky", "zaradiť ho do pravidelnej rutiny"),
        ],
        "mistakes": [
            "Prať chránič spolu s ťažkými uterákmi, ktoré mu nedovolia dobre sa vypláchnuť.",
            "Pridať dvojnásobnú dávku gélu, pretože textil pôsobí veľký a ťažký.",
            "Dať chránič späť na matrac, keď je ešte vlhký pri rohoch alebo švoch.",
            "Ignorovať nepremokavú vrstvu a použiť vysoké teplo bez kontroly štítku.",
            "Používať vôňu na prekrytie zatuchnutia namiesto riešenia vlhkosti.",
            "Prať chránič len vtedy, keď už posteľ nepríjemne zapácha.",
        ],
        "sections": [
            ("Ako vyprať chránič matraca po potení", [
                "Pri nočnom potení nečakajte na veľké pranie celej domácnosti. Pot obsahuje vodu, soli a kožný maz, ktoré sa postupne viažu na textil. Ak chránič necháte dlho napnutý na matraci, pach sa môže presunúť aj do plachty a pyžama.",
                "Najpraktickejšie je chránič vyprať samostatne alebo s ľahkou posteľnou bielizňou. Pri silnejšom zápachu pomôže dlhší program a dôkladný oplach. Ak používate parfumy do prania, dávkujte ich jemne až po vyriešení čistoty, nie ako náhradu za pranie.",
            ]),
            ("Ako prať nepremokavý chránič matraca", [
                "Nepremokavý chránič matraca má funkčnú vrstvu, ktorá chráni matrac pred tekutinou. Táto vrstva však nemusí znášať vysoké teploty, bielidlá, agresívne odstreďovanie alebo horúcu sušičku. Štítok je v tomto prípade dôležitejší než univerzálna rada.",
                "Po praní ho sušte rozložený tak, aby vzduch prúdil aj k spodnej strane. Ak ho prevesíte cez hrubú tyč alebo necháte zle poskladaný, v preložených miestach môže schnúť príliš dlho a práve tam vznikne pach vlhkosti.",
            ]),
            ("Ako často prať chránič matraca", [
                "V bežnej domácnosti má zmysel prať chránič pravidelne, nie až pri viditeľnej škvrne. Interval závisí od potenia, ročného obdobia, detí, domácich zvierat a zdravotnej situácie. Ak sa posteľ používa denne, chránič by nemal byť zabudnutý textil pod plachtou.",
                "Pri alergii, častom potení alebo detskej posteli je lepšie mať druhý chránič na výmenu. Vtedy nemusíte sušenie urýchľovať na úkor kvality a nehrozí, že vlhký kus vrátite späť na matrac len preto, že večer nemáte náhradu.",
            ]),
            ("Ako vysušiť chránič matraca bez zatuchnutia", [
                "Sušenie je pri chrániči rovnako dôležité ako pranie. Najprv ho po praní pretrepte, vyrovnajte rohy a skontrolujte prešívanie. Hrubšie miesta schnú pomalšie než hladká plocha, preto nestačí dotknúť sa stredu textilu.",
                "Ak používate sušičku, riaďte sa štítkom. Pri membráne môže príliš vysoká teplota znížiť životnosť chrániča. Bez sušičky pomáha prúdenie vzduchu, otočenie počas sušenia a trpezlivosť.",
            ]),
            ("Chránič matraca, roztoče a spálňová vlhkosť", [
                "Odborné zdroje pri roztočoch zdôrazňujú pranie posteľných textílií a znižovanie vlhkosti v spálni. Pri chrániči matraca to znamená nepestovať vlhké prostredie priamo nad matracom. Vetrajte, nenechávajte posteľ dlhodobo zakrytú po spotenej noci a sušte textílie do sucha.",
                "Ak je v domácnosti diagnostikovaná alergia, riešte rutinu komplexne: obliečky, plachty, chránič, vankúše, paplóny aj prach v okolí postele. Samotný prací gél nenahradí režim v spálni.",
            ]),
            ("Kedy chránič radšej nevyprať doma", [
                "Ak má chránič poškodenú membránu, zvláštnu výplň, veľký rozmer alebo štítok zakazuje domáce pranie, neriskujte ho v malej práčke. Príliš veľký kus sa v bubne nevyperie rovnomerne a môže zaťažiť spotrebič.",
                "Pri drahšom alebo zdravotnom chrániči sa oplatí overiť pokyny výrobcu. Domáce pranie má byť bezpečná údržba, nie skúška, či výrobok prežije maximálny program.",
            ]),
        ],
        "caution": [
            "Pri chráničoch s membránou nepoužívajte univerzálne horúce pranie bez štítku. To, čo je vhodné na bavlnenú plachtu, môže skrátiť životnosť funkčnej vrstvy.",
            "Ak textil po praní stále zapácha, nehľadajte prvé riešenie vo výraznejšej vôni. Skontrolujte, či nebol bubon preplnený, či sa gél vypláchol a či chránič preschol úplne.",
        ],
        "expert": [
            "EPA pri plesniach a vlhkosti zdôrazňuje, že základom prevencie je kontrola vlhkosti. V posteli to znamená nenechať medzi matracom, chráničom a plachtou uzavretý vlhký textil. Vlhkosť nie je len pocit, ale podmienka, ktorá podporuje zatuchnutie a môže zhoršiť kvalitu vzduchu v spálni.",
            "Zdroje zamerané na roztoče upozorňujú, že matrace a posteľné textílie patria medzi typické miesta ich výskytu. Pri alergii sa preto odporúča kombinovať pranie, bariérové obaly, znižovanie vlhkosti a pravidelné upratovanie. Domáca prax musí byť vždy prispôsobená štítku textilu.",
            "Pri chrániči matraca je praktický záver jasný: čistota, oplach a sušenie sú dôležitejšie než silná vôňa. Až keď je textil suchý a bez pachu, má zmysel riešiť jemný voňavý dojem.",
        ],
        "faq_title": "pranie chrániča matraca",
        "faq": [
            ("Môžem prať chránič matraca na 60 stupňov?", "Len vtedy, ak to povoľuje štítok. Pri alergii môže vyššia teplota pomôcť, ale nepremokavá vrstva alebo elastické časti ju nemusia zniesť."),
            ("Prečo chránič matraca po praní zapácha?", "Najčastejšie preto, že sa zle vypláchol, bol praný v preplnenom bubne alebo nepreschol úplne do hĺbky."),
            ("Môže ísť chránič matraca do sušičky?", "Niektoré áno, iné nie. Rozhoduje štítok a najmä prítomnosť membrány alebo citlivého záteru."),
            ("Ako často prať chránič matraca pri potení?", "Pri výraznom potení častejšie než v bežnej domácnosti. Praktické je mať náhradný kus, aby ste nemuseli vracať vlhký chránič na matrac."),
        ],
    },
    {
        "title": "Ako prať paplón a prikrývku: veľkosť bubna, výplň a sušenie bez zápachu",
        "short": "Paplón perte doma iba vtedy, keď sa voľne zmestí do bubna a štítok to povoľuje. Najdôležitejšie je dôkladné vypláchnutie a úplné vysušenie výplne.",
        "answer": "Paplón alebo prikrývku perte podľa výplne a štítku. Duté vlákno často zvládne domáce pranie, páperie, vlna alebo veľký objem môžu vyžadovať čistiareň alebo väčší bubon. Ak sa prikrývka v práčke stlačí do tvrdej gule, nevyperie sa dobre. Po praní ju sušte úplne do hĺbky, inak začne zapáchať.",
        "quick": [
            "<strong>Veľkosť bubna:</strong> paplón sa musí v bubne pohybovať, nie byť natlačený na doraz.",
            "<strong>Výplň:</strong> duté vlákno, perie, vlna a zmesi potrebujú odlišný prístup.",
            "<strong>Oplach:</strong> zvyšky pracieho prostriedku vo výplni sú častý dôvod ťažkého pachu.",
            "<strong>Sušenie:</strong> výplň musí byť suchá aj v strede, nielen po povrchu.",
            "<strong>Čistiareň:</strong> pri veľkom alebo citlivom paplóne je bezpečnejšia než malý domáci bubon.",
        ],
        "intro": [
            "Paplón a prikrývka sú objemné textílie, ktoré sa správajú inak než obliečky. Majú výplň, švy, rohy a veľa priestoru, kde sa drží vlhkosť. Preto nestačí položiť otázku, či sa zmestia do práčky. Dôležité je, či sa v nej dokážu aj prať, vypláchnuť a neskôr úplne vysušiť.",
            "Najväčší problém vzniká vtedy, keď je prikrývka pre bubon príliš veľká. Navonok program prebehne, ale voda a prací prostriedok sa nedostanú rovnomerne cez celú výplň. Výsledkom môže byť mokrá ťažká hmota, fľaky, zle vypláchnutý gél a zatuchnutý pach po jednom alebo dvoch dňoch sušenia.",
            "Druhý rozdiel je typ výplne. Syntetické duté vlákno býva na domácu údržbu tolerantnejšie, no páperie, vlna alebo špeciálne výplne potrebujú opatrnosť. Ak výrobca odporúča profesionálne čistenie, je to často preto, že výplň môže pri nesprávnom praní zhrudkovatieť alebo stratiť objem.",
            "Tento návod rieši otázky ako ako prať paplón v práčke, ako prať prikrývku z dutého vlákna, ako sušiť paplón bez zápachu, kedy použiť sušičku a kedy je lepšie odniesť prikrývku do čistiarne.",
        ],
        "why_heading": "Prečo paplón po praní zhrudkovatie alebo zapácha",
        "why": [
            "Objemná výplň potrebuje pri praní priestor. Ak je paplón v bubne natlačený, mechanický pohyb je slabý a voda neprúdi cez celý objem. Prací prostriedok sa môže usadiť vo výplni a oplach ho nedostane von.",
            "Zápach po praní často nesúvisí s tým, že by ste použili slabý produkt. Vzniká preto, že výplň schne príliš dlho, vnútri ostáva vlhká alebo bola zle vypláchnutá. Povrch môže byť suchý na dotyk, ale stred prikrývky môže ešte držať vlhkosť.",
            "Pri perovej alebo páperovej výplni je rizikom zhlukovanie. Perie potrebuje šetrný režim a dlhé, vzdušné sušenie. Pri vlne je zase problém zrážanie a plstnatenie. Preto sa pri výplniach neoplatí hádať, ale čítať štítok.",
            "Paplón tiež nesie veľa prachu a zvyškov kože, aj keď je v obliečke. Obliečka znižuje znečistenie, ale nenahrádza pranie alebo profesionálne čistenie samotnej prikrývky.",
        ],
        "decision_rows": [
            ("Duté vlákno", "často vhodné na domáce pranie", "overiť štítok a bubon nepreplniť"),
            ("Páperie alebo perie", "riziko zhlukov a dlhého sušenia", "prať iba podľa štítku, často lepšie profesionálne"),
            ("Vlnená prikrývka", "zrážanie a plstnatenie", "veľmi opatrne alebo čistiareň"),
            ("Veľký dvojpaplón", "malý bubon ho nevyperie rovnomerne", "využiť väčšiu práčku alebo čistiareň"),
            ("Paplón po chorobe", "vyššia hygienická záťaž", "riadiť sa štítkom a zvážiť profesionálne čistenie"),
        ],
        "steps": [
            "Najprv skontrolujte štítok a typ výplne.",
            "Overte, či sa paplón v bubne voľne pohybuje aj po nasiaknutí vodou.",
            "Nepoužívajte zbytočne veľa pracieho gélu, výplň sa vyplachuje ťažšie než hladký textil.",
            "Zvoľte program podľa výplne, nie podľa obliečok.",
            "Pri objemnej prikrývke pridajte dôkladný oplach, ak to práčka umožňuje.",
            "Po praní paplón pretrepte, aby sa výplň uvoľnila.",
            "Sušte dlhšie, pravidelne otáčajte a kontrolujte hrubé miesta.",
            "Do postele ho vráťte až vtedy, keď je úplne suchý a bez vlhkého pachu.",
        ],
        "check_rows": [
            ("Paplón sa ledva zmestí", "slabé pranie a oplach", "neprať doma v malom bubne"),
            ("Výplň je v hrudkách", "nesprávny program alebo sušenie", "sušiť pomaly, pretriasať, pri citlivej výplni čistiareň"),
            ("Pach po dvoch dňoch", "vlhkosť ostala v strede", "dosušiť, vetrať, nabudúce nepreplniť"),
            ("Biele mapy", "zvyšky gélu", "menej produktu a lepší oplach"),
            ("Strata objemu", "výplň sa poškodila alebo zľahla", "dodržiavať pokyny výrobcu"),
        ],
        "mistakes": [
            "Prať veľký paplón v bubne, kde sa nedokáže voľne otočiť.",
            "Použiť veľa pracieho gélu a očakávať, že sa z výplne ľahko vypláchne.",
            "Sušiť prikrývku zloženú alebo prehodenú tak, že stred ostane vlhký.",
            "Neriadiť sa výplňou a prať perie, vlnu aj syntetiku rovnakým programom.",
            "Navliecť obliečku na paplón skôr, než je výplň suchá.",
            "Zakryť vlhký pach vôňou namiesto dôkladného dosušenia.",
        ],
        "sections": [
            ("Ako prať paplón v práčke", [
                "Domáce pranie paplóna má zmysel iba vtedy, keď má práčka dostatočný objem. Dôležitá nie je len suchá veľkosť. Po nasiaknutí vodou je paplón ťažší a potrebuje priestor, aby sa voda dostala cez výplň.",
                "Ak sa paplón v bubne nehýbe, pranie bude skôr namáčanie než čistenie. V takom prípade je lepšia väčšia práčka v práčovni alebo profesionálne čistenie. Ušetríte si zápach, hrudky a riziko preťaženia spotrebiča.",
            ]),
            ("Ako prať prikrývku z dutého vlákna", [
                "Duté vlákno býva praktické, pretože mnohé prikrývky sú navrhnuté na domáce pranie. Aj tak však čítajte štítok. Rozdiel môže byť v teplote, odstreďovaní a možnosti sušenia v sušičke.",
                "Použite primeranú dávku gélu a dlhší oplach. Pri syntetickej výplni je častým problémom nie špina, ale zvyšky pracieho prostriedku uväznené vo vnútri. Práve tie môžu po usušení pôsobiť ťažko alebo chemicky.",
            ]),
            ("Ako prať páperový paplón", [
                "Páperový paplón je citlivejší. Perie potrebuje šetrné pranie a veľmi dôkladné sušenie, inak sa môže zhlukovať. Ak štítok domáce pranie nepovoľuje alebo nemáte dostatočný bubon, čistiareň je bezpečnejšia voľba.",
                "Pri domácom praní páperia nikdy neimprovizujte s vysokou teplotou alebo agresívnym žmýkaním. Cieľom nie je len odstrániť špinu, ale zachovať vzdušnosť a pružnosť výplne.",
            ]),
            ("Ako sušiť paplón bez zápachu", [
                "Sušenie paplóna môže trvať výrazne dlhšie než sušenie obliečok. Počas sušenia ho viackrát pretrepte a otočte. Kontrolujte hlavne rohy, hrubé švy a stred, kde sa vlhkosť drží najdlhšie.",
                "Ak paplón po usušení pôsobí ťažko alebo cítiť vlhkosť, nepoužívajte ho. Dosušte ho. Spánok pod mierne vlhkou prikrývkou môže zhoršiť zatuchnutie celej postele.",
            ]),
            ("Ako často prať paplón a prikrývku", [
                "Paplón sa neperie tak často ako obliečky, ale nemal by byť roky bez údržby. Interval závisí od potenia, alergií, domácich zvierat, sezóny a toho, či používate obliečku a chránič matraca.",
                "Medzi praniami pomáha pravidelné vetranie, pretrepanie a rýchle riešenie nehôd. Ak sa objaví zatuchnutý pach, nečakajte, kým sa prenesie do obliečok a plachty.",
            ]),
            ("Kedy dať paplón do čistiarne", [
                "Čistiareň dáva zmysel pri veľkých rozmeroch, citlivej výplni, páperí, vlne alebo nejasnom štítku. Domáca práčka nie je test odolnosti prikrývky. Ak sa paplón v bubne neotáča, výsledok nebude spoľahlivý.",
                "Profesionálne čistenie je vhodné aj po výraznom znečistení, nehode alebo pri drahej prikrývke, kde by poškodenie výplne stálo viac než údržba.",
            ]),
        ],
        "caution": [
            "Pri paplónoch a prikrývkach neplatí, že viac tepla automaticky znamená lepšiu hygienu. Výplň a švy môžu mať vlastné limity, preto je štítok rozhodujúci.",
            "Ak máte alergiu alebo astmu, neberte tento článok ako zdravotnú liečbu. Berte ho ako praktickú rutinu prania a znižovania prachu v spálni; zdravotný režim riešte s odborníkom.",
        ],
        "expert": [
            "Zdroje o roztočoch spájajú posteľné textílie s prachom, vlhkosťou a pravidelným praním. Pri paplóne to však musí byť vyvážené s ochranou výplne. Vyššia teplota môže mať hygienický význam, ale len vtedy, ak ju prikrývka povoľuje.",
            "EPA pri vlhkosti opakovane zdôrazňuje kontrolu vody a dôkladné vysušenie. Pri objemnej prikrývke je to prakticky najdôležitejšia časť. Zle vysušený paplón môže znehodnotiť aj správne pranie.",
            "Zákaznícky pohľad je jednoduchý: ak chcete čistý a príjemný paplón, neriešte len program v práčke. Riešte aj bubon, výplň, oplach a dĺžku sušenia.",
        ],
        "faq_title": "pranie paplóna a prikrývky",
        "faq": [
            ("Môžem prať paplón doma?", "Áno, ak to povoľuje štítok a paplón sa v bubne voľne pohybuje. Ak je natlačený, radšej použite väčšiu práčku alebo čistiareň."),
            ("Prečo paplón po praní smrdí?", "Najčastejšie preto, že výplň nepreschla do hĺbky alebo v nej zostali zvyšky pracieho prostriedku."),
            ("Ako prať páperovú prikrývku?", "Iba podľa štítku. Pri neistote, malom bubne alebo drahej prikrývke je bezpečnejšia čistiareň."),
            ("Kedy navliecť obliečku späť na paplón?", "Až keď je prikrývka úplne suchá v strede, v rohoch aj pri švoch."),
        ],
    },
    {
        "title": "Ako prať plachtu s gumou: rohy, pot, zrazenie a sušenie bez pokrčenia",
        "short": "Plachtu s gumou perte naruby alebo voľne rozloženú, nepreplňte bubon a po praní ju hneď vytraste. Najviac trpia rohy, guma a miesta, kde sa drží pot.",
        "answer": "Plachtu s gumou perte podľa materiálu a štítku, najlepšie s podobnou posteľnou bielizňou, ale nie v preplnenom bubne. Pred praním vyrovnajte rohy, odstráňte vlasy a väčšie nečistoty, dávkujte prací gél striedmo a po praní ju hneď vytraste. Aby sa nezrazila, neprekračujte odporúčanú teplotu a nesušte ju zbytočne horúco.",
        "quick": [
            "<strong>Rohy:</strong> pred praním ich vytiahnite, aby sa v nich nedržal prach a vlasy.",
            "<strong>Guma:</strong> chráňte ju pred vysokým teplom, ak to štítok neodporúča.",
            "<strong>Pot:</strong> pri častom potení perte plachtu častejšie než dekoratívne textílie.",
            "<strong>Zrazenie:</strong> riziko zvyšuje vysoká teplota a horúce sušenie.",
            "<strong>Pokrčenie:</strong> plachtu vyberte hneď po praní a napnite ju ešte pred sušením.",
        ],
        "intro": [
            "Plachta s gumou je v priamom kontakte s telom každú noc. Zachytáva pot, kožný maz, prach, vlasy, zvyšky krémov a drobné nečistoty z matraca. Zároveň má rohy a elastické časti, ktoré sa správajú inak než rovná plachta. Preto sa pri praní ľahko stane, že sa rohy zrolujú a nevyperú poriadne.",
            "Mnohí ju hádžu do práčky ako bežný kus posteľnej bielizne. To často stačí, ale nie vždy. Pri preplnenom bubne sa z plachty môže stať veľké vrecko, ktoré v sebe drží iné textílie. Výsledkom je slabší oplach, pokrčenie a miestami horšie vypranie.",
            "Kvalitné pranie plachty s gumou je hlavne o príprave a sušení. Vytiahnuť rohy, nenechať ju stáť mokrú v práčke, neprehnať dávku pracieho prostriedku a nepoužiť teplo, ktoré poškodí pružnosť. Takáto rutina predĺži životnosť plachty aj pohodlie pri spaní.",
            "Nižšie nájdete odpovede na praktické otázky: ako prať napínaciu plachtu, ako prať plachtu s gumou aby sa nezrazila, ako odstrániť pot z plachty, ako ju sušiť bez pokrčenia a čo robiť, keď rohy po praní stále zapáchajú.",
        ],
        "why_heading": "Prečo sa plachta s gumou zle vyperie alebo zrazí",
        "why": [
            "Plachta s gumou má tvar, ktorý sa pri praní ľahko zbalí. Rohy vytvoria kapsy, v ktorých sa drží voda, vlasy, ponožka alebo menší kus bielizne. Ak sa to stane, časť textilu sa perie horšie a zvyšky pracieho prostriedku sa môžu držať práve v záhyboch.",
            "Zrazenie najčastejšie súvisí s materiálom, teplotou a sušením. Bavlna, jersey, froté aj zmesi majú rozdielnu toleranciu. Plachta, ktorá sedí na matrac presne, po horúcom praní alebo sušení zrazu nejde natiahnuť cez rohy.",
            "Pot sa hromadí hlavne v miestach, kde leží trup a hlava. Ak sa plachta perie na veľmi krátkom programe alebo v preplnenom bubne, pach sa môže vrátiť hneď po prvej noci. Vôňa do prania to nevyrieši, ak plachta nie je skutočne vypraná a vypláchnutá.",
            "Pokrčenie vzniká aj po praní. Keď plachta ostane mokrá stočená v práčke, záhyby sa zafixujú. Stačí ju vybrať skôr, vytriasť a pri sušení napnúť rohy.",
        ],
        "decision_rows": [
            ("Jersey plachta", "pružná, ale môže sa vytiahnuť alebo zraziť", "prať podľa štítku a neprehrievať"),
            ("Bavlnená plachta", "znesie bežné pranie, ale krčí sa", "vybrať hneď po praní a napnúť"),
            ("Froté plachta", "drží viac vody a prachu", "nepreplniť bubon a dobre sušiť"),
            ("Plachta po potení", "pach sa drží v strede plochy", "prať pravidelne a použiť dostatočný program"),
            ("Plachta s oslabenou gumou", "guma môže stratiť pružnosť", "vyhnúť sa príliš vysokému teplu"),
        ],
        "steps": [
            "Pred praním plachtu straste a odstráňte vlasy alebo väčšie nečistoty.",
            "Vytiahnite rohy, aby neostali zrolované do klbka.",
            "Perte s podobnými textíliami, nie s ťažkými uterákmi alebo objemným paplónom.",
            "Dávkujte prací gél podľa náplne, nie podľa toho, že plachta je veľká.",
            "Vyberte teplotu podľa štítku a materiálu.",
            "Po praní plachtu hneď vyberte, pretrepte a vyrovnajte rohy.",
            "Sušte rozloženú alebo napnutú tak, aby sa guma zbytočne neprehrievala.",
            "Skladajte ju až úplne suchú, inak sa v rohoch môže objaviť zatuchnutie.",
        ],
        "check_rows": [
            ("Rohy plachty", "držia vlasy a prach", "pred praním ich vyrovnať"),
            ("Elastická guma", "citlivá na teplo", "neprehrievať a nepresušovať"),
            ("Stred plachty", "najviac potu a kožného mazu", "nepoužiť príliš krátky program"),
            ("Biely povlak", "zvyšky gélu", "menej produktu a lepší oplach"),
            ("Zrazenie", "teplota a sušička", "riadiť sa štítkom"),
        ],
        "mistakes": [
            "Prať plachtu s gumou spolu s paplónom, ktorý zaberie celý bubon.",
            "Nechať rohy zrolované a dúfať, že sa v práčke samy otvoria.",
            "Použiť vysokú teplotu bez kontroly materiálu a potom riešiť zrazenie.",
            "Nechať mokrú plachtu v práčke niekoľko hodín.",
            "Dávať veľa pracieho gélu, ktorý sa drží v záhyboch.",
            "Sušiť gumu príliš horúco alebo príliš dlho.",
        ],
        "sections": [
            ("Ako prať napínaciu plachtu", [
                "Napínacia plachta potrebuje priestor. Ak ju vložíte do preplneného bubna, zabalí do seba ďalšie kusy a časť textilu sa perie horšie. Pred praním ju rozložte, vytiahnite rohy a nedávajte k nej príliš ťažké textílie.",
                "Pri bežnom praní použite primeranú dávku pracieho gélu a program podľa materiálu. Ak je plachta len mierne používaná, nemusí potrebovať extrémne dlhý program. Pri potení alebo zápachu však krátky cyklus často nestačí.",
            ]),
            ("Ako prať plachtu s gumou aby sa nezrazila", [
                "Zrazenie nie je iba otázka prania. Veľkú rolu hrá sušenie. Ak plachtu vyperiete na vhodnej teplote, ale potom ju vystavíte príliš horúcej sušičke, výsledok môže byť rovnaký: horšie sa natiahne cez matrac.",
                "Najbezpečnejší postup je držať sa štítku, nepreháňať teplotu a po praní plachtu napnúť. Ak je matrac vysoký, nechajte si radšej malú rezervu a nekupujte plachtu, ktorá sedí už pred prvým praním úplne natesno.",
            ]),
            ("Ako odstrániť pot z plachty", [
                "Pot sa najviac drží v strede plachty a v oblasti hlavy. Ak sú tam mapy alebo zápach, pomôže skoršie pranie, primeraný program a dôkladný oplach. Dlhé skladovanie spotenej plachty v koši problém zhoršuje.",
                "Pri opakovanom zápachu skontrolujte aj matracový chránič, obliečky a vankúš. Plachta môže byť čistá, ale pach sa vráti z textilu pod ňou alebo nad ňou.",
            ]),
            ("Ako sušiť plachtu s gumou bez pokrčenia", [
                "Po praní plachtu hneď vyberte a silno pretrepte. Rohy vyrovnajte rukou. Ak ju sušíte na sušiaku, rozložte ju tak, aby sa v rohoch nedržala vlhkosť a aby guma nebola stočená v mokrom uzle.",
                "Pri sušičke sa riaďte štítkom. Nižšia teplota a skoršie vybratie často pomôžu viac než úplné presušenie na vysokom stupni. Plachtu dosušte voľne a potom ju zložte.",
            ]),
            ("Plachta s gumou a vôňa v posteli", [
                "Vôňa na plachte má byť jemná, pretože je v priamom kontakte s pokožkou a dýchate pri nej celú noc. Ak používate parfumy do prania, začnite nízkou intenzitou a sledujte, či vás pri spaní nerušia.",
                "Ak plachta zapácha zatuchnuto, vôňa nie je riešenie. Najprv skontrolujte sušenie, matracový chránič, vankúš a vetranie spálne. Až čistý suchý textil má zmysel jemne prevoňať.",
            ]),
            ("Kedy plachtu radšej vymeniť", [
                "Ak je guma vyťahaná, plachta sa z matraca stále vyťahuje alebo sú rohy stenčené, pranie už problém nevyrieši. Starý textil sa môže horšie napínať, viac krčiť a horšie sedieť na matraci.",
                "Pri poškodených vláknach tiež rastie riziko žmolkov a drsného povrchu. Vtedy je lepšie plachtu presunúť na menej náročné použitie alebo ju vymeniť.",
            ]),
        ],
        "caution": [
            "Pri plachte s gumou si dávajte pozor na teplotu sušenia. Elastické časti môžu starnúť rýchlejšie, ak ich pravidelne vystavujete príliš vysokému teplu.",
            "Pri citlivej pokožke a detskej posteli používajte jemnejšiu parfumáciu alebo ju vynechajte. Komfort pri spánku je dôležitejší než výrazná vôňa.",
        ],
        "expert": [
            "Odborné odporúčania k spálni často spájajú prach, roztoče a vlhkosť. Plachta je prvá vrstva, ktorá zachytáva pot a prach z tela, preto jej pravidelné pranie dáva praktický zmysel aj vtedy, keď nie je viditeľne špinavá.",
            "EPA odporúča udržiavať vnútornú vlhkosť približne v rozumnom pásme a riešiť miesta, kde sa drží voda. Pri plachte s gumou to znamená nenechávať vlhké rohy a záhyby uzavreté v skrini alebo priamo na matraci.",
            "Pri starostlivosti o plachtu je preto najlepšia jednoduchá opakovateľná rutina: správna teplota, primeraná dávka, nepreplnený bubon, rýchle vybratie a úplné vysušenie.",
        ],
        "faq_title": "pranie plachty s gumou",
        "faq": [
            ("Ako prať plachtu s gumou, aby sa nezrazila?", "Držte sa štítku, neprekračujte teplotu a nesušte ju zbytočne horúco. Po praní ju vytraste a vyrovnajte rohy."),
            ("Môžem prať plachtu s obliečkami?", "Áno, ak bubon nie je preplnený. Vyhnite sa kombinácii s veľkým paplónom alebo ťažkými uterákmi."),
            ("Prečo rohy plachty po praní zapáchajú?", "Často v nich ostane vlhkosť, vlasy alebo zvyšky pracieho prostriedku. Pred praním ich vyrovnajte a po praní dobre vysušte."),
            ("Je vhodný parfum do prania na plachty?", "Áno, ale jemne. Plachta je blízko pokožky a tváre, preto je lepšia nižšia intenzita než výrazná vôňa."),
        ],
    },
    {
        "title": "Ako prať prehoz na posteľ: prach, chlpy, objem a sušenie",
        "short": "Prehoz na posteľ perte podľa materiálu, objemu a množstva prachu alebo chlpov. Pred praním ho vytraste, odstráňte chlpy a neperte ho v malom bubne, kde sa iba zroluje.",
        "answer": "Prehoz na posteľ pred praním vytraste, odstráňte chlpy a skontrolujte štítok. Ľahký bavlnený alebo syntetický prehoz často zvládne domáce pranie, ťažký prešívaný, vlnený alebo dekoratívny prehoz môže potrebovať čistiareň. Perte ho vo voľnom bubne, s primeranou dávkou gélu a sušte rozložený, aby nezostal vlhký v záhyboch.",
        "quick": [
            "<strong>Prach:</strong> pred praním prehoz vytraste alebo povysávajte, aby ste nezaťažili práčku.",
            "<strong>Chlpy:</strong> najprv ich odstráňte valčekom alebo kefou, pranie ich nemusí všetky uvoľniť.",
            "<strong>Objem:</strong> ak sa prehoz v bubne nehýbe, radšej ho neperte doma.",
            "<strong>Dekorácie:</strong> strapce, výšivky a štruktúra potrebujú šetrnejší prístup.",
            "<strong>Sušenie:</strong> rozložiť, pretrepať a dosušiť záhyby.",
        ],
        "intro": [
            "Prehoz na posteľ býva medzi dekoračným a praktickým textilom. Cez deň chráni posteľ pred prachom, sadaním, domácimi zvieratami a oblečením, ktoré na posteľ odložíme. Zároveň je často veľký, ťažký alebo štruktúrovaný, takže sa neperie tak jednoducho ako plachta.",
            "Práve prehoz je textil, ktorý v spálni zbiera prach aj vtedy, keď sa na ňom priamo nespí. Ak máte doma psa alebo mačku, pridajú sa chlpy. Ak prehoz skladáte na noc na stoličku alebo na zem, zbiera ďalšie nečistoty. Preto má zmysel zaradiť ho do sezónnej alebo pravidelnej starostlivosti.",
            "Najčastejšia chyba je vložiť veľký prehoz do práčky bez prípravy. Chlpy sa v práčke nemusia stratiť, prach zaťaží filter a veľký kus sa zroluje do jednej masy. Výsledok potom nie je svieži, hoci program prebehol.",
            "Tento návod rieši, ako prať prehoz na posteľ v práčke, ako odstrániť chlpy z prehozu, ako prať prešívaný alebo dekoratívny prehoz, ako ho sušiť a kedy je rozumnejšie zvoliť čistiareň.",
        ],
        "why_heading": "Prečo je prehoz po praní ťažký, chlpatý alebo zapácha",
        "why": [
            "Prehoz má často väčší objem než bežné obliečky. Ak sa v bubne zroluje, voda a prací prostriedok sa nedostanú rovnomerne ku všetkým plochám. Vnútorné záhyby sa len namočia, ale nevyperú.",
            "Chlpy domácich zvierat sa pri praní správajú inak než prach. Časť sa uvoľní, časť ostane prichytená a časť sa presunie na inú bielizeň alebo do filtra. Preto je mechanické odstránenie pred praním veľmi dôležité.",
            "Dekoratívne prvky sú ďalším rizikom. Strapce, tkané vzory, výšivky alebo hrubé švy môžu zachytávať vodu a prací prostriedok. Ak ich po praní nevyrovnáte a nevysušíte, zatuchnutie sa objaví práve tam.",
            "Prehoz je tiež často textil, ktorý sa perie menej často. Prach, pach miestnosti a zvyšky z kontaktu s oblečením sa potom hromadia pomaly, takže výsledok jedného krátkeho programu nemusí stačiť.",
        ],
        "decision_rows": [
            ("Ľahký bavlnený prehoz", "bežnejšie domáce pranie", "prať s priestorom v bubne"),
            ("Prešívaný prehoz", "drží vodu vo švoch", "dôkladne sušiť a pretriasať"),
            ("Prehoz s chlpmi", "chlpy sa môžu držať aj po praní", "odstrániť ich pred vložením do práčky"),
            ("Vlnený alebo tkaný prehoz", "zrazenie, deformácia, plstnatenie", "štítok alebo čistiareň"),
            ("Dekoratívny prehoz so strapcami", "poškodenie detailov", "šetrný režim alebo profesionálne čistenie"),
        ],
        "steps": [
            "Prehoz vytraste vonku alebo pri otvorenom okne, ak to podmienky dovoľujú.",
            "Chlpy odstráňte valčekom, kefou alebo rukavicou pred praním.",
            "Skontrolujte štítok, materiál, dekorácie a rozmery.",
            "Do bubna vložte prehoz tak, aby sa vedel pohybovať.",
            "Nepoužívajte nadmernú dávku pracieho gélu.",
            "Zvoľte program podľa najcitlivejšej časti prehozu.",
            "Po praní ho pretrepte, vyrovnajte švy a strapce.",
            "Sušte rozložený a pred uložením skontrolujte hrubšie miesta.",
        ],
        "check_rows": [
            ("Prach na povrchu", "zaťaží pranie a filter", "vytriasť alebo povysávať"),
            ("Chlpy zvierat", "ostávajú na vlákne", "odstrániť pred praním"),
            ("Hrubé švy", "držia vlhkosť", "dosušiť a kontrolovať"),
            ("Strapce", "zamotanie a deformácia", "použiť šetrný postup"),
            ("Veľký rozmer", "slabý pohyb v bubne", "zvoliť väčšiu práčku"),
        ],
        "mistakes": [
            "Prať prehoz bez odstránenia chlpov a prachu.",
            "Natlačiť veľký prehoz do malej práčky.",
            "Použiť rovnaký program na hladký bavlnený aj dekoratívny prehoz.",
            "Sušiť prehoz zložený tak, že švy ostanú vlhké.",
            "Použiť veľa vône, aby prekryla pach miestnosti.",
            "Skladovať prehoz späť do skrine skôr, než je úplne suchý.",
        ],
        "sections": [
            ("Ako prať prehoz na posteľ v práčke", [
                "Domáce pranie je vhodné vtedy, keď prehoz nie je príliš veľký a štítok ho povoľuje. V bubne musí mať priestor. Ak ho musíte silou zatlačiť dnu, pranie nebude rovnomerné a môžete preťažiť práčku.",
                "Použite primeraný prací gél a program podľa materiálu. Pri prešívaní alebo hrubšom textile je dôležitý oplach. Zvyšky produktu vo švoch môžu po usušení pôsobiť ako ťažký alebo zatuchnutý pach.",
            ]),
            ("Ako odstrániť chlpy z prehozu pred praním", [
                "Chlpy najlepšie odstránite ešte nasucho. Pomôže valček, gumová rukavica, kefa alebo vysávač s vhodným nadstavcom. Práčka nemá byť prvý filter na všetky chlpy zo spálne.",
                "Ak dáte chlpatý prehoz rovno do práčky, chlpy sa môžu presunúť na iné textílie alebo zostať v tesnení a filtri. To zhorší aj ďalšie pranie.",
            ]),
            ("Ako prať prešívaný prehoz", [
                "Prešívaný prehoz drží vodu v švoch a medzi vrstvami. Po praní ho preto nenechajte stočený. Pretrepte ho, vyrovnajte a počas sušenia otočte. Najdlhšie schnú práve miesta, ktoré pôsobia najpevnejšie.",
                "Ak je prešívanie husté a prehoz veľký, zvážte väčší bubon. Krásny prehoz znehodnotí skôr zlé sušenie než samotné pranie.",
            ]),
            ("Ako prať dekoračný prehoz so strapcami", [
                "Strapce a ozdobné okraje sa môžu v bubne zamotať alebo zdeformovať. Ak štítok povoľuje pranie, použite šetrnejší režim a neperte ho s textíliami, ktoré majú zipsy alebo háčiky.",
                "Po praní strapce rozčešte prstami a nechajte ich uschnúť voľne. Neťahajte za ne silou, keď sú mokré, pretože vlákna sú vtedy zraniteľnejšie.",
            ]),
            ("Ako často prať prehoz na posteľ", [
                "Ak prehoz používate denne ako ochranu postele, perte ho častejšie než čisto dekoratívny kus. Pri domácich zvieratách, prachu alebo častom sedení na posteli má zmysel kratší interval.",
                "Medzi praniami pomáha vytrasenie, vysávanie a vetranie. Ak však prehoz cítiť po miestnosti, prachu alebo zvieratách, samotné vetranie nestačí.",
            ]),
            ("Kedy prehoz radšej zveriť čistiarni", [
                "Čistiareň je rozumná pri vlne, veľkom rozmere, výšivkách, strapcoch, nejasnom zložení alebo drahom kuse. Ak neviete, ako sa materiál zachová vo vode, neriskujte ostrý domáci test.",
                "Profesionálna údržba dáva zmysel aj vtedy, keď prehoz presahuje kapacitu práčky. To nie je pohodlnostný problém, ale technický limit prania.",
            ]),
            ("Ako prehoz skladovať, aby znovu nenatiahol prach", [
                "Ak prehoz používate iba sezónne, pred uložením ho nechajte úplne vyschnúť a vyvetrať. Skladovanie mierne vlhkého alebo zaprášeného textilu v uzavretom vaku je častý dôvod, prečo po vytiahnutí cítiť zatuchnutie.",
                "Do skrine ho ukladajte voľnejšie, nie natlačený medzi vlhké uteráky alebo textílie zo športu. Pri dlhšom skladovaní pomôže priedušný obal a čistá suchá polica. Keď ho po sezóne znovu vyberiete, najprv ho vytraste a až potom rozhodnite, či stačí vetranie alebo potrebuje pranie.",
            ]),
        ],
        "caution": [
            "Prehoz môže mať dekoračné časti, ktoré nie sú určené na bežné pranie. Pred praním skontrolujte nielen materiál hlavnej plochy, ale aj okraje, výšivku, aplikácie a štítok.",
            "Pri prehoze od domácich zvierat najprv odstráňte chlpy mechanicky. Inak sa problém iba presunie do práčky, filtra alebo na ďalšie textílie.",
        ],
        "expert": [
            "Prach a roztoče sa viažu na textílie v okolí postele, nielen na obliečky. American Lung Association uvádza, že alergény roztočov sa držia v prachu a textíliách, najmä v matracoch, posteľnej bielizni, čalúnení a závesoch. Prehoz preto nie je iba dekorácia, ale aj zachytávač prachu.",
            "EPA pri kvalite vnútorného vzduchu odporúča udržiavať priestory čisté a suché. Pri prehoze to znamená neprať ho iba výnimočne, ale zároveň ho po praní nevracať vlhký do spálne.",
            "Prakticky platí: čím väčší a štruktúrovanejší textil, tým viac rozhoduje príprava pred praním a sušenie po praní. Samotný program v práčke je len stred procesu.",
        ],
        "faq_title": "pranie prehozu na posteľ",
        "faq": [
            ("Môžem prať prehoz na posteľ v práčke?", "Áno, ak to povoľuje štítok a prehoz sa voľne zmestí do bubna. Veľký alebo dekoratívny kus radšej čistiareň."),
            ("Ako odstrániť chlpy z prehozu?", "Najprv nasucho valčekom, kefou, rukavicou alebo vysávačom. Pranie nechajte až ako ďalší krok."),
            ("Prečo prehoz po praní zapácha?", "Mohol byť preplnený bubon, slabý oplach alebo nedostatočné sušenie v švoch a záhyboch."),
            ("Môžem použiť parfum do prania?", "Áno, jemne a až na čistý textil. Nepoužívajte vôňu na prekrytie prachu, chlpov alebo zatuchnutia."),
        ],
    },
    {
        "title": "Ako prať posteľnú sukňu a textílie okolo postele: prach, chlpy a sezónne pranie",
        "short": "Posteľná sukňa, volány a textílie okolo postele zbierajú prach pri podlahe, chlpy a nečistoty zo spálne. Pred praním ich vytraste, skontrolujte uchytenie a perte šetrne podľa materiálu.",
        "answer": "Posteľnú sukňu a textílie okolo postele perte podľa materiálu a spôsobu uchytenia. Najprv ich zložte, vytraste prach, odstráňte chlpy a skontrolujte, či nemajú suchý zips, háčiky alebo dekorácie. Perte ich s ľahkými podobnými textíliami, nepreplňte bubon a po praní ich vyrovnajte, aby sa volány a rohy nepokrčili.",
        "quick": [
            "<strong>Prach pri podlahe:</strong> posteľná sukňa ho zachytáva viac než obliečky.",
            "<strong>Chlpy:</strong> odstráňte ich pred praním, najmä ak textil siaha k podlahe.",
            "<strong>Uchytenie:</strong> suché zipsy, háčiky a pásy zapnite alebo chráňte.",
            "<strong>Sezónnosť:</strong> aj menej používaný textil treba občas vyprať alebo vyvetrať.",
            "<strong>Žehlenie:</strong> pokrčenie znížite rýchlym vybratím z práčky a napnutím pri sušení.",
        ],
        "intro": [
            "Posteľná sukňa, textilné volány, návleky na čelo postele, dekoračné panely a textílie okolo lôžka sa často prehliadajú. Nie sú v priamom kontakte s telom ako plachta, no zbierajú prach zo spálne, chlpy, vlákna z kobercov a nečistoty pri podlahe. Práve preto môžu zhoršovať pocit čistoty v spálni.",
            "Tieto textílie sa perú menej často a mnohé domácnosti si na ne spomenú až pri veľkom jarnom upratovaní. To nie je automaticky zlé, ale ak máte doma alergika, domáce zvieratá alebo posteľ s úložným priestorom, prach sa pri posteli drží viac, než si človek všimne.",
            "Pri praní je dôležité skontrolovať uchytenie. Niektoré posteľné sukne majú suchý zips, gumičky, pásy pod matrac, háčiky alebo kombináciu materiálov. Ak ich dáte do práčky bez prípravy, môžu sa zachytiť, zdeformovať alebo poškodiť iné textílie.",
            "Tento článok vysvetľuje, ako prať posteľnú sukňu, ako čistiť textílie okolo postele, ako odstrániť prach a chlpy, ako často ich prať sezónne a kedy stačí vysávanie alebo vetranie.",
        ],
        "why_heading": "Prečo textílie okolo postele zhoršujú pocit čistoty",
        "why": [
            "Textílie pri podlahe sú v zóne, kde sa usádza prach. Pri chôdzi, vysávaní, ukladaní vecí pod posteľ alebo pohybe domácich zvierat sa prach víri a zachytáva sa na látke. Posteľná sukňa potom vyzerá nenápadne, ale drží nečistoty.",
            "Ak textil prekrýva úložný priestor pod posteľou, môže zároveň obmedzovať prúdenie vzduchu. V kombinácii s prachom a občasnou vlhkosťou vzniká zatuchnutý dojem, ktorý sa prenesie na celú spálňu.",
            "Chlpy sú samostatná kapitola. Na zvislých textíliách sa zachytávajú ľahko a práčka ich nemusí odstrániť všetky. Ak ich neodstránite pred praním, časť skončí v tesnení, filtri alebo na iných kusoch bielizne.",
            "Pri sezónnych textíliách je problém aj skladovanie. Ak ich uložíte mierne vlhké alebo zaprášené, po vytiahnutí zo skrine budú cítiť zatuchnuto, aj keď neboli používané.",
        ],
        "decision_rows": [
            ("Posteľná sukňa pri podlahe", "veľa prachu a chlpov", "vytriasť, odstrániť chlpy, prať šetrne"),
            ("Volány a dekoračné pásy", "pokrčenie a deformácia", "vybrať hneď po praní a napnúť"),
            ("Textil so suchým zipsom", "zachytí iné tkaniny", "zapnúť alebo prať v ochrannom vaku"),
            ("Návlek na čelo postele", "prach v hornej časti a kontakt s vlasmi", "vysávať medzi praniami"),
            ("Sezónne textílie zo skrine", "zatuchnutie zo skladovania", "pred použitím vyvetrať alebo vyprať"),
        ],
        "steps": [
            "Textíliu najprv opatrne zložte z postele a skontrolujte spôsob uchytenia.",
            "Vytraste prach a odstráňte chlpy ešte nasucho.",
            "Zapnite suché zipsy, háčiky alebo pásy, aby nepoškodili inú bielizeň.",
            "Perte s ľahkými podobnými textíliami, nie s uterákmi alebo paplónom.",
            "Dávkujte prací gél primerane, tieto textílie často nepotrebujú silnú dávku.",
            "Zvoľte program podľa najcitlivejšej časti.",
            "Po praní ich hneď vyberte, vyrovnajte volány a rohy.",
            "Skladujte až úplne suché a ideálne vo vzdušnom obale.",
        ],
        "check_rows": [
            ("Prach pri podlahe", "usádza sa denne", "vytriasť a vysávať medzi praniami"),
            ("Chlpy zvierat", "držia sa na zvislej látke", "odstrániť pred praním"),
            ("Suchý zips", "ničí jemné tkaniny", "zapnúť alebo zakryť"),
            ("Volány", "krčia sa v bubne", "prať voľne a vyrovnať"),
            ("Skladovanie", "riziko zatuchnutia", "ukladať úplne suché"),
        ],
        "mistakes": [
            "Prať posteľnú sukňu až vtedy, keď je viditeľne sivá od prachu.",
            "Nevyčistiť chlpy pred praním a nechať ich prejsť do práčky.",
            "Nezapnúť suchý zips alebo háčiky.",
            "Prať jemné volány spolu s ťažkými uterákmi.",
            "Skladovať textílie mierne vlhké v uzavretom boxe.",
            "Prevoňať zatuchnutý textil bez prania alebo vetrania.",
        ],
        "sections": [
            ("Ako prať posteľnú sukňu", [
                "Posteľnú sukňu najprv zložte tak, aby ste neťahali za švy alebo uchytenie. Ak je pod matracom, je praktické využiť prezliekanie celej postele. Textil vytraste a pozrite sa na spodný okraj, kde býva najviac prachu.",
                "Perte ju šetrne a s dostatkom priestoru. Volány a záhyby potrebujú voľný pohyb, inak sa iba zmačkajú. Po praní ju hneď vyberte, vyrovnajte a nechajte uschnúť tak, aby sa spodný lem nestočil.",
            ]),
            ("Ako čistiť textílie okolo postele medzi praniami", [
                "Medzi praniami pomôže vysávanie s jemným nadstavcom, vytriasanie a vetranie. Pri čele postele alebo dekoračných paneloch je praktické prejsť povrch handričkou alebo kefou podľa materiálu.",
                "Ak je textil blízko podlahy, riešte aj okolie postele. Prach pod posteľou sa bude vracať na látku, ak sa upratuje iba viditeľná časť spálne.",
            ]),
            ("Ako odstrániť chlpy z posteľnej sukne", [
                "Chlpy odstraňujte nasucho. Valček, gumová rukavica alebo kefa fungujú lepšie pred praním než po ňom. Pri dlhých chlpoch postupujte po smerovaní vlákna, aby ste látku zbytočne nenaťahovali.",
                "Ak máte doma zviera, posteľná sukňa môže fungovať ako tichý zachytávač chlpov. Zaradte ju do rutiny spolu s prehozom a dekami, nie iba s obliečkami.",
            ]),
            ("Ako prať sezónne textílie zo spálne", [
                "Sezónne textílie, ktoré vyberáte na zimu alebo sviatky, pred použitím vyvetrajte. Ak cítiť zatuchnutie, vyperte ich podľa štítku a nechajte úplne vyschnúť. Vôňa sama o sebe skladový pach nevyrieši.",
                "Pred uložením ich perte alebo aspoň očistite od prachu. Skladujte ich suché, nie natlačené v plastovom vreci hneď po sušení.",
            ]),
            ("Posteľná sukňa, alergia a prach v spálni", [
                "Pri alergii môže byť textil pri podlahe problém, lebo zachytáva prach v zóne, kde sa ľahko víri. Ak je alergia výrazná, zvážte, či dekoratívna posteľná sukňa stojí za zvýšenú údržbu.",
                "Ak si ju necháte, perte ju pravidelne, vysávajte okolie postele a znižujte vlhkosť v spálni. Roztoče a prach sa neriešia jedným produktom, ale režimom.",
            ]),
            ("Ako textílie jemne prevoňať po vypraní", [
                "Pri textíliách okolo postele môže byť vôňa príjemná, pretože nie sú vždy v priamom kontakte s pokožkou. Aj tu však platí, že vôňa patrí na čistý a suchý textil. Ak látka cítiť skladom alebo vlhkosťou, najprv ju vyperte a vyvetrajte.",
                "Parfumy do prania používajte skôr jemne, aby spálňa nepôsobila ťažko. Pri posteľných textíliách je lepší čistý a pokojný dojem než výrazná parfumácia.",
            ]),
        ],
        "caution": [
            "Pri textíliách so suchým zipsom, háčikmi alebo ozdobami chráňte ostatnú bielizeň. Jeden nezapnutý háčik môže poškodiť obliečku alebo jemnú plachtu.",
            "Ak je v spálni alergik, zvážte množstvo dekoratívnych textílií. Každá látka navyše znamená viac povrchov, na ktorých sa drží prach.",
        ],
        "expert": [
            "American Lung Association uvádza, že roztoče a ich alergény sa viažu na prach a textílie ako posteľná bielizeň, matrace, čalúnený nábytok, koberce a závesy. Posteľná sukňa alebo textílie okolo postele do tejto logiky zapadajú, najmä keď sú blízko podlahy.",
            "EPA pri vnútornom vzduchu odporúča udržiavať priestory čisté a suché a kontrolovať vlhkosť. V praxi to znamená, že dekoratívne textílie v spálni nemajú byť iba pekné, ale aj udržiavateľné.",
            "Najlepší kompromis je nezahltiť posteľ textíliami, ktoré neviete pravidelne čistiť. To, čo ostáva v spálni celoročne, musí mať jednoduchú rutinu prania, vysávania alebo vetrania.",
        ],
        "faq_title": "pranie posteľnej sukne a textílií okolo postele",
        "faq": [
            ("Ako často prať posteľnú sukňu?", "Závisí od prachu, zvierat a alergií. Pri bežnej domácnosti stačí sezónne alebo podľa znečistenia, pri zvieratách častejšie."),
            ("Môžem prať posteľnú sukňu s obliečkami?", "Áno, ak nemá háčiky, suchý zips alebo ťažké dekoračné časti. Inak ju perte oddelene alebo v ochrannom vaku."),
            ("Ako odstrániť prach z textílií okolo postele?", "Medzi praniami pomôže vysávanie s jemným nadstavcom, vytriasanie a utieranie okolia postele."),
            ("Prečo posteľná sukňa zapácha po skladovaní?", "Pravdepodobne bola uložená s prachom alebo zvyškovou vlhkosťou. Vyvetrajte ju, prípadne vyperte a skladujte až úplne suchú."),
        ],
    },
]


def render_article(article):
    parts = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['answer']}</p>",
        callout("Rýchly postup bez pokazenia textilu", article["quick"], background="#f7fbff", border="#d7e2ec"),
    ]
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["intro"])
    parts.append(f"<h2>{esc(article['why_heading'])}</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["why"])
    parts.append("<h2>Rýchle rozhodnutie podľa typu textilu</h2>")
    parts.append(table(["Typ alebo situácia", "Riziko", "Odporúčaný postup"], article["decision_rows"]))
    parts.append("<h2>Postup krok za krokom</h2>")
    parts.append("<ol>" + "".join(f"<li>{step}</li>" for step in article["steps"]) + "</ol>")
    parts.append("<h2>Najčastejšie príčiny problémov</h2>")
    parts.append(table(["Problém", "Pravdepodobná príčina", "Čo skúsiť"], article["check_rows"]))
    parts.append("<h2>Chyby, ktoré sa pri praní oplatí vynechať</h2>")
    parts.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    for heading, paragraphs in article["sections"]:
        if isinstance(paragraphs, str):
            raise TypeError(f"Section paragraphs for {article['title']} must be a list, not a string")
        parts.append(f"<h2>{esc(heading)}</h2>")
        for paragraph in paragraphs:
            parts.append(f"<p>{paragraph}</p>")
    parts.append(callout("Zapamätajte si", [
        "Posteľný textil nevracajte do postele ani do skrine, kým nie je úplne suchý.",
        "Silnejšia vôňa nenahradí pranie, oplach, vetranie ani správne sušenie.",
        "Pri každom textile rozhoduje štítok výrobcu a kapacita vašej práčky.",
    ], background="#fffaf5", border="#e6ded2"))
    parts.append("<h2>Kedy byť opatrný</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["caution"])
    parts.append("<h2>Odbornejší pohľad: vlhkosť, prach a roztoče</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    parts.append(source_box())
    parts.append(product_and_category_blocks(article))
    parts.append(related_links(COMMON_RELATED))
    parts.append(faq(article["faq"], article["faq_title"]))
    return "\n".join(parts)


def hrefs(markup):
    return re.findall(r'href="([^"]+)"', markup)


def preflight_links(articles):
    rows = []
    seen = set()
    for article in articles:
        target_url = f"{BASE}/n/{article['link']}"
        try:
            response = requests.get(target_url, timeout=25, allow_redirects=True)
            status = response.status_code
            ok = status == 404
            error = None
        except Exception as exc:  # pragma: no cover
            status = None
            ok = False
            error = str(exc)
        rows.append({"url": target_url, "kind": "target_slug_precheck", "ok": ok, "status": status, "error": error})
        for href in hrefs(article["long"]):
            url = urljoin(BASE, href)
            if url in seen:
                continue
            seen.add(url)
            status = None
            error = None
            try:
                response = requests.get(url, timeout=25, allow_redirects=True)
                status = response.status_code
                ok = 200 <= status < 400
            except Exception as exc:  # pragma: no cover
                ok = False
                error = str(exc)
            rows.append({"url": url, "kind": "article_link", "ok": ok, "status": status, "error": error})
    return {
        "checked_count": len(rows),
        "failure_count": sum(1 for row in rows if not row["ok"]),
        "links": rows,
    }


def main():
    rendered = []
    for index, article in enumerate(ARTICLES):
        long = render_article(article)
        for field in ("title", "short"):
            hits = FORBIDDEN_PUBLIC_RE.findall(article[field])
            if hits:
                raise SystemExit(f"Forbidden public wording in {article['title']}: {hits}")
        hits = FORBIDDEN_PUBLIC_RE.findall(long)
        if hits:
            raise SystemExit(f"Forbidden public wording in {article['title']}: {hits}")
        rendered.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": long,
                "date_posted": TODAY,
                "time_posted": f"09:{index * 12:02d}",
                "active": True,
                "link": slugify(article["title"]),
                "commenting": False,
            }
        )

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(rendered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = preflight_links(rendered)
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"article_count": len(rendered), "output": str(OUT_JSON), **report}, ensure_ascii=False, indent=2))
    if report["failure_count"]:
        raise SystemExit("Link preflight failed")


if __name__ == "__main__":
    main()
