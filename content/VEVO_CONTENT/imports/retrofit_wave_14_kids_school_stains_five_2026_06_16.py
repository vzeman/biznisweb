import argparse
import json
import re
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = Path.home() / ".codex" / "config.toml"
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-14-kids-school-stains-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-14-kids-school-stains-five-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-odstranit-ceresne-z-detskeho-tricka-a-letnych-siat",
        "post_id": "2159",
        "url": "https://www.vevo.sk/n/ako-odstranit-ceresne-z-detskeho-tricka-a-letnych-siat",
        "topic": "cherries",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-odstranit-vitaminovy-sirup-z-detskeho-body-a-podbradnika",
        "post_id": "2197",
        "url": "https://www.vevo.sk/n/ako-odstranit-vitaminovy-sirup-z-detskeho-body-a-podbradnika",
        "topic": "vitamin_syrup",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-sliz-z-detskeho-tricka-a-deky-bez-lepkavych-zvyskov",
        "post_id": "2170",
        "url": "https://www.vevo.sk/n/ako-odstranit-sliz-z-detskeho-tricka-a-deky-bez-lepkavych-zvyskov",
        "topic": "slime",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-zvyraznovac-z-rukava-mikiny-a-skolskeho-tricka",
        "post_id": "2172",
        "url": "https://www.vevo.sk/n/ako-odstranit-zvyraznovac-z-rukava-mikiny-a-skolskeho-tricka",
        "topic": "highlighter",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-vodove-farby-z-detskej-zastery-a-rukavov-mikiny",
        "post_id": "2168",
        "url": "https://www.vevo.sk/n/ako-odstranit-vodove-farby-z-detskej-zastery-a-rukavov-mikiny",
        "topic": "watercolor",
    },
]


def clean(markup):
    return textwrap.dedent(markup).strip()


def table(headers, rows):
    header_html = "".join(
        f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{header}</th>'
        for header in headers
    )
    body_html = "\n".join(
        "<tr>"
        + "".join(f'<td style="border: 1px solid #e5e5e5; padding: 10px;">{cell}</td>' for cell in row)
        + "</tr>"
        for row in rows
    )
    return (
        '<table style="width: 100%; border-collapse: collapse; margin: 20px 0;">\n'
        f"<thead><tr>{header_html}</tr></thead>\n<tbody>\n{body_html}\n</tbody>\n</table>"
    )


def note_card(title, bullets):
    items = "".join(f"<li>{item}</li>" for item in bullets)
    return clean(
        f"""
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">{title}</h2>
        <ul>{items}</ul>
        </div>
        """
    )


def recommendation_card(config):
    return clean(
        f"""
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie pre šetrné predpranie</h2>
        <p>{config["recommendation_intro"]}</p>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; margin: 14px 0;">
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>{config["product_text"]}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
        </div>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Kategória pracie gély</h3>
        <p>Ak riešite detské oblečenie, školské škvrny alebo farebné textílie pravidelne, oplatí sa mať doma šetrný prací gél a dávkovať ho podľa veľkosti dávky a znečistenia.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        </div>
        </div>
        """
    )


TOPICS = {
    "cherries": {
        "marker": "Detailnejší postup na čerešne, ovocné farbivo a detské letné oblečenie",
        "problem": "čerešňová šťava je vodnatá ovocná škvrna s výrazným červeným až fialovým pigmentom",
        "main_textile": "detské tričko a letné šaty",
        "avoid": "horúcu sušičku, žehlenie a silné trenie pred opláchnutím",
        "intro": [
            "Čerešne vedia na bielom alebo svetlom detskom tričku zanechať ružovú až fialovú mapu. Pri letných šatách je problém ešte citlivejší, pretože bývajú tenšie, farebné alebo zo zmesi bavlny, viskózy a elastanu. Najdôležitejšie je konať rýchlo, ale bez paniky: najprv dostať von šťavu a až potom prať.",
            "Pri ovocných škvrnách ľudia často siahnu po horúcej vode alebo hneď po silnom praní. To nemusí byť najlepší prvý krok. Čerstvá čerešňová šťava sa správa inak než zaschnutá mapa po pikniku a inak než škvrna, ktorá už prešla práčkou.",
        ],
        "diagnosis": [
            "<strong>Čerstvá škvrna:</strong> opláchnite studenšou vodou z rubovej strany, aby sa šťava nevťahovala hlbšie.",
            "<strong>Zaschnutá škvrna:</strong> najprv ju zvlhčite a nechajte uvoľniť, netrite ju nasucho.",
            "<strong>Letné šaty:</strong> skontrolujte štítok, zmes a farbostálosť, až potom predčistite.",
            "<strong>Po praní:</strong> pred sušením skontrolujte ružový tieň pri dennom svetle.",
        ],
        "state_rows": [
            ("čerstvá šťava", "opláchnuť z rubu, predčistiť jemne", "nenechať zaschnúť na slnku"),
            ("zaschnutá mapa", "zvlhčiť a postup opakovať", "nešúchať nasucho"),
            ("škvrna po praní", "nesušiť horúco, predčistiť znova", "teplo zvyšok fixuje"),
            ("jemné šaty", "testovať na skrytom mieste", "farba a tvar môžu byť citlivé"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "zvyčajne znesie dôkladnejšie predpranie", "pozor na potlač a farebné lemy"),
            ("letné šaty", "šetrný postup podľa štítku", "materiál môže byť tenký alebo viskózový"),
            ("detská mikina", "predčistiť lokálne a prať naruby", "hrubší úplet drží pigment dlhšie"),
            ("obrus alebo servítka", "pracovať od okrajov ku stredu", "škvrna sa ľahko rozšíri"),
        ],
        "sections": [
            ("Ako odstrániť čerešne z detského trička", "Tričko otočte naruby a škvrnu oplachujte z rubovej strany. Cieľ je vytlačiť šťavu von z vlákna, nie ju zatlačiť hlbšie do látky. Potom naneste malé množstvo pracieho gélu na zafarbené miesto, jemne zapracujte prstami a nechajte krátko pôsobiť podľa citlivosti materiálu.", "Ak má tričko potlač, netrite ju kefkou. Po praní skontrolujte škvrnu ešte pred sušičkou alebo sušením na prudkom slnku. Pri podobných ovocných pigmentoch nadväzuje návod <a href=\"/n/ako-vyprat-granatove-jablko-z-oblecenia-bez-ruzovych-map\">ako vyprať granátové jablko</a>."),
            ("Čerešne na letných šatách", "Letné šaty bývajú jemnejšie než bežné tričko. Ak sú z viskózy, ľanu alebo zmesi s elastanom, riešte nielen škvrnu, ale aj tvar. Škvrnu zvlhčite, predčistite lokálne a šaty perte v šetrnejšom režime bez preplnenia bubna.", "Šaty po praní nenechávajte zrolované v práčke. Vyberte ich, upravte švy a sušte tak, aby sa nevytiahli. Ak je škvrna na veľmi viditeľnom mieste, je lepšie postup zopakovať jemne než poškodiť materiál jedným agresívnym čistením."),
            ("Čo ak čerešňová škvrna ostala po praní", "Ak po praní zostal ružový tieň, nepovažujte pranie za hotové. Teplo zo sušičky alebo žehličky môže zvyšok pigmentu stabilizovať a ďalšie čistenie bude náročnejšie. Miesto znovu navlhčite, lokálne predčistite a perte podľa štítku.", "Pri bielych bavlnených kusoch máte viac možností než pri farebných letných šatách. Pri farebnom textile najprv overte stálosť farby na nenápadnom mieste."),
            ("Prečo čerešne zanechávajú ružové mapy", "Ovocná šťava obsahuje vodu, cukry, organické kyseliny a farebné látky. Vodnatá časť sa rýchlo vpije do látky a pigment zostane viditeľný aj vtedy, keď povrch už vyzerá suchý. Preto je dôležité škvrnu riešiť ešte pred hlavným praním.", "Podobný princíp platí aj pri iných farebných potravinách. Pri čokoláde je navyše tuk, preto sa postup líši; pomôže článok <a href=\"/n/ako-vyprat-cokoladu-z-detskeho-oblecenia-a-obrusov\">ako vyprať čokoládu z detského oblečenia</a>."),
            ("Ako upraviť rutinu pri detskom letnom oblečení", "Detské letné veci perte skôr v menších dávkach, aby sa škvrny dobre opláchli a látka nebola stlačená v bubne. Pri ovocí pomáha mať škvrnu pred praním vyriešenú lokálne. Samotný dlhý program bez predčistenia nemusí stačiť.", "Ak sa škvrny opakujú často, držte pri praní jednoduchý systém: opláchnuť, predčistiť, prať podľa štítku, skontrolovať pred sušením. Tento postup znižuje riziko trvalých máp."),
        ],
        "depth": [
            ("Čerstvé čerešne z ihriska alebo pikniku", "Pri škvrne z výletu často nemáte práčku po ruke. Pomôže aspoň čistá voda a obrúsok, ale bez agresívneho drhnutia. Ak je možné, prepláchnite škvrnu z rubu a nechajte textil voľne preschnúť, nie prilepený v igelitke.", "Doma potom pokračujte predčistením. Ak škvrna preschla na slnku, môže potrebovať viac času a druhé pranie."),
            ("Kontrola pri svetlom a farebnom textile", "Na bielom tričku je ružový tieň viditeľný hneď, na vzorovaných šatách sa môže ukázať až po vyschnutí. Preto kontrolujte látku pri dennom svetle a z oboch strán.", "Pri farebných kusoch sa vyhnite univerzálnym silným postupom bez testu. Cieľ je odstrániť pigment bez toho, aby ste zosvetlili pôvodnú farbu textilu."),
        ],
        "faq": [
            ("Môžem čerešňovú škvrnu hneď vyprať bez predčistenia?", "Pri čerstvej malej škvrne to niekedy vyjde, ale bezpečnejšie je najprv oplach z rubu a lokálne predčistenie."),
            ("Prečo ostala ružová mapa po praní?", "Pigment sa úplne neuvoľnil pred hlavným praním alebo sa škvrna sušila teplom."),
            ("Ako prať letné šaty po čerešniach?", "Podľa štítku, šetrne, bez krútenia a s kontrolou škvrny pred sušením."),
        ],
        "recommendation_intro": "Pri ovocných škvrnách je produkt užitočný hlavne na lokálne predčistenie po oplachu. Najprv odstráňte šťavu a pigment, až potom riešte bežné pranie celej dávky.",
        "product_text": "Hodí sa ako šetrný základ pri predpraní a následnom praní detských tričiek, obrusov a bežnej bielizne. Pri jemných šatách vždy rozhoduje štítok.",
        "links": [
            ("/n/ako-vyprat-granatove-jablko-z-oblecenia-bez-ruzovych-map", "Ako vyprať granátové jablko z oblečenia bez ružových máp"),
            ("/n/ako-vyprat-cokoladu-z-detskeho-oblecenia-a-obrusov", "Ako vyprať čokoládu z detského oblečenia a obrusov"),
            ("/n/casto-kladene-otazky-skvrny-na-obleceni-po-prani", "Škvrny na oblečení po praní"),
        ],
    },
    "vitamin_syrup": {
        "marker": "Detailnejší postup na vitamínový sirup, lepkavý zvyšok a detské body",
        "problem": "vitamínový sirup spája cukor, farbivo a lepivú vrstvu, ktorá sa vie po praní tváriť ako tvrdé miesto",
        "main_textile": "detské body, podbradník a pyžamo",
        "avoid": "sušenie bez kontroly, pretože cukrový zvyšok môže stvrdnúť",
        "intro": [
            "Vitamínový sirup je typická detská škvrna: kvapka na body, farebný fľak na podbradníku alebo lepkavý pás pri golieri pyžama. Na prvý pohľad vyzerá ako bežná sladká škvrna, ale často obsahuje aj výrazné farbivo a hustý cukrový základ.",
            "Najlepšie funguje postup, ktorý najprv rozpustí lepkavú časť a až potom rieši farbu. Ak sa sirup hneď vysuší alebo prejde horúcim cyklom bez oplachu, môže zostať tuhý okraj, zafarbená mapa alebo nepríjemný sladký pach.",
        ],
        "diagnosis": [
            "<strong>Lepkavé miesto:</strong> pred praním ho najprv zvlhčite a uvoľnite, nestačí iba prevoňať dávku.",
            "<strong>Farebný sirup:</strong> pracujte z rubovej strany a sledujte pigment.",
            "<strong>Detské body:</strong> oplach je dôležitý, aby pri pokožke nezostali zvyšky produktu.",
            "<strong>Podbradník:</strong> skontrolujte okraje a švy, sirup sa drží aj v lemoch.",
        ],
        "state_rows": [
            ("čerstvá kvapka", "odsajte prebytok a opláchnite", "netrieť do väčšej mapy"),
            ("lepkavý zaschnutý sirup", "zvlhčiť a nechať uvoľniť", "cukor potrebuje čas"),
            ("farebný tieň", "predčistiť lokálne", "kontrola pred sušením"),
            ("sirup v leme", "prepracovať okraj prstami", "lem drží zvyšky dlhšie"),
        ],
        "textile_rows": [
            ("detské body", "jemne predčistiť a dobre opláchnuť", "textil je priamo pri pokožke"),
            ("podbradník", "riešiť prednú vrstvu aj lem", "zvyšky jedla sa držia v okrajoch"),
            ("pyžamo", "prať s detskou bielizňou bez preplnenia", "dlhý kontakt s pokožkou"),
            ("farebná mikina", "test farby a nižšia mechanika", "pigment sirupu sa môže rozpiť"),
        ],
        "sections": [
            ("Ako odstrániť vitamínový sirup z detského body", "Body najprv prepláchnite vlažnou až chladnejšou vodou podľa štítku a škvrnu uvoľňujte z rubu. Ak je miesto lepkavé, nechajte ho krátko zvlhčené, aby cukrová vrstva povolila. Až potom pridajte malé množstvo pracieho gélu na lokálne predčistenie.", "Pri detskom body je dôležité dôkladné opláchnutie. Textil je pri pokožke, preto nepreháňajte množstvo prípravku a nekompenzujte zvyšok sirupu silnou vôňou."),
            ("Ako odstrániť sirup z podbradníka", "Podbradník býva znečistený opakovane a sirup sa často drží v leme, pri zapínaní alebo v prešívaní. Pred praním skontrolujte okraje, zvlhčite ich a jemne prepracujte prstami. Ak je podbradník nepremokavý alebo vrstvený, rešpektujte štítok.", "Pri látkovom podbradníku nečakajte, že hlavné pranie vyrieši zaschnutý cukor v leme samo. Pomôže krátke predčistenie a menšia dávka bielizne v bubne."),
            ("Čo ak sirup zanechal farebný fľak", "Niektoré sirupy farbia silnejšie než samotná lepkavá vrstva. Ak po oplachu zostane oranžový, červený alebo hnedý tieň, riešte ho ešte pred sušením. Teplo môže farebný zvyšok zhoršiť.", "Pri podobných detských škvrnách pomôže aj prehľad <a href=\"/n/casto-kladene-otazky-skvrny-na-obleceni-po-prani\">škvrny na oblečení po praní</a>, najmä keď sa fľak ukáže až po cykle."),
            ("Prečo sirup tvrdne po praní", "Ak v látke zostane cukrový zvyšok, po vysušení môže pôsobiť ako tvrdé alebo lepkavé miesto. Nie je to chyba vône ani aviváže. Najčastejšie chýbal oplach, predčistenie alebo bol bubon príliš plný.", "Riešenie je miesto znovu zvlhčiť, uvoľniť zvyšok a prať s lepším oplachom. Pri detských veciach je lepší presný postup než pridávanie väčšieho množstva produktu."),
            ("Ako nastaviť pranie detských vecí po sirupe", "Detské body, podbradníky a pyžamá perte tak, aby sa voda dostala ku každému kusu. Nepreplňte bubon, dávkujte primerane a po praní skontrolujte lemy. Ak sa škvrna opakuje, predčistenie zaraďte ako rutinu, nie ako výnimočný krok.", "Pri mastných detských škvrnách sa postup mení. Napríklad arašidové maslo vyžaduje najprv odobrať tukovú pastu; nadväzuje článok <a href=\"/n/ako-odstranit-arasidove-maslo-z-tricka-obrusu-a-detskej-mikiny\">ako odstrániť arašidové maslo</a>."),
        ],
        "depth": [
            ("Sirup na body počas noci", "Ak sirup kvapne na pyžamo alebo body večer, nenechávajte ho do rána úplne zaschnúť, ak sa tomu dá vyhnúť. Stačí rýchly oplach a voľné preschnutie do prania.", "Tým zabránite tomu, aby sa cukor a pigment pevnejšie naviazali na látku alebo lem."),
            ("Citlivá pokožka a zvyšky produktu", "Pri detských veciach je čistota dôležitejšia než výrazná vôňa. Ak v tkanine zostane prací film alebo sirupový zvyšok, môže textil pôsobiť drsnejšie.", "Dobrý oplach a primerané dávkovanie sú preto súčasťou riešenia škvrny, nie iba komfortný detail."),
        ],
        "faq": [
            ("Prečo je body po praní stále lepkavé?", "V látke pravdepodobne zostal cukrový zvyšok. Miesto zvlhčite, jemne uvoľnite a perte znova s dobrým oplachom."),
            ("Môžem sirup z detského oblečenia prať horúco?", "Len ak to povoľuje štítok. Najprv je bezpečnejšie uvoľniť sirup a pigment lokálne."),
            ("Ako riešiť sirup na podbradníku?", "Skontrolujte lemy, zvlhčite zaschnuté miesta a pred praním ich jemne prepracujte."),
        ],
        "recommendation_intro": "Pri sirupe je cieľom rozpustiť lepkavú časť, vypláchnuť pigment a potom vyprať textil bez zvyškov pri pokožke.",
        "product_text": "Vhodný ako jemný základ na detské body, pyžamá a bežnú bielizeň po lokálnom predčistení. Dávkujte striedmo a sledujte dôkladný oplach.",
        "links": [
            ("/n/ako-odstranit-arasidove-maslo-z-tricka-obrusu-a-detskej-mikiny", "Ako odstrániť arašidové maslo z trička, obrusu a detskej mikiny"),
            ("/n/ako-vyprat-cokoladu-z-detskeho-oblecenia-a-obrusov", "Ako vyprať čokoládu z detského oblečenia a obrusov"),
            ("/n/casto-kladene-otazky-skvrny-na-obleceni-po-prani", "Škvrny na oblečení po praní"),
        ],
    },
    "slime": {
        "marker": "Detailnejší postup na sliz, lepkavý polymér a textil po hraní",
        "problem": "sliz je lepkavá hmota, ktorá sa môže natiahnuť do vlákien a po zaschnutí stvrdnúť",
        "main_textile": "detské tričko, deka a tepláky",
        "avoid": "horúcu vodu ako prvý krok a násilné trhanie z vlákien",
        "intro": [
            "Sliz na tričku alebo deke nie je obyčajná škvrna. Najprv ide o hmotu prilepenú na povrchu, až potom o fľak. Ak ju začnete hneď prať bez mechanického odstránenia, môže sa rozotrieť do väčšej plochy alebo zostať v úplete ako lepkavý zvyšok.",
            "Pri slize je dôležité rozlíšiť čerstvú mäkkú hmotu, zaschnutý tvrdší zvyšok a farbivo, ktoré zostane po odstránení objemu. Každá fáza potrebuje trochu iný postup.",
        ],
        "diagnosis": [
            "<strong>Čerstvý sliz:</strong> najprv odoberte čo najviac hmoty tupou hranou.",
            "<strong>Zaschnutý sliz:</strong> neuvoľňujte ho násilím, aby ste nevytrhali vlákna.",
            "<strong>Deka a úplet:</strong> hmota sa drží medzi vláknami, preto treba trpezlivosť.",
            "<strong>Farebný sliz:</strong> po odstránení hmoty skontrolujte zafarbený tieň.",
        ],
        "state_rows": [
            ("mäkký sliz", "odobrať objem a chladiť podľa potreby", "nevtláčať do látky"),
            ("zaschnutý zvyšok", "uvoľňovať postupne", "pozor na vytrhnutie vlákien"),
            ("farebná mapa", "riešiť až po odstránení hmoty", "farbivo je druhý problém"),
            ("sliz v deke", "čistiť po malých častiach", "vlákna držia zvyšky dlhšie"),
        ],
        "textile_rows": [
            ("detské tričko", "odobrať sliz, predčistiť a prať naruby", "potlač netrieť kefkou"),
            ("deka", "pracovať po úsekoch a dobre opláchnuť", "hrubší vlas drží zvyšky"),
            ("tepláky", "skontrolovať kolená a švy", "úplet môže byť elastický"),
            ("koberec alebo poťah", "nepremáčať bez kontroly výplne", "nie je to bežné pranie"),
        ],
        "sections": [
            ("Ako odstrániť sliz z detského trička", "Najprv odoberte hmotu tupou hranou alebo prstami, ale nešúchajte ju do strán. Ak sliz drží, pomôže postupné uvoľňovanie po malých častiach. Až keď je objem preč, riešte farebný alebo lepkavý zvyšok.", "Tričko perte naruby a s podobnými farbami. Pri potlači netrite agresívne. Ak sa v látke drží farbivo, postupujte podobne ako pri iných detských pigmentoch a kontrolujte pred sušením."),
            ("Ako dostať sliz z deky", "Deka má hrubší povrch a sliz sa vie dostať medzi vlákna. Nepokúšajte sa ho vytrhnúť jedným ťahom. Najprv odstráňte viditeľnú hmotu, potom miesto postupne uvoľňujte a oplachujte. Pri väčšej deke sledujte, či ju práčka zvládne dôkladne opláchnuť.", "Ak je deka jemná alebo má výplň, riaďte sa štítkom. Pri podobných detských hmotách pomôže aj článok <a href=\"/n/ako-odstranit-plastelinu-z-teplakov-koberca-a-potahu\">ako odstrániť plastelínu z teplákov, koberca a poťahu</a>."),
            ("Lepkavý zvyšok po slize", "Keď objem zmizne, môže zostať lepkavý film. Ten sa často ukáže až pri dotyku alebo po čiastočnom vyschnutí. Miesto znovu zvlhčite, jemne predčistite a perte s dostatočným oplachom.", "Ak ostal iba farebný tieň, riešte ho ako pigmentovú škvrnu. Ak je miesto stále gumové, ešte nie je odstránený hlavný zvyšok slizu."),
            ("Prečo sliz v práčke nie je dobrý nápad", "Ak do práčky vložíte kus s veľkým množstvom slizu, hmota sa môže rozotrieť na ďalšie veci alebo ostať v záhyboch textilu. Práčka má dokončiť čistenie, nie nahradiť prvé mechanické odstránenie.", "Podobne pri lepidle je potrebné najprv zhodnotiť typ škvrny; nadväzuje návod <a href=\"/n/ako-odstranit-lepidlo-z-oblecenia-po-tvoreni-s-detmi\">ako odstrániť lepidlo z oblečenia po tvorení s deťmi</a>."),
            ("Ako nastaviť pranie po tvorení s deťmi", "Veci od slizu, plastelíny, farieb a lepidla netrieďte len podľa farby. Najprv ich rozdeľte podľa typu zvyškov. Kusy s hmotou predčistite zvlášť, pigmentové škvrny riešte lokálne a až potom skladajte praciu dávku.", "Takýto postup znižuje riziko, že jeden kus zanesie zvyšky na celú dávku bielizne."),
        ],
        "depth": [
            ("Sliz a potlačené tričká", "Pri potlači je problém dvojitý: chcete odstrániť sliz, ale nechcete poškodiť obrázok. Preto nepoužívajte tvrdú kefku na potlačenú časť a neťahajte sliz cez okraj potlače.", "Ak je sliz priamo na potlači, postupujte pomalšie a radšej viackrát jemne než raz agresívne."),
            ("Deka po hraní v detskej izbe", "Deku po odstránení slizu pretrepte a skontrolujte aj okolité miesta. Malé kúsky hmoty sa môžu prilepiť inde a v práčke sa znovu objaviť.", "Pri praní deky nepoužívajte príliš veľkú dávku. Voda a oplach musia mať priestor, inak lepkavé zvyšky zostanú vo vlákne."),
        ],
        "faq": [
            ("Mám sliz hneď namočiť do horúcej vody?", "Nie ako prvý krok. Najprv odstráňte čo najviac hmoty a postupujte podľa materiálu."),
            ("Čo ak sliz po praní stále lepí?", "Zostala časť filmu. Miesto znovu zvlhčite, predčistite a perte s dobrým oplachom."),
            ("Môžem sliz z deky odstrániť v práčke?", "Až po mechanickom odstránení hlavnej hmoty. Inak riskujete roznesenie zvyškov."),
        ],
        "recommendation_intro": "Pri slize má prací produkt zmysel až po odstránení hlavnej hmoty. Najprv dostaňte z textilu lepkavý objem, potom riešte zvyšok a pranie.",
        "product_text": "Vhodný na následné pranie po mechanickom odstránení slizu a lokálnom predčistení. Pri dekách a detských veciach sledujte dostatočný oplach.",
        "links": [
            ("/n/ako-odstranit-plastelinu-z-teplakov-koberca-a-potahu", "Ako odstrániť plastelínu z teplákov, koberca a poťahu"),
            ("/n/ako-odstranit-lepidlo-z-oblecenia-po-tvoreni-s-detmi", "Ako odstrániť lepidlo z oblečenia po tvorení s deťmi"),
            ("/n/ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu", "Ako vyprať voskovky z peračníka a textilného obalu"),
        ],
    },
    "highlighter": {
        "marker": "Detailnejší postup na zvýrazňovač, školský pigment a rukáv mikiny",
        "problem": "zvýrazňovač je farebná škvrna z atramentu, ktorá sa vie rozpiť do väčšej plochy",
        "main_textile": "rukáv mikiny, školské tričko a textilný peračník",
        "avoid": "silné trenie a horúce sušenie pred kontrolou pigmentu",
        "intro": [
            "Zvýrazňovač na rukáve mikiny alebo školskom tričku vzniká rýchlo: dieťa sa oprie o zošit, prejde fixkou po rukáve alebo si zafarbí lem pri písaní. Škvrna býva jasná, neónová a na svetlom textile veľmi viditeľná.",
            "Pri zvýrazňovači je cieľom nerozpiť atrament do väčšej mapy. Najprv treba znížiť množstvo farbiva v mieste škvrny, pracovať savým podkladom a až potom prať. Ak začnete silným trením, pigment sa môže dostať hlbšie alebo ďalej od pôvodného miesta.",
        ],
        "diagnosis": [
            "<strong>Čerstvá čiara:</strong> podložte savou handričkou a nešúchajte do strán.",
            "<strong>Rukáv mikiny:</strong> farbivo môže byť aj v rebrovanom leme.",
            "<strong>Školské tričko:</strong> pri potlači postupujte bez tvrdej kefky.",
            "<strong>Po praní:</strong> neónový tieň kontrolujte pred sušičkou.",
        ],
        "state_rows": [
            ("tenká čiara", "odsávať a predčistiť lokálne", "nerozotierať"),
            ("väčšia mapa", "pracovať od okraja ku stredu", "podložiť savou vrstvou"),
            ("lem rukáva", "prepracovať záhyb jemne", "pigment sedí v rebrovaní"),
            ("škvrna po praní", "zopakovať predčistenie", "nesušiť horúco"),
        ],
        "textile_rows": [
            ("mikina", "riešiť rukáv a lem zvlášť", "hrubší úplet drží atrament"),
            ("školské tričko", "pracovať cez savý podklad", "tenká bavlna sa ľahko prepije"),
            ("peračník", "nepremáčať výplň bez kontroly", "môže mať vrstvy a výstuž"),
            ("biela košeľa", "testovať a kontrolovať tieň", "škvrna je veľmi viditeľná"),
        ],
        "sections": [
            ("Ako odstrániť zvýrazňovač z rukáva mikiny", "Rukáv podložte čistou savou handričkou. Škvrnu netrite do strán, ale pracujte jemne tak, aby sa farbivo presúvalo do podkladu. Pri rebrovanom leme postupujte po malých úsekoch, pretože pigment sa drží v záhyboch.", "Po predčistení perte mikinu naruby a bez preplnenia bubna. Ak ostane neónový tieň, nepoužite sušičku a postup zopakujte."),
            ("Ako odstrániť zvýrazňovač zo školského trička", "Tenké tričko sa ľahko prepije na druhú stranu. Preto podložte škvrnu, pracujte z rubu aj líca podľa toho, kde je pigment silnejší, a priebežne posúvajte čistú časť handričky. Cieľ je farbivo vytiahnuť, nie rozotrieť.", "Ak má tričko potlač, vyhnite sa tvrdému treniu. Pri školských škvrnách od voskovky alebo farieb nadväzuje článok <a href=\"/n/ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu\">ako vyprať voskovky</a>."),
            ("Čo ak zvýrazňovač ostal po praní", "Zvýrazňovač môže po praní zoslabnúť, ale zostať ako svetlý farebný závoj. Vtedy škvrnu neriešte žehlením ani sušičkou. Znovu ju navlhčite, podložte savou vrstvou a predčistite lokálne.", "Pri bielych kusoch je možné postup opakovať, pri farebných najprv testujte stálosť farby. Hlavne nemeňte celý kus agresívnym zásahom, ak ide o malú lokálnu škvrnu."),
            ("Prečo sa atrament rozpíja", "Atrament zo zvýrazňovača je navrhnutý tak, aby zanechal výraznú stopu na papieri. Na textile sa však môže rozptýliť vo vlákne, najmä ak pridáte veľa vody naraz alebo silno šúchate. Preto pomáha savý podklad a postupné odoberanie pigmentu.", "Podobnú logiku majú aj niektoré výtvarné škvrny. Pri farbách si pozrite <a href=\"/n/ako-odstranit-akrylovu-farbu-z-tricka-bez-zafixovania\">ako odstrániť akrylovú farbu z trička</a>."),
            ("Ako prať školské oblečenie so škvrnami", "Školské veci často obsahujú mix atramentu, lepidla, farieb a jedla. Pred praním preto skontrolujte rukávy, vrecká, lemy a spodok trička. Ak je v práčke viac škvrnitých kusov, nepreplňte bubon, aby sa pigmenty dobre opláchli.", "Pri školských škvrnách je dôležitá kontrola pred sušením. Mnohé fľaky vyzerajú po praní lepšie, ale ešte nie sú úplne preč."),
        ],
        "depth": [
            ("Zvýrazňovač na bielom a tmavom textile", "Na bielom textile vidíte každý zvyšok pigmentu, na tmavom môže zostať matná mapa alebo zmena povrchu. V oboch prípadoch je lepšie postupovať lokálne a sledovať výsledok na svetle.", "Tmavý textil netrite silno, aby ste nevytvorili svetlé vydraté miesto, ktoré bude nakoniec viditeľnejšie než pôvodná škvrna."),
            ("Školská prevencia pri peračníku a rukávoch", "Pravidelne kontrolujte peračník, či v ňom nie je otvorená fixka. Rukávy mikiny sa často zašpinia pri písaní, preto ich pred praním rýchlo prehliadnite.", "Rýchle predčistenie čerstvej čiary je jednoduchšie než riešiť zaschnutý pigment po celom týždni."),
        ],
        "faq": [
            ("Dá sa zvýrazňovač vyprať na prvýkrát?", "Pri čerstvej malej škvrne často áno, ale pomáha lokálne predčistenie a savý podklad."),
            ("Prečo sa škvrna zväčšila?", "Pigment sa rozpil vodou alebo trením. Nabudúce pracujte od okraja a podložte textil."),
            ("Môžem použiť sušičku po praní?", "Až keď pigment zmizne. Teplo môže zvyšok zafixovať."),
        ],
        "recommendation_intro": "Pri atramentových školských škvrnách je dôležité pracovať lokálne a neprehnať množstvo vody ani prípravku. Po predčistení má nasledovať dobre opláchnuté pranie.",
        "product_text": "Vhodný na následné pranie mikín, tričiek a školského oblečenia po lokálnom predčistení pigmentu. Pri farebných kusoch najprv overte stálosť farby.",
        "links": [
            ("/n/ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu", "Ako vyprať voskovky z peračníka a textilného obalu"),
            ("/n/ako-odstranit-akrylovu-farbu-z-tricka-bez-zafixovania", "Ako odstrániť akrylovú farbu z trička bez zafixovania"),
            ("/n/ako-odstranit-lepidlo-z-oblecenia-po-tvoreni-s-detmi", "Ako odstrániť lepidlo z oblečenia po tvorení s deťmi"),
        ],
    },
    "watercolor": {
        "marker": "Detailnejší postup na vodové farby, detskú zásteru a rukávy mikiny",
        "problem": "vodové farby kombinujú pigment, spojivo a vodu, takže čerstvá škvrna sa dá často riešiť lepšie než zaschnutá vrstva",
        "main_textile": "detská zástera, rukáv mikiny a školské tričko",
        "avoid": "sušenie a žehlenie pred kontrolou farebného tieňa",
        "intro": [
            "Vodové farby vyzerajú nevinne, ale na detskej zástere, rukáve mikiny alebo školskom tričku môžu po vyschnutí zanechať pigmentový tieň. Čerstvá škvrna sa zvyčajne rieši ľahšie, pretože farba ešte nie je pevne usadená vo vlákne.",
            "Postup závisí od toho, či ide o obyčajnú školskú vodovú farbu, intenzívny pigment, zmiešanú škvrnu s lepidlom alebo staršiu zaschnutú vrstvu. Najbezpečnejšie je najprv oplachovať z rubu a až potom predčistiť.",
        ],
        "diagnosis": [
            "<strong>Čerstvá farba:</strong> oplachujte z rubovej strany a nenechajte pigment rozliať.",
            "<strong>Zaschnutá farba:</strong> jemne uvoľnite povrch, netrite ho do látky.",
            "<strong>Zástera:</strong> skontrolujte vrecká, lemy a viac vrstiev látky.",
            "<strong>Rukáv mikiny:</strong> pigment sa drží v rebrovaní a švoch.",
        ],
        "state_rows": [
            ("mokrá škvrna", "oplach z rubu", "rýchla reakcia pomáha"),
            ("zaschnutá farba", "zvlhčiť a uvoľniť", "nešúchať nasucho"),
            ("zmes s lepidlom", "najprv riešiť vrstvu", "samotné pranie nestačí"),
            ("pigment po praní", "zopakovať lokálne", "nesušiť horúco"),
        ],
        "textile_rows": [
            ("detská zástera", "riešiť vrecká a lemy", "farba sa drží v švoch"),
            ("rukáv mikiny", "predčistiť rebrovanie", "hrubý úplet drží pigment"),
            ("školské tričko", "oplach z rubu a jemný program", "tenká látka sa prepije"),
            ("textilný obal", "nepremáčať výstuž bez kontroly", "môže mať viac vrstiev"),
        ],
        "sections": [
            ("Ako odstrániť vodové farby z detskej zástery", "Zásteru najprv prehliadnite celú. Farba býva nielen v strede, ale aj na vreckách, šnúrkach a lemoch. Čerstvé škvrny oplachujte z rubu, aby sa pigment netlačil hlbšie do tkaniny. Zaschnuté miesta najprv zvlhčite a až potom jemne predčistite.", "Ak je zástera z tenkej bavlny, postup bude jednoduchší než pri vrstvenej alebo nepremokavej zástere. Pri vrstvených kusoch nepremačajte výplň bez kontroly štítku."),
            ("Ako odstrániť vodové farby z rukávov mikiny", "Rukávy mikiny bývajú pri maľovaní najviac zasiahnuté. Pigment sa dostane do rebrovania, švu a spodnej časti rukáva. Pred praním tieto miesta navlhčite, jemne prepracujte a skontrolujte z oboch strán.", "Mikinu perte naruby a bez preplnenia bubna. Ak zostane farebný tieň, nesušte horúco. Pri podobných pigmentových škvrnách pomôže aj článok <a href=\"/n/ako-odstranit-akrylovu-farbu-z-tricka-bez-zafixovania\">ako odstrániť akrylovú farbu</a>."),
            ("Čo ak boli vodové farby zmiešané s lepidlom", "Pri tvorení deti často miešajú farby s lepidlom alebo inými hmotami. Vtedy nejde iba o pigment, ale aj o vrstvu, ktorá drží na povrchu. Najprv uvoľnite povrchový zvyšok, až potom riešte farebnú mapu.", "Ak je v škvrne lepidlo, postupujte opatrnejšie a nadviažte na návod <a href=\"/n/ako-odstranit-lepidlo-z-oblecenia-po-tvoreni-s-detmi\">ako odstrániť lepidlo z oblečenia</a>."),
            ("Prečo vodové farby niekedy nejdú dole ľahko", "Názov vodové farby zvádza k predstave, že stačí voda. V praxi záleží na pigmente, množstve farby, čase schnutia a materiáli. Ak farba zaschla v úplete alebo v šve, jednoduché pranie nemusí stačiť.", "Preto sa oplatí škvrnu skontrolovať pred hlavným praním a po praní ešte pred sušením. Práve teplo vie zvyšný pigment zhoršiť."),
            ("Ako prať oblečenie po výtvarnej výchove", "Po výtvarnej výchove skontrolujte vrecká, rukávy, spodný lem trička a zásteru. Oddelte kúsky s farbou, lepidlom alebo voskovkou a riešte ich zvlášť. Až keď sú najväčšie zvyšky preč, dajte ich do bežnej pracej dávky.", "Pri voskovkách je postup iný, pretože ide aj o voskovú zložku; pozrite si <a href=\"/n/ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu\">ako vyprať voskovky z peračníka</a>."),
        ],
        "depth": [
            ("Rýchle predčistenie v škole alebo doma", "Ak sa farba dostane na rukáv hneď počas tvorenia, pomôže aspoň jemné zotretie prebytku a oplach, ak je dostupný. Škvrnu netrite suchou servítkou tak, aby sa pigment rozmazal do väčšej plochy.", "Doma potom pokračujte podľa materiálu. Čím menej farby ostane pred hlavným praním, tým väčšia šanca na čistý výsledok."),
            ("Kontrola po praní pri farebných pigmentoch", "Po praní môže mokrý textil vyzerať čisto, ale po preschnutí sa objaví bledý farebný tieň. Preto pred sušičkou skontrolujte miesto na dennom svetle.", "Ak tieň zostal, zopakujte lokálne predčistenie. Pri detskej zástere je lepšie zachovať materiál než ju poškodiť príliš tvrdým zásahom."),
        ],
        "faq": [
            ("Idú vodové farby z oblečenia dole?", "Často áno, najmä keď sú čerstvé. Dôležitý je oplach z rubu a kontrola pred sušením."),
            ("Čo ak farba zaschla?", "Zvlhčite ju, jemne uvoľnite a predčistite lokálne. Netrite zaschnutú vrstvu nasucho."),
            ("Môžem zásteru prať s ostatnou bielizňou?", "Až keď odstránite najväčšie zvyšky farby. Inak môžete pigment preniesť na ďalšie kusy."),
        ],
        "recommendation_intro": "Pri vodových farbách je dôležité najprv vypláchnuť pigment a uvoľniť povrchové zvyšky. Prací gél potom pomáha pri následnom praní, nie pri zakrytí farby.",
        "product_text": "Vhodný na následné pranie detských záster, tričiek a mikín po lokálnom predčistení pigmentu. Pri intenzívnych farbách kontrolujte výsledok pred sušením.",
        "links": [
            ("/n/ako-odstranit-akrylovu-farbu-z-tricka-bez-zafixovania", "Ako odstrániť akrylovú farbu z trička bez zafixovania"),
            ("/n/ako-odstranit-lepidlo-z-oblecenia-po-tvoreni-s-detmi", "Ako odstrániť lepidlo z oblečenia po tvorení s deťmi"),
            ("/n/ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu", "Ako vyprať voskovky z peračníka a textilného obalu"),
        ],
    },
}


def build_related_links(links):
    items = "\n".join(f'<li><a href="{href}">{label}</a></li>' for href, label in links)
    items += '\n<li><a href="/n/ako-vybrat-praci-gel-podla-typu-bielizne">Ako vybrať prací gél podľa typu bielizne</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


def build_expansion(topic):
    config = TOPICS[topic]
    state_table = table(["Stav škvrny", "Čo urobiť", "Poznámka"], config["state_rows"])
    textile_table = table(["Textil", "Postup", "Prečo"], config["textile_rows"])
    sections = "\n".join(
        f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>"
        for title, first, second in config["sections"]
    )
    depth = "\n".join(
        f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>"
        for title, first, second in config["depth"]
    )
    faq = "\n".join(f"<h3>{question}</h3>\n<p>{answer}</p>" for question, answer in config["faq"])
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["intro"][0]}</p>
        <p>{config["intro"][1]}</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Prečo je táto škvrna špecifická</h2>
        <p>{config["problem"].capitalize()}. Preto sa neoplatí začínať iba silným pracím cyklom. Najprv treba zistiť, či riešite objem, lepkavosť, pigment alebo zvyšok po predchádzajúcom praní.</p>
        <p>Pri textile ako {config["main_textile"]} je rovnako dôležitý materiál. Detské veci môžu mať potlač, elastan, rebrované lemy alebo viac vrstiev. Vyhnite sa hlavne tomu, čo škvrnu zafixuje: {config["avoid"]}.</p>
        <h2>Postup podľa stavu škvrny</h2>
        {state_table}
        <h2>Postup podľa typu textilu</h2>
        {textile_table}
        {sections}
        <h2>Odbornejší pohľad: prečo rozhoduje predčistenie</h2>
        <p>Pri domácich škvrnách často nerozhoduje iba značka pracieho prostriedku, ale poradie krokov. Najprv treba odstrániť to, čo je na povrchu, potom uvoľniť to, čo drží vo vlákne, a až následne prať celú dávku. Tento princíp sa opakuje pri ovocí, sirupe, farbách, atramente aj detských tvorivých hmotách.</p>
        <p>Praktické databázy škvrn odporúčajú posudzovať typ škvrny, typ textilu a kontrolu pred sušením. Ako užitočný odborný zdroj k všeobecnému princípu predčistenia slúži <a rel="noopener" href="https://extension.illinois.edu/global/stain-solutions" target="_blank">Illinois Extension Stain Solutions</a>.</p>
        {depth}
        <h2>Kontrolný checklist pred praním</h2>
        <p>Predtým než dáte {config["main_textile"]} do práčky, skontrolujte štyri veci: či na povrchu nezostal objem škvrny, či miesto už nie je lepkavé, či sa pigment nerozpíja do okolia a či štítok povoľuje zvolený program. Tento krátky kontrolný krok často rozhodne o tom, či sa škvrna stratí, alebo sa po praní ukáže ešte výraznejšie.</p>
        <p>Ak je textil farebný, potlačený alebo jemný, najprv skúste postup na menej viditeľnom mieste. Pri detskom oblečení má zmysel aj kontrola lemov, vreciek, rebrovania a švov. Práve tam sa zvyšky držia najdlhšie a bežné pranie ich nemusí vypláchnuť.</p>
        <h2>Kedy postup zopakovať a kedy už nepokračovať agresívne</h2>
        <p>Ak po prvom praní zostal iba slabý tieň, je lepšie zopakovať šetrné lokálne predčistenie než zvýšiť teplotu naslepo. Agresívny postup môže poškodiť farbu, potlač alebo tvar odevu, zatiaľ čo opakované jemné kroky často odstránia zvyšok bez viditeľnej škody.</p>
        <p>Naopak, ak textil púšťa farbu, mení povrch alebo ide o drahší kus so špeciálnou úpravou, zastavte domáce experimentovanie skôr. Cieľom nie je vyhrať nad škvrnou za každú cenu, ale zachovať oblečenie použiteľné.</p>
        <h2>Ako predísť zafixovaniu škvrny pri sušení</h2>
        <p>Najčastejšia chyba pri detských a školských škvrnách je sušiť textil hneď po prvom praní bez kontroly. Mokrá látka vie klamať: pigment alebo lepkavý film môže byť menej viditeľný a ukáže sa až po preschnutí. Preto kontrolujte miesto pri dennom svetle a sušičku používajte až vtedy, keď je výsledok čistý.</p>
        <p>Ak máte pochybnosť, nechajte kus vyschnúť voľne a bez tepla. Potom sa rozhodnite, či treba ďalšie lokálne predčistenie. Tento postup je pomalší, ale pri detských tričkách, mikinách, šatách a zásterách znižuje riziko trvalej mapy.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Rýchla zásada</h2>
        <p>Najprv riešte konkrétnu škvrnu, potom perte celý kus. Vôňa, aviváž ani dlhý program nenahradia predčistenie, ak v látke stále zostal pigment, cukor, lepkavý film alebo zvyšok hmoty.</p>
        </div>
        {recommendation_card(config)}
        {build_related_links(config["links"])}
        <h2>FAQ: praktické otázky</h2>
        {faq}
        """
    )


MARKERS = {key: value["marker"] for key, value in TOPICS.items()}
EXPANSIONS = {key: build_expansion(key) for key in TOPICS}


def article_slug(article):
    if article.get("link"):
        return article["link"]
    if article.get("slug"):
        return article["slug"]
    if article.get("url"):
        return article["url"].rstrip("/").split("/")[-1]
    return ""


def load_source(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data, data
    if isinstance(data, dict) and isinstance(data.get("updates"), list):
        return data, data["updates"]
    raise SystemExit(f"Unsupported source format: {path}")


def insertion_index(long):
    candidates = [
        long.find('<div style="border: 1px solid #dbe5de'),
        long.find("\n<h2>Súvisiace"),
        long.find("\n<h2>FAQ"),
    ]
    candidates = [index for index in candidates if index != -1]
    if not candidates:
        raise ValueError("Could not find safe insertion point")
    return min(candidates)


def insert_expansion(long, key):
    if MARKERS[key] in long:
        start = long.find(f"<h2>{MARKERS[key]}</h2>")
        faq_start = long.find("<h2>FAQ: praktick", start)
        search_from = faq_start if faq_start != -1 else start + len(MARKERS[key])
        candidates = [
            long.find('<div style="border: 1px solid #dbe5de', search_from),
            long.find("\n<h2>Súvisiace", search_from),
            long.find("\n<h2>FAQ", search_from + 1),
        ]
        candidates = [index for index in candidates if index != -1]
        if not candidates:
            raise ValueError("Could not find safe replacement end point")
        end = min(candidates)
        return long[:start].rstrip() + "\n" + EXPANSIONS[key] + "\n" + long[end:].lstrip()
    index = insertion_index(long)
    return long[:index].rstrip() + "\n" + EXPANSIONS[key] + "\n" + long[index:].lstrip()


def mcp_url():
    config = CONFIG_PATH.read_text(encoding="utf-8")
    match = re.search(r'(?s)\[mcp_servers\.biznisweb-vevo\]\s*url\s*=\s*"([^"]+)"', config)
    if not match:
        raise SystemExit("biznisweb-vevo MCP URL not found in ~/.codex/config.toml")
    return match.group(1)


def parse_sse_json(text):
    for line in text.splitlines():
        if line.startswith("data: "):
            return json.loads(line[6:])
    raise ValueError(f"No JSON data line in MCP response: {text[:500]}")


def call_update(endpoint, payload, request_id):
    body = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": "biznisweb-update_news_post", "arguments": payload},
    }
    response = requests.post(
        endpoint,
        json=body,
        headers={"Accept": "application/json, text/event-stream"},
        timeout=120,
    )
    response.raise_for_status()
    parsed = parse_sse_json(response.text)
    if "error" in parsed:
        raise RuntimeError(json.dumps(parsed["error"], ensure_ascii=False))
    result = parsed.get("result", {})
    for item in result.get("content", []):
        if item.get("type") != "text":
            continue
        try:
            inner = json.loads(item.get("text", ""))
        except json.JSONDecodeError:
            continue
        if inner.get("error"):
            raise RuntimeError(inner["error"])
    return parsed


def main():
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 14 kids and school stain articles.")
    parser.add_argument("--update-live", action="store_true")
    parser.add_argument("--sleep", type=float, default=1.5)
    args = parser.parse_args()

    loaded = {}
    updates = []

    for config in ARTICLES:
        source = config["source"]
        if source not in loaded:
            loaded[source] = load_source(source)
        data, rows = loaded[source]

        for article in rows:
            if article_slug(article) != config["slug"]:
                continue
            original = {
                "title": article.get("title"),
                "short": article.get("short", ""),
                "slug": article_slug(article),
                "date_posted": article.get("date_posted"),
                "time_posted": article.get("time_posted"),
                "active": article.get("active"),
                "link": article.get("link"),
                "url": article.get("url"),
            }
            original_long = article["long"]
            article["long"] = insert_expansion(article["long"], config["topic"])
            if (
                article.get("title") != original["title"]
                or article_slug(article) != original["slug"]
                or article.get("short", "") != original["short"]
                or article.get("date_posted") != original["date_posted"]
                or article.get("time_posted") != original["time_posted"]
                or article.get("active") != original["active"]
                or article.get("link") != original["link"]
            ):
                raise SystemExit(f"Retrofit attempted to change protected metadata for {config['slug']}")
            if original["url"] and article.get("url") != original["url"]:
                raise SystemExit(f"Retrofit attempted to change URL for {config['slug']}")
            updates.append(
                {
                    "post_id": config["post_id"],
                    "slug": config["slug"],
                    "url": config["url"],
                    "title": article["title"],
                    "short": article["short"],
                    "long": article["long"],
                    "source_file": str(source.relative_to(ROOT)),
                    "original_length": len(original_long),
                    "new_length": len(article["long"]),
                    "title_preserved": True,
                    "slug_preserved": True,
                    "url_preserved": True,
                    "short_preserved": True,
                    "date_preserved": True,
                    "visibility_preserved": True,
                }
            )
            break
        else:
            raise SystemExit(f"Article not found: {config['slug']}")

    for source, (data, _) in loaded.items():
        source.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    OUT_JSON.write_text(
        json.dumps(
            {
                "project": "VEVO_CONTENT",
                "wave": "retrofit-wave-14-kids-school-stains-five",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "purpose": "Conservative additive expansion. Titles, slugs, URLs, dates, visibility, and short descriptions are preserved.",
                "updates": updates,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    mcp_updates = []
    if args.update_live:
        endpoint = mcp_url()
        for index, item in enumerate(updates, start=1):
            result = call_update(
                endpoint,
                {
                    "post_id": item["post_id"],
                    "title": item["title"],
                    "short": item["short"],
                    "long": item["long"],
                    "visible": True,
                },
                index,
            )
            mcp_updates.append(
                {
                    "post_id": item["post_id"],
                    "slug": item["slug"],
                    "url": item["url"],
                    "mcp_result": result.get("result", result),
                }
            )
            time.sleep(args.sleep)

    MCP_RESULTS.write_text(
        json.dumps(
            {
                "project": "VEVO_CONTENT",
                "wave": "retrofit-wave-14-kids-school-stains-five",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "live_updated": args.update_live,
                "updated_count": len(mcp_updates),
                "updates": mcp_updates,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "source_updates": len(updates),
                "live_updated": args.update_live,
                "mcp_updates": len(mcp_updates),
                "out": str(OUT_JSON),
                "mcp_results": str(MCP_RESULTS),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
