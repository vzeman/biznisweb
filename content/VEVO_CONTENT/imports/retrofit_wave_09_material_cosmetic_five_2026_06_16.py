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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-09-material-cosmetic-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-09-material-cosmetic-five-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-odstranit-maskaru-z-uteraka-zupanu-a-bielej-osusky",
        "post_id": "2146",
        "url": "https://www.vevo.sk/n/ako-odstranit-maskaru-z-uteraka-zupanu-a-bielej-osusky",
        "topic": "mascara_towel",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-odstranit-mastnu-mast-z-uteraka-pyzama-a-tricka",
        "post_id": "2217",
        "url": "https://www.vevo.sk/n/ako-odstranit-mastnu-mast-z-uteraka-pyzama-a-tricka",
        "topic": "greasy_ointment",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-prat-viskozovu-bluzku-aby-nestratila-tvar-a-neostala-vytahana",
        "post_id": "2153",
        "url": "https://www.vevo.sk/n/ako-prat-viskozovu-bluzku-aby-nestratila-tvar-a-neostala-vytahana",
        "topic": "viscose_blouse",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-akrylovu-farbu-z-tricka-bez-zafixovania",
        "post_id": "2167",
        "url": "https://www.vevo.sk/n/ako-odstranit-akrylovu-farbu-z-tricka-bez-zafixovania",
        "topic": "acrylic_paint",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit",
        "post_id": "2126",
        "url": "https://www.vevo.sk/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit",
        "topic": "softshell_impregnation",
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


def product_card(kind):
    if kind == "samples":
        return clean(
            """
            <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
            <h2 style="margin-top: 0;">Odporúčané riešenie na jemné testovanie vône</h2>
            <p>Pri jemných alebo funkčných materiáloch má zmysel začať opatrne. Najprv vyriešte čistotu a až potom skúšajte vôňu v nižšej intenzite.</p>
            <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
            <h3 style="margin-top: 0;">Vevo Essence Sample Set 9x10ml</h3>
            <p>Vzorkový set pomôže porovnať viac vôní postupne a bez veľkého balenia.</p>
            <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1621/vevo-essence-sample-set">Pozrieť vzorkový set</a></p>
            </div>
            <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vzorky/parfum-do-prania-vzorky">Pozrieť vzorky parfumov do prania</a></p>
            </div>
            """
        )
    return clean(
        """
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie na pranie po škvrnách</h2>
        <p>Pri kozmetike, mastnote a farbách najprv riešte konkrétnu škvrnu. Prací produkt má pomôcť odstrániť zvyšky z vlákna, nie prekryť problém.</p>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>Univerzálny základ na bežné pranie po lokálnom predčistení škvrny. Pri špeciálnych materiáloch vždy rešpektujte štítok.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
        </div>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        """
    )


TOPICS = {
    "mascara_towel": {
        "marker": "Prečo maskara na uteráku drží pigment aj mastnotu",
        "product_kind": "laundry",
        "intro": [
            "Maskara na uteráku, župane alebo bielej osuške je kombinácia pigmentu, voskov, olejov a často aj vodeodolných zložiek. Preto sa pri praní nespráva ako obyčajná sivá šmuha. Keď ju zotriete z tváre do froté uteráka, dostane sa medzi slučky vlákna a môže po vysušení zostať ako tmavý tieň.",
            "Najdôležitejšie je nevtierať maskaru hlbšie. Pri uteráku a župane je froté savé, ale práve preto vie kozmetiku držať pevnejšie. Pri bielej osuške sa chyba ukáže hneď, pri farebnom uteráku často až po tom, čo miesto stratí savosť alebo ostane mastnejšie.",
        ],
        "bullets": [
            "<strong>Čerstvá maskara:</strong> najprv odsajte, netrite do strán.",
            "<strong>Vodeodolná maskara:</strong> počítajte aj s olejovou zložkou.",
            "<strong>Biela osuška:</strong> kontrolujte tieň pred sušením.",
            "<strong>Župan:</strong> skontrolujte golier, rukávy a miesta pri tvári.",
        ],
        "table": {
            "headers": ["Textil", "Ako sa škvrna prejaví", "Prvý krok"],
            "rows": [
                ("Froté uterák", "tmavý pigment medzi slučkami", "jemne predčistiť bez drhnutia"),
                ("Biela osuška", "sivý alebo čierny tieň", "kontrola pred sušením"),
                ("Župan", "šmuha pri golieri alebo rukáve", "lokálne ošetriť a prať podľa štítku"),
                ("Uterák na tvár", "pigment aj mastný film", "prať oddelene od čistých uterákov"),
                ("Farebný uterák", "škvrna je menej viditeľná, ale drží film", "sledovať savosť a dotyk"),
            ],
        },
        "sections": [
            ("Ako odstrániť čerstvú maskaru", "Čerstvú maskaru najprv jemne odsajte papierovou utierkou alebo čistou látkou. Netrite ju do strán, pretože pigment sa rozmaže do väčšej plochy. Pri froté uteráku pracujte jemne, aby ste maskaru nezatlačili hlbšie medzi vlákna.", "Potom použite malé množstvo pracieho roztoku a ošetrite len miesto škvrny. Pri bielej osuške kontrolujte zvyškový tieň ešte pred praním. Ak sa pigment drží, zopakujte lokálne predčistenie radšej pred sušením než po ňom."),
            ("Ako riešiť vodeodolnú maskaru", "Vodeodolná maskara často obsahuje zložky, ktoré odpudzujú vodu. Preto samotné opláchnutie nemusí stačiť. Najprv treba uvoľniť pigment a mastnejší film. Pri uteráku to robte trpezlivo a v malom množstve, aby sa produkt nerozniesol do väčšej plochy.", "Ak po praní zostane hladký alebo mastnejší dotyk, maskara ešte neodišla úplne. Nepoužívajte sušičku ani radiátor, kým nie ste spokojní s výsledkom. Teplo môže zvyšky zvýrazniť a ďalšie čistenie bude ťažšie."),
            ("Ako prať biele osušky po kozmetike", "Biele osušky po kozmetike perte s dostatočným priestorom v bubne. Ak ich periete natlačené s ďalšími uterákmi, pigment a mastnota sa nemusia dobre vypláchnuť. Pri bielych textíliách sledujte nielen farbu, ale aj savosť.", "Ak je osuška po praní čistá na pohľad, ale miesto odpudzuje vodu alebo je tvrdšie, problém je stále vo vlákne. Vtedy pomôže opakované lokálne predčistenie a ďalší oplach, nie silnejšia vôňa."),
            ("Ako predchádzať škvrnám pri odličovaní", "Na odličovanie nepoužívajte bežný biely uterák, ak často používate vodeodolnú maskaru. Praktickejší je menší tmavší uterák vyhradený na tvár alebo odličovacie tampóny. Znížite tým riziko, že pigment skončí na veľkej osuške alebo župane.", "Ak používate odličovací olej, škvrna môže obsahovať aj mastnú zložku. Uterák potom perte skôr a nenechávajte ho vlhký v koši. Vlhkosť, pigment a mastnota sú kombinácia, ktorá sa po pár dňoch odstraňuje horšie."),
            ("Ako kontrolovať výsledok po praní", "Po praní sa pozrite na uterák pri dennom svetle a prejdite miesto prstami. Ak je textil hladší, tmavší alebo menej savý, maskara ešte nie je úplne preč. Pri bielej osuške kontrolujte aj okolie škvrny, pretože pigment sa môže počas predčistenia rozšíriť.", "Súvisiace postupy nájdete aj pri kozmetických škvrnách, napríklad <a href=\"/n/ako-odstranit-podkladovy-krem-z-goliera-bluzky-a-kosele\">podkladový krém na golieri</a> alebo <a href=\"/n/ako-odstranit-vlasove-serum-z-uteraka-a-goliera-kosele\">vlasové sérum na uteráku</a>."),
        ],
        "box": ("Kontrola pred sušičkou", "Maskaru sušte teplom až vtedy, keď nevidíte tieň a necítite mastný film. Teplo zvyšky zafixuje a škvrna sa bude riešiť ťažšie."),
        "faq": [
            ("Dá sa maskara vyprať z bieleho uteráka?", "Áno, ale najlepšie po lokálnom predčistení. Pri bielej osuške vždy kontrolujte tieň pred sušením."),
            ("Prečo maskara ostala aj po praní?", "Môže obsahovať vosky, pigmenty a vodeodolné zložky. Bežné pranie bez predčistenia nemusí stačiť."),
            ("Môžem použiť silnejšiu vôňu?", "Vôňa škvrnu neodstráni. Najprv treba dostať z vlákna pigment a mastný film."),
        ],
    },
    "greasy_ointment": {
        "marker": "Prečo mastná masť zanecháva film aj po praní",
        "product_kind": "laundry",
        "intro": [
            "Mastná masť na uteráku, pyžame alebo tričku je typická tým, že sa neprejaví len farbou. Často zanechá hladký film, tmavšiu mapu a miesto, ktoré po praní horšie schne alebo inak saje vodu. Ak ide o detskú masť, ochranný krém alebo hojivú masť, olejový základ môže držať vo vlákne veľmi pevne.",
            "Najhorší postup je hneď naliať horúcu vodu alebo hodiť textil do sušičky. Teplo môže mastnotu rozptýliť alebo zvýrazniť. Lepší postup je mechanicky odobrať prebytok, lokálne predčistiť a až potom prať podľa štítku.",
        ],
        "bullets": [
            "<strong>Najprv odobrať prebytok:</strong> lyžičkou alebo tupou hranou, nie trením.",
            "<strong>Potom predčistiť:</strong> malé množstvo pracieho roztoku priamo na mastné miesto.",
            "<strong>Pred sušením:</strong> skontrolovať mapu aj dotyk.",
            "<strong>Pri detských veciach:</strong> kontrolovať lemy, švy a vrstvy látky.",
        ],
        "table": {
            "headers": ["Textil", "Riziko", "Praktický postup"],
            "rows": [
                ("Uterák", "strata savosti a mastný dotyk", "lokálne predčistiť a dobre opláchnuť"),
                ("Pyžamo", "mastnota v lemoch a pri švoch", "predčistiť pred praním"),
                ("Tričko", "tmavšia mapa po vysušení", "nesušiť teplom pred kontrolou"),
                ("Detské body", "masť pri zapínaní a okrajoch", "prať skôr a neodkladať vlhké"),
                ("Posteľná bielizeň", "prenos z pokožky počas noci", "kontrolovať obliečku a plachtu"),
            ],
        },
        "sections": [
            ("Ako odobrať masť bez rozmazania", "Prebytočnú masť najprv jemne odoberte tupou hranou, kartičkou alebo papierovou utierkou. Netrite ju do strán, pretože sa rozmaže do väčšej plochy a dostane sa hlbšie do vlákna. Pri uteráku s froté slučkami pracujte obzvlášť opatrne.", "Až potom použite malé množstvo pracieho roztoku. Cieľom je uvoľniť mastný film, nie premočiť celý kus. Pri farebnom tričku testujte na skrytom mieste, aby ste nepoškodili farbu."),
            ("Ako prať pyžamo a detské veci", "Pyžamo a detské oblečenie po masti perte čo najskôr, najmä ak je masť v lemoch, okolo zapínania alebo na miestach s viacerými vrstvami. Práve tam sa mastný film drží dlhšie. Pred praním ošetrite konkrétne miesta, nie iba celý kus.", "Ak po praní ostane mastná mapa, textil nesušte teplom. Zopakujte lokálne predčistenie a perte v menšej dávke. Preplnený bubon môže spôsobiť, že mastnota a prací prostriedok sa nevypláchnu dostatočne."),
            ("Ako riešiť uteráky po mastných krémoch", "Uteráky po mastiach a krémoch môžu po čase horšie sať. Olejový film obalí vlákna a bežné pranie ho nemusí odstrániť na prvýkrát. Pri uterákoch preto sledujte savosť, nielen vzhľad. Ak voda na mieste stojí alebo steká, mastnota ešte ostala.", "Uteráky neperte v preplnenej dávke a dávkujte prací gél primerane. Veľa produktu môže zhoršiť oplach a uterák bude tvrdší. Ak riešite podobné kozmetické škvrny, pomôže aj návod <a href=\"/n/ako-vyprat-stare-skvrny-kompletny-sprievodca-pre-ciste-oblecenie\">ako vyprať staré škvrny</a>."),
            ("Ako predchádzať mastným mapám", "Pri používaní mastí nechajte produkt chvíľu vsiaknuť, až potom si oblečte pyžamo alebo si ľahnite do postele. Ak ide o masť používanú denne, vyhraďte si konkrétne pyžamo alebo uterák, ktorý budete prať častejšie a nebudete ho miešať s jemnými kúskami.", "Pri detských mastiach dávajte pozor aj na prebaľovaciu podložku, deku a body. Mastnota sa prenáša dotykom. Ak ošetríte iba jeden kus a ostatné nie, škvrny sa budú objavovať znova."),
            ("Ako skontrolovať výsledok", "Po praní sa na miesto pozrite pri dennom svetle a prejdite ho prstami. Mastná škvrna často nezmizne úplne ani vtedy, keď je menej viditeľná. Ak je látka hladšia alebo tmavšia, masť ešte zostala. Pred žehlením alebo sušením teplom postup zopakujte.", "Pri uteráku spravte test kvapkou vody. Ak sa voda nevsiakne podobne ako inde, film ešte ostal. Vôňu pridávajte až po odstránení mastnoty, inak sa bude miešať s ťažkým kozmetickým pachom."),
        ],
        "box": ("Kontrola pred teplom", "Mastné škvrny nekontrolujte až po sušičke. Teplo môže mapu zvýrazniť. Skontrolujte dotyk, farbu aj savosť ešte mokré alebo po voľnom preschnutí."),
        "faq": [
            ("Dá sa mastná masť vyprať z uteráka?", "Áno, ale často potrebuje lokálne predčistenie. Pri uteráku sledujte aj savosť po praní."),
            ("Prečo škvrna zostala po vysušení?", "Mastný film sa mohol držať vo vlákne. Teplo ho zvýrazní, preto treba kontrolovať pred sušením."),
            ("Je lepšie použiť viac gélu?", "Nie automaticky. Dôležitejšie je lokálne predčistenie, nepreplnený bubon a dobrý oplach."),
        ],
    },
    "viscose_blouse": {
        "marker": "Prečo viskózová blúzka mení tvar najmä za mokra",
        "product_kind": "samples",
        "intro": [
            "Viskózová blúzka pôsobí ľahko, mäkko a splývavo, ale za mokra je citlivejšia než mnohé bežné materiály. Môže sa vytiahnuť, skrútiť, zmeniť dĺžku alebo pôsobiť po praní pokrčenejšie. Problém často nevznikne samotným praním, ale krútením, ťažkým zavesením a zlým sušením.",
            "Pri viskóze je preto dôležité prať šetrne, netlačiť ju do preplneného bubna a po praní ju nevytiahnuť za mokra. Ak blúzku zavesíte plnú vody na vešiak, vlastná hmotnosť ju môže natiahnuť. Ak ju silno vyžmýkate, môžete poškodiť povrch alebo švy.",
        ],
        "bullets": [
            "<strong>Pred praním:</strong> skontrolujte štítok a zapnite gombíky.",
            "<strong>Po praní:</strong> nekrútiť, iba jemne vytlačiť vodu.",
            "<strong>Sušenie:</strong> tvarovať a sušiť bez ťahu.",
            "<strong>Žehlenie:</strong> opatrne podľa štítku a skôr z rubu.",
        ],
        "table": {
            "headers": ["Situácia", "Riziko", "Lepší postup"],
            "rows": [
                ("Preplnený bubon", "krčenie a trenie", "prať v menšej dávke"),
                ("Silné žmýkanie", "vytiahnutie a poškodenie švov", "nízke otáčky alebo jemné vytlačenie"),
                ("Vešanie mokrej blúzky", "predĺženie tvaru", "najprv odsať vodu uterákom"),
                ("Horúce žehlenie", "lesk alebo poškodenie povrchu", "žehliť opatrne z rubu"),
                ("Silná vôňa", "ťažký pocit na jemnej látke", "testovať mierne dávkovanie"),
            ],
        },
        "sections": [
            ("Ako prať viskózovú blúzku v práčke", "Ak štítok povoľuje práčku, použite jemný program, nižšiu teplotu a nižšie otáčky. Blúzku otočte naruby a perte ju s podobne jemnými kúskami. Neperte ju s uterákmi, rifľami, zipsami alebo ťažkými mikinami. Viskóza nemá rada drsné trenie, najmä keď je mokrá.", "Dávkujte primerane. Priveľa pracieho prostriedku sa z jemnej látky horšie vyplachuje a blúzka môže po vysušení pôsobiť tvrdšie alebo ťažšie. Ak chcete pridať vôňu, robte to jemne, pretože viskózová blúzka je často nosená blízko tela."),
            ("Ako sušiť viskózu bez vytiahnutia", "Po praní blúzku nekrúťte. Položte ju do uteráka a jemne vytlačte prebytočnú vodu. Potom ju vytvarujte do pôvodnej dĺžky a šírky. Pri ľahších blúzkach môže byť vešiak v poriadku až po odsatí väčšiny vody, pri ťažšej mokrej viskóze je bezpečnejšie sušenie naplocho alebo cez širšiu plochu.", "Nesušte ju na radiátore ani na priamom horúcom vzduchu, ak to štítok neodporúča. Teplo môže zmeniť povrch a podporiť zrážanie alebo krčenie. Počas sušenia skontrolujte švy a lem, či sa nekrútia."),
            ("Ako riešiť pokrčenie a tvar po praní", "Viskóza sa rada krčí, ale často sa dá upraviť parou alebo jemným žehlením podľa štítku. Žehlite radšej z rubu a skúste najprv menej viditeľné miesto. Ak je blúzka ešte mierne vlhká, tvarovanie býva jednoduchšie než po úplnom presušení.", "Ak sa blúzka po praní predĺžila, pravdepodobne visela príliš mokrá. Nabudúce najprv odsajte vodu uterákom a sušte bez ťahu. Ak sa zrazila, skontrolujte teplotu prania aj sušenia. Súvisiaci materiálový návod je <a href=\"/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost\">čo je viskóza</a>."),
            ("Ako predchádzať poškodeniu blúzky", "Pred praním zapnite gombíky, uvoľnite opasok alebo viazanie a skontrolujte, či blúzka nemá jemné aplikácie. Pri veľmi tenkej viskóze pomôže pracie vrecko. Blúzku neperte v dávke, kde sa môže omotať okolo ťažších kusov.", "Pri škvrnách na viskóze postupujte opatrne. Mokrý materiál je citlivý, preto silné trenie môže vytvoriť svetlejší fľak alebo zmeniť povrch. Lokálne ošetrenie vždy testujte na skrytom mieste."),
            ("Ako kombinovať čistotu a vôňu pri viskóze", "Viskóza vie dobre držať jemnú vôňu, ale príliš intenzívna parfumácia môže pôsobiť ťažko. Najprv riešte správne pranie, oplach a sušenie. Ak blúzka po praní zapácha, problém bude skôr v pomalom sušení, preplnenom bubne alebo zvyškoch pracieho prostriedku.", "Pri citlivej pokožke alebo blúzke nosenej celý deň začnite nižšou intenzitou. Vôňa má byť príjemná pri pohybe, nie výrazná hneď po otvorení skrine."),
        ],
        "box": ("Kontrola po vysušení", "Blúzku po vysušení skontrolujte v dĺžke, šírke a pri švoch. Ak sa vytiahla, upravte hlavne sušenie, nie iba prací program."),
        "faq": [
            ("Môže ísť viskóza do práčky?", "Iba ak to povoľuje štítok. Použite jemný program, nízke otáčky a menšiu dávku bielizne."),
            ("Prečo sa viskózová blúzka vytiahla?", "Pravdepodobne visela príliš mokrá alebo bola silno namáhaná za mokra. Viskóza je vtedy citlivejšia."),
            ("Ako sušiť viskózovú blúzku?", "Najprv odsajte vodu uterákom, vytvarujte ju a sušte bez ťahu. Vyhnite sa horúcemu radiátoru."),
        ],
    },
    "acrylic_paint": {
        "marker": "Prečo akrylovú farbu riešiť skôr, než zaschne",
        "product_kind": "laundry",
        "intro": [
            "Akrylová farba je vodou riediteľná najmä v čerstvom stave. Keď zaschne, vytvorí pevnejší plastový film, ktorý drží na vlákne výrazne lepšie. Preto je rozdiel medzi čerstvou škvrnou z tvorenia a zaschnutou farbou po niekoľkých hodinách zásadný. Čím skôr zasiahnete, tým väčšia šanca na dobrý výsledok.",
            "Pri tričku, detskej zástere alebo mikine je dôležité farbu nerozotrieť do väčšej plochy a nezafixovať teplom. Horúca voda, sušička alebo žehlenie môžu zvyšky farby spevniť. Najprv odstráňte prebytok, preplachujte zo zadnej strany a až potom perte.",
        ],
        "bullets": [
            "<strong>Čerstvá farba:</strong> odstrániť prebytok a prepláchnuť zo zadnej strany.",
            "<strong>Zaschnutá farba:</strong> najprv mechanicky uvoľniť povrch, opatrne.",
            "<strong>Teplo:</strong> nepoužiť, kým škvrna nezmizne.",
            "<strong>Detské oblečenie:</strong> skontrolovať rukávy, lem a zásteru.",
        ],
        "table": {
            "headers": ["Stav farby", "Čo znamená", "Prvý krok"],
            "rows": [
                ("Mokrá", "stále sa dá riediť vodou", "odsatie a studenšie prepláchnutie"),
                ("Polosuchá", "začína tvoriť film", "jemne uvoľniť, nešúchať do strán"),
                ("Zaschnutá", "film drží na vlákne", "opatrne mechanicky odstrániť vrch"),
                ("Na potlači", "riziko poškodenia obrázka", "testovať a nešúchať agresívne"),
                ("Na jemnej látke", "riziko svetlej mapy", "skúsiť skryté miesto"),
            ],
        },
        "sections": [
            ("Ako postupovať pri čerstvej akrylovej farbe", "Čerstvú farbu najprv jemne odstráňte z povrchu. Nepoužívajte trenie do strán, aby ste škvrnu nerozšírili. Potom preplachujte zo zadnej strany látky studenšou vodou. Cieľom je tlačiť farbu von z vlákna, nie hlbšie do trička.", "Po prepláchnutí ošetrite miesto malým množstvom pracieho roztoku a perte podľa štítku. Pred sušením skontrolujte, či nezostal farebný tieň alebo tvrdší okraj. Ak áno, postup zopakujte."),
            ("Ako riešiť zaschnutú akrylovú farbu", "Zaschnutú farbu najprv skúste opatrne uvoľniť z povrchu. Nepoužívajte ostrý nôž ani agresívne škrabanie, ktoré môže prerezať vlákna. Ak je farba hrubá, odstráňte len vrchnú časť a až potom pokračujte lokálnym ošetrením.", "Pri zaschnutej farbe môže zostať tieň alebo tvrdý film aj po praní. Nepoužívajte sušičku, kým nie ste spokojní s výsledkom. Teplo je pri akrylovej farbe nevýhodné, pretože môže zvyšky viac zafixovať."),
            ("Čo robiť pri detskom tvorení", "Pri tvorení s deťmi skontrolujte rukávy, spodný lem trička, kolená teplákov a zásteru. Farba často nekončí len na jednom viditeľnom fľaku. Ak oblečenie hodíte rovno do koša, farba zaschne a šanca na jednoduché odstránenie klesne.", "Praktické je mať tvorivé oblečenie, pri ktorom nevadí menší tieň. Ak však chcete zachrániť bežné tričko, riešte farbu hneď. Súvisiaci návod je aj <a href=\"/n/ako-vyprat-stare-skvrny-kompletny-sprievodca-pre-ciste-oblecenie\">ako vyprať staré škvrny</a>."),
            ("Ako predísť zafixovaniu farby", "Kým neviete, že škvrna zmizla, nepoužívajte teplú sušičku, radiátor ani žehličku. Akrylová farba po zaschnutí tvorí film a teplo jej nepomáha. Pri praní voľte postup podľa látky, nie podľa snahy škvrnu spáliť vysokou teplotou.", "Ak ide o farebné tričko, testujte aj stálosť farby textilu. Silné lokálne čistenie môže vyčistiť škvrnu, ale zanechať svetlejší kruh. Preto pracujte postupne a kontrolujte výsledok medzi krokmi."),
            ("Ako kontrolovať výsledok", "Po praní sa pozrite na miesto proti svetlu a prejdite ho prstami. Ak je tvrdšie, farba ešte zostala ako film. Ak vidíte iba slabý tieň, zvážte opakovanie jemného predčistenia. Pri detskom oblečení je niekedy realistické zachrániť nositeľnosť, nie dokonalý pôvodný vzhľad.", "Vôňa do prania nemá pri farbe veľký význam, kým je farba vo vlákne. Najprv odstráňte materiál škvrny, potom perte bežne a až následne riešte sviežosť textilu."),
        ],
        "box": ("Kontrola pred teplom", "Ak je miesto tvrdé alebo farebné, ešte ho nesušte teplom. Pri akrylovej farbe je teplo častý dôvod, prečo škvrna zostane natrvalo."),
        "faq": [
            ("Dá sa akrylová farba vyprať?", "Čerstvá často áno, zaschnutá je výrazne náročnejšia. Rozhoduje rýchlosť zásahu a materiál textilu."),
            ("Mám použiť horúcu vodu?", "Nie ako prvý krok. Pri akrylovej farbe môže teplo zvyšky zafixovať. Začnite studenším prepláchnutím."),
            ("Čo ak farba zaschla?", "Opatrne uvoľnite povrch, lokálne predčistite a nežehlite ani nesušte teplom, kým škvrna nezmizne."),
        ],
    },
    "softshell_impregnation": {
        "marker": "Kedy softshell impregnáciu naozaj potrebuje",
        "product_kind": "samples",
        "intro": [
            "Impregnáciu softshellu netreba obnovovať po každom praní. Najprv treba rozlíšiť, či je problém v špine, zvyškoch pracieho prostriedku, zlej starostlivosti alebo skutočne v oslabenej vodoodpudivej úprave. Ak voda na povrchu ešte tvorí kvapky, impregnácia môže byť stále funkčná.",
            "Softshell je praktický materiál, ale pri praní potrebuje presnosť. Aviváž, príliš veľa pracieho prostriedku, preplnená práčka alebo horúce sušenie môžu zhoršiť pocit z materiálu aj jeho funkčnosť. Obnova impregnácie má zmysel až po správnom vypraní a vysušení.",
        ],
        "bullets": [
            "<strong>Najprv test kvapiek:</strong> voda má na povrchu perliť.",
            "<strong>Najprv čistota:</strong> špina môže vyzerať ako strata impregnácie.",
            "<strong>Bez aviváže:</strong> môže zhoršiť funkčné vlastnosti.",
            "<strong>Podľa štítku:</strong> nie každý softshell sa ošetruje rovnako.",
        ],
        "table": {
            "headers": ["Prejav", "Možná príčina", "Čo spraviť"],
            "rows": [
                ("Voda sa perlí", "úprava ešte funguje", "impregnáciu neriešiť"),
                ("Voda sa vpíja plošne", "oslabená úprava alebo špina", "najprv vyprať správne"),
                ("Bunda je ťažká po praní", "slabý oplach alebo veľa produktu", "znížiť dávku a pridať oplach"),
                ("Materiál je mastný", "nevhodný prípravok alebo špina", "čistiť podľa štítku"),
                ("Membrána alebo úprava", "citlivý funkčný prvok", "nepoužiť aviváž ani horúci postup"),
            ],
        },
        "sections": [
            ("Ako spraviť test kvapiek", "Na čistý a suchý softshell kvapnite trochu vody. Ak sa kvapky držia na povrchu a dajú sa striasť, vodoodpudivá úprava ešte funguje. Ak sa voda rýchlo vpíja do väčšej plochy, môže byť úprava oslabená alebo je materiál zanesený špinou.", "Test robte až po vypraní a vysušení, nie na zablatenom alebo mastnom povrchu. Špina dokáže narušiť perlenie vody, hoci impregnácia ešte nemusí byť úplne preč."),
            ("Ako prať softshell pred impregnáciou", "Pred obnovou impregnácie musí byť softshell čistý. Zapnite zipsy, vyprázdnite vrecká, otočte citlivé časti podľa štítku a neperte bundu s uterákmi alebo ťažkým oblečením. Použite primerané množstvo vhodného pracieho produktu a nepreplňte bubon.", "Aviváž pri softshelle vynechajte. Môže zanechať film a zhoršiť funkčné vlastnosti materiálu. Ak bunda po praní pôsobí ťažko alebo lepkavo, problém môže byť v oplachu, nie v impregnácii."),
            ("Kedy impregnáciu neriešiť", "Ak softshell používate najmä do mesta alebo ako ľahkú vrstvu a voda na ňom stále perlí, impregnáciu netreba obnovovať naslepo. Zbytočné ošetrenie môže vytvoriť nerovnomerný film alebo zmeniť pocit z materiálu. Najprv sledujte reálne použitie bundy.", "Impregnácia nie je náhrada za pranie. Ak bunda zapácha, je mastná pri golieri alebo má blato na spodnom leme, najprv ju vyčistite. Až potom hodnotíte, či vodoodpudivá úprava potrebuje obnovu."),
            ("Ako impregnáciu aplikovať opatrne", "Ak štítok a výrobca povoľujú obnovu, zvoľte prípravok vhodný na softshell alebo funkčné textílie. Aplikujte ho rovnomerne na čistý materiál a dodržte pokyny k aktivácii alebo sušeniu. Nepoužívajte univerzálny prípravok naslepo, ak neviete, či sedí konkrétnej bunde.", "Pri nohaviciach alebo bunde s membránou buďte ešte opatrnejší. Niektoré postupy môžu zmeniť priedušnosť alebo povrch. Súvisiaci návod je <a href=\"/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany\">ako prať softshell bundu a nohavice</a>."),
            ("Ako skladovať softshell po sezóne", "Softshell pred odložením vyperte podľa potreby, nechajte úplne vyschnúť a skladujte voľne. Neodkladajte ho vlhký alebo zablatený v taške. Vlhkosť, pot a špina zhoršujú pach aj povrch materiálu. Na začiatku sezóny potom test kvapiek ukáže reálnejší stav.", "Ak chcete textilu dodať príjemnú vôňu, robte to veľmi jemne. Funkčné oblečenie sa nosí pri pohybe a zahriatí tela; príliš intenzívna vôňa môže byť rušivá. Najdôležitejšia je správna čistota a funkčnosť."),
        ],
        "box": ("Kontrola pred impregnáciou", "Impregnáciu riešte až na čistom a suchom softshelle. Ak voda stále perlí, ďalší prípravok pravdepodobne nepotrebujete."),
        "faq": [
            ("Treba impregnáciu obnoviť po každom praní?", "Nie. Najprv spravte test kvapiek na čistom a suchom materiáli."),
            ("Môžem použiť aviváž na softshell?", "Radšej nie. Pri funkčných materiáloch môže zhoršiť vlastnosti a zanechať film."),
            ("Prečo bunda po praní horšie odpudzuje vodu?", "Môže ísť o špinu, slabý oplach alebo oslabenú úpravu. Najprv skontrolujte pranie a až potom impregnáciu."),
        ],
    },
}


DEEPENINGS = {
    "mascara_towel": {
        "marker": "Detailnejší postup pre maskaru na uteráku a osuške",
        "table": {
            "headers": ["Otázka pri kontrole", "Čo si všimnúť", "Ako upraviť postup"],
            "rows": [
                ("Je škvrna skôr sivá alebo mastná?", "sivý tieň býva pigment, hladký dotyk býva film", "pri pigmente opakovať jemné lokálne čistenie, pri filme pridať čas pôsobenia"),
                ("Je uterák stále savý?", "voda sa má vpíjať podobne ako mimo škvrny", "ak voda stojí na povrchu, pred sušením zopakovať predčistenie"),
                ("Je maskara pri okraji alebo v slučkách?", "froté zachytí častice hlbšie ako hladká bavlna", "nešúchať kefkou do strán, radšej pretláčať vodou a pracím roztokom"),
                ("Ide o bielu osušku?", "slabý tieň vidno až pri dennom svetle", "kontrolovať pred radiátorom, sušičkou alebo žehlením"),
            ],
        },
        "sections": [
            (
                "Detailnejší postup pre maskaru na uteráku a osuške",
                "Maskara je problematická tým, že spája viac druhov nečistoty naraz. Čierny alebo hnedý pigment sa drží medzi vláknami, vosková časť vytvára klzký film a pri vodeodolných typoch sa pridáva zložka, ktorá sa obyčajným opláchnutím neuvoľní. Preto nie je dobré posudzovať škvrnu iba podľa farby. Uterák môže vyzerať svetlejší, ale stále môže horšie sať vodu alebo byť na dotyk hladší.",
                "Pri bielych osuškách a županoch je najbezpečnejšie pracovať v niekoľkých miernych krokoch. Najprv odstráňte prebytok, potom lokálne rozpustite film a až potom perte celý kus. Ak preskočíte lokálny krok, bubon síce zníži viditeľnosť škvrny, ale časť pigmentu a mastnoty sa môže rozložiť do väčšej plochy. Výsledkom je sivý závoj, tvrdšie miesto alebo uterák, ktorý po čase pôsobí menej sviežo.",
            ),
            (
                "Ako rozlíšiť pigment, mastnotu a zvyšky odličovača",
                "Ak je miesto tmavé, ale na dotyk rovnaké ako zvyšok uteráka, hlavný problém je pigment. Ak je miesto hladké, vodu odpudzuje alebo má tmavšiu mapu po vyschnutí, riešite aj mastný film. Ak sa škvrna objavila po odlíčení olejom alebo balzamom, berte ju ako kombináciu maskary a kozmetického tuku. Vtedy pomáha kratšie namočenie konkrétneho miesta v pracom roztoku a trpezlivé preplachovanie.",
                "Pri froté materiáli sa vyhnite tvrdému kefovaniu. Slučky uteráka sa môžu vytiahnuť a škvrna sa roznesie do strán. Lepšie je pracovať prstami cez textil, miesto pretláčať a oplachovať zo zadnej strany, ak to tvar uteráka umožňuje. Po každom kroku si skontrolujte, či sa škvrna zmenšuje, alebo sa len rozmazáva. Ak sa rozmazáva, používate príliš veľa vody alebo príliš agresívny pohyb.",
            ),
            (
                "Ako nastaviť pranie po predčistení",
                "Po predčistení perte uteráky v dávke, ktorá má v bubne priestor. Froté potrebuje vodu a pohyb, inak sa z neho horšie vyplaví pigment aj prací roztok. Ak pridáte príliš veľa gélu, môžete si vytvoriť nový problém: uterák bude tvrdší, menej savý a na tmavšom mieste zostane film. Pri kozmetických škvrnách preto nie je cieľom silnejšia vôňa, ale čisté vlákno a dobrý oplach.",
                "Ak periete bielu osušku s viacerými uterákmi, skontrolujte škvrnu pred vložením do sušičky alebo pred sušením na radiátore. Teplo vie zvyšky kozmetiky zvýrazniť a neskôr budete riešiť starú škvrnu, nie čerstvú. Pri opakovaných problémoch pomôže vyhradiť si jeden menší uterák na odlíčenie a veľké osušky používať až po dôkladnom očistení tváre.",
            ),
        ],
    },
    "greasy_ointment": {
        "marker": "Detailnejší postup pre mastnú masť v textílii",
        "table": {
            "headers": ["Typ masti alebo krému", "Čo robí s vláknom", "Praktické riešenie"],
            "rows": [
                ("ochranná detská masť", "vytvára vodoodpudivý film", "odobrať prebytok, predčistiť a kontrolovať savosť"),
                ("hojivá masť", "často drží v lemoch a švoch", "pred praním ošetriť aj okraje škvrny"),
                ("telové maslo", "zanechá tmavšiu mapu po vyschnutí", "nepoužiť teplo, kým je mapa viditeľná"),
                ("krém z posteľnej bielizne", "prenáša sa na pyžamo aj plachtu", "prať súvisiace kusy spolu alebo postupne skontrolovať všetky"),
            ],
        },
        "sections": [
            (
                "Detailnejší postup pre mastnú masť v textílii",
                "Mastná masť sa v praní nespráva ako bežná škvrna od jedla. Často je navrhnutá tak, aby na pokožke vytvorila ochranný film, odpudzovala vlhkosť a držala dlhšie. Presne tieto vlastnosti potom robia problém na pyžame, uteráku alebo tričku. Ak ju iba vyperiete bez predčistenia, časť filmu ostane na vláknach a po uschnutí sa ukáže ako tmavšia mapa alebo hladší povrch.",
                "Pri takýchto škvrnách rozhoduje poradie krokov. Najprv odobrať prebytok, potom uvoľniť mastný film, potom prať s dostatočným priestorom a až nakoniec sušiť. Ak sa začne sušením alebo horúcou vodou, film sa môže rozliať do okolia a z jedného miesta vznikne väčšia mapa. Pri detských veciach je navyše dôležité skontrolovať lemy, zapínanie a viacvrstvové časti, kde sa masť drží najdlhšie.",
            ),
            (
                "Ako pracovať s uterákmi, pyžamom a tričkom rozdielne",
                "Uterák je savý, preto mastný film spoznáte hlavne podľa toho, že miesto horšie prijíma vodu. Pyžamo býva mäkké a často sa nosí niekoľko hodín, takže masť sa vie dostať hlbšie do vlákna aj do švov. Tričko zase po vyschnutí ukáže mapu najmä pri dennom svetle. Rovnaký produkt preto môže na každom textile vyzerať inak a vyžaduje trochu iný dôraz pri kontrole.",
                "Pri uteráku sledujte savosť, pri pyžame pružné lemy a pri tričku farebnú mapu. Ak je textil po praní na dotyk mastný, nedávajte ho do sušičky ani na radiátor. Zopakujte lokálne predčistenie, prípadne perte v menšej dávke. Preplnená práčka pri mastnote často zlyhá preto, že textil nemá dosť pohybu a mastný film sa z vlákien neoddelí rovnomerne.",
            ),
            (
                "Ako zabrániť opakovaniu škvŕn od masti",
                "Ak masť používate pravidelne, nastavte si jednoduchú domácu rutinu. Po nanesení nechajte produkt chvíľu vsiaknuť, používajte konkrétne pyžamo alebo uterák a posteľnú bielizeň perte skôr, než sa mastnota začne vrstviť. Pri deťoch pomáha oddeliť kusy, ktoré prišli do kontaktu s masťou, od bežnej bielizne. Znížite tým prenos filmu na ďalšie oblečenie.",
                "Dôležité je aj dávkovanie pracieho produktu. Viac gélu automaticky neznamená lepší výsledok. Ak je gélu priveľa, môže zhoršiť oplach a mastný textil bude pôsobiť tvrdšie alebo lepkavejšie. Lepší výsledok dáva presné predčistenie škvrny, primeraná dávka a kontrola pred sušením. Vôňu pridávajte až vtedy, keď je film preč, inak iba prekryje problém, ktorý sa po nosení vráti.",
            ),
        ],
    },
    "viscose_blouse": {
        "marker": "Detailnejší postup pre viskózovú blúzku po praní",
        "table": {
            "headers": ["Problém po praní", "Pravdepodobná príčina", "Čo urobiť nabudúce"],
            "rows": [
                ("blúzka je dlhšia", "mokrá viskóza visela pod vlastnou váhou", "sušiť naležato alebo tvarovať bez ťahu"),
                ("švy sa krútia", "trenie v bubne alebo silné odstreďovanie", "prať v menšej dávke a znížiť otáčky"),
                ("látka je veľmi pokrčená", "preplnený bubon a dlhé státie po praní", "vybrať hneď a vyhladiť rukami"),
                ("povrch je matný alebo drsný", "nevhodné trenie alebo teplo", "prať jemnejšie a žehliť podľa štítku"),
            ],
        },
        "sections": [
            (
                "Detailnejší postup pre viskózovú blúzku po praní",
                "Viskóza je príjemná práve preto, že je splývavá a mäkká, ale za mokra je menej stabilná. Vlákno prijme vodu, blúzka oťažie a pri nesprávnom pohybe sa ľahšie vytiahne. Preto sa pri nej nerieši iba teplota prania, ale aj to, ako plný je bubon, aké silné je odstreďovanie a čo s blúzkou urobíte v prvých minútach po praní.",
                "Najčastejšia chyba je zavesiť mokrú viskózu na úzky vešiak. Voda ťahá materiál nadol, ramená sa môžu vytiahnuť a spodný lem zmení líniu. Druhá častá chyba je krútenie v rukách, ktoré síce odstráni vodu, ale zároveň namáha švy. Pri viskóze je lepšie vodu jemne vytlačiť do uteráka, blúzku vyrovnať a sušiť bez ťahu.",
            ),
            (
                "Ako chrániť tvar, švy a povrch",
                "Pred praním zapnite gombíky, otočte blúzku podľa potreby naruby a neperte ju s ťažkými uterákmi alebo džínsami. Ťažké kusy spôsobujú v bubne ťah a trenie, ktoré jemná blúzka nepotrebuje. Ak má viskózová blúzka čipku, volán, viazanie alebo tenšie ramienka, prací vak môže znížiť mechanické namáhanie.",
                "Po praní blúzku nenechávajte dlho pokrčenú v práčke. Viskóza si vie zapamätať záhyby a potom vyzerá horšie aj po vyžehlení. Vyberte ju hneď, vyhlaďte rukami a položte tak, aby švy išli prirodzene. Ak sa zdá, že sa blúzka trochu zrazila alebo natiahla, často pomôže jemné tvarovanie za vlhka, nie silové ťahanie.",
            ),
            (
                "Ako riešiť vôňu pri jemnej blúzke",
                "Pri viskóze je vôňa príjemný doplnok, nie riešenie nesprávneho prania. Ak blúzka po praní zapácha, najprv skontrolujte, či nestála mokrá v bubne, či nebola praná v preplnenej dávke a či sa dobre vysušila. Jemné materiály môžu zachytiť pach aj vtedy, keď sa sušia príliš pomaly v slabo vetranej miestnosti.",
                "Vôňu testujte opatrne, najmä ak ide o blúzku nosenú priamo pri pokožke. Začnite menšou intenzitou a sledujte, či sa vôňa s materiálom správa príjemne aj po uschnutí. Pri drahších alebo citlivých kúskoch je rozumnejšie skúsiť vôňu najprv na menej dôležitom textile alebo použiť vzorku. Základom však ostáva šetrné pranie a sušenie bez deformácie.",
            ),
        ],
    },
    "acrylic_paint": {
        "marker": "Detailnejší postup pre akrylovú farbu na oblečení",
        "table": {
            "headers": ["Kedy škvrnu riešite", "Čo sa deje s farbou", "Najbezpečnejší smer"],
            "rows": [
                ("hneď po zašpinení", "farba je ešte vodou riediteľná", "odstrániť prebytok a preplachovať zozadu"),
                ("po niekoľkých hodinách", "film začína tuhnúť", "uvoľňovať postupne, bez tepla"),
                ("po úplnom zaschnutí", "farba drží ako povlak", "opatrne odstrániť vrch a predčistiť"),
                ("po vypraní a sušení", "zvyšky sa mohli spevniť", "nežehliť, skúsiť opakované jemné kroky"),
            ],
        },
        "sections": [
            (
                "Detailnejší postup pre akrylovú farbu na oblečení",
                "Akrylová farba je zradná tým, že v čerstvom stave pôsobí pomerne nevinne, ale po zaschnutí vytvorí pružný film. Ten sa nechová ako bežná špina, ktorú práčka jednoducho vyplaví. Pri tričku, mikine alebo detskej zástere preto rozhoduje rýchlosť zásahu. Čím skôr farbu z povrchu odstránite a prepláchnete zo zadnej strany, tým väčšia šanca, že sa nedostane hlboko do vlákna.",
                "Najhoršie je farbu rozotrieť do strán a potom použiť teplo. Horúca voda, radiátor, sušička alebo žehlička môžu zvyšky spevniť. Ak je farba ešte mokrá, postupujte trpezlivo: odobrať prebytok, preplachovať zozadu, lokálne predčistiť a prať podľa štítku. Ak je farba suchá, najprv uvoľnite vrchný film, ale bez ostrého škrabania, ktoré by poškodilo tkaninu.",
            ),
            (
                "Ako chrániť farbu trička a potlač",
                "Pri farebnom tričku alebo potlači neriešite iba akrylovú farbu, ale aj stálosť pôvodného textilu. Agresívne drhnutie môže síce zmenšiť škvrnu, ale zároveň vytvorí svetlejší kruh alebo poškodí obrázok. Preto je lepšie testovať na malej časti, pracovať od okraja škvrny ku stredu a priebežne oplachovať. Pri potlači nikdy netrite tvrdou kefou cez obrázok.",
                "Ak je škvrna na tenkej bavlne, dajte pod látku čistú savú vrstvu a pracujte jemne. Pri mikine alebo hrubšej teplákovine môže farba držať v štruktúre dlhšie, takže jeden cyklus nestačí. Dôležité je nevyhodnotiť výsledok až po sušení. Kontrola musí prísť ešte pred teplom, keď je možné postup zopakovať bez ďalšieho zafixovania.",
            ),
            (
                "Ako nastaviť prevenciu pri detskom tvorení",
                "Ak sa doma často maľuje, najpraktickejšie je mať samostatné tvorivé tričko alebo zásteru. Pri bežnom oblečení pomáha skontrolovať rukávy a spodný lem hneď po dokončení tvorenia, nie až večer pri triedení bielizne. Akrylová farba zaschne rýchlo a potom sa z jednoduchej úlohy stane záchrana staršej škvrny.",
                "Pri praní takto zašpinených vecí nepreplňte bubon. Textil potrebuje vodu a pohyb, aby sa zvyšky farby a pracieho roztoku vyplavili. Ak po praní ostane tvrdý okraj, znamená to, že časť filmu stále drží. Vtedy je lepšie zopakovať lokálne ošetrenie než pridávať vôňu alebo sušiť teplom. Vôňa má zmysel až na čistom textile.",
            ),
        ],
    },
    "softshell_impregnation": {
        "marker": "Detailnejší postup pre softshell a impregnáciu po praní",
        "table": {
            "headers": ["Test alebo prejav", "Ako ho čítať", "Rozumný ďalší krok"],
            "rows": [
                ("kvapka vody sa drží na povrchu", "vodoodpudivá úprava stále pracuje", "impregnáciu nepridávať zbytočne"),
                ("voda sa vpije iba na špinavých miestach", "problém môže byť nečistota", "najprv šetrne vyprať a vysušiť"),
                ("celý povrch rýchlo tmavne", "úprava môže byť oslabená", "po kontrole štítku zvážiť obnovu"),
                ("bunda je po praní ťažká alebo lepkavá", "môže ísť o zvyšky produktu", "pridať oplach a znížiť dávkovanie nabudúce"),
            ],
        },
        "sections": [
            (
                "Detailnejší postup pre softshell a impregnáciu po praní",
                "Softshell nie je jeden univerzálny materiál. Môže ísť o jednoduchšiu mestskú bundu, nohavice na turistiku alebo technickejší kus s membránou. Preto je dôležité nebrať impregnáciu ako automatický krok po každom praní. Najprv treba zistiť, či je povrch naozaj bez vodoodpudivej úpravy, alebo je iba zanesený špinou, potom, zvyškami gélu alebo nevhodným avivážnym filmom.",
                "Test kvapiek má zmysel robiť až na čistom a suchom materiáli. Ak ho spravíte na zablatenom kolene alebo mastnom golieri, výsledok nebude spoľahlivý. Špina vie vodu vtiahnuť do povrchu aj vtedy, keď zvyšok bundy ešte funguje. Preto najprv správne vyperte, dobre opláchnite, vysušte podľa štítku a až potom hodnotíte, či impregnácia potrebuje obnovu.",
            ),
            (
                "Ako oddeliť problém prania od problému impregnácie",
                "Ak softshell po praní pôsobí ťažko, lepkavo alebo zvláštne hladko, príčinou nemusí byť slabá impregnácia. Často ide o zvyšky pracieho prostriedku, priveľkú dávku, preplnený bubon alebo nevhodnú aviváž. V takom prípade ďalší impregnačný prípravok problém skôr prekryje a pridá ďalšiu vrstvu na povrch. Lepší krok je čistý oplach a úprava pracej rutiny.",
                "Ak sa voda vpíja rovnomerne po celom povrchu aj po správnom praní, obnova môže dávať zmysel. Stále však platí, že treba rešpektovať štítok a odporúčanie výrobcu. Funkčné materiály môžu mať rôzne vrstvy a nie každý prípravok je vhodný na každý typ. Pri bunde s membránou je opatrnosť dôležitejšia než rýchly efekt.",
            ),
            (
                "Ako sa o softshell starať medzi praniami",
                "Softshell po nosení nenechávajte dlho vlhký v batohu alebo v aute. Pot a vlhkosť zhoršujú pach, špina sa drží pri golieri, manžetách a spodnom leme a povrch potom pri teste kvapiek vyzerá horšie, než v skutočnosti je. Po výlete nechajte bundu preschnúť, vytraste blato a väčšie nečistoty riešte skôr, než zaschnú do vrstiev.",
                "Pri sezónnom odkladaní musí byť bunda suchá a čistá. Neskladujte ju stlačenú v taške s vlhkými vecami. Ak chcete pridať vôňu, robte to veľmi jemne a mimo funkčných vrstiev, pretože softshell sa nosí pri pohybe a intenzívna aróma môže byť pri zahriatí nepríjemná. Hlavným cieľom je funkčný, čistý a dobre vysušený materiál.",
            ),
        ],
    },
}


def build_deepening(topic):
    config = DEEPENINGS[topic]
    sections_html = "\n".join(
        f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>"
        for title, first, second in config["sections"]
    )
    return clean(
        f"""
        {sections_html}
        <h2>Kontrolná tabuľka pred ďalším krokom</h2>
        {table(config["table"]["headers"], config["table"]["rows"])}
        """
    )


BOOSTS = {
    "viscose_blouse": clean(
        """
        <h2>Čo robiť, keď sa viskózová blúzka po praní už vytiahla</h2>
        <p>Ak sa blúzka po jednom praní vytiahla, netreba ju hneď považovať za zničenú. Najprv ju jemne navlhčite, položte na uterák a rukami upravte ramená, bočné švy a spodný lem do pôvodnej línie. Nerobte prudké ťahanie proti smeru deformácie. Viskóza reaguje lepšie na pomalé tvarovanie, rovnomernú podporu a sušenie bez vlastnej váhy než na silové naťahovanie.</p>
        <p>Ak je blúzka skrátená, príčinou môže byť aj teplo alebo sušenie, nie iba samotné pranie. Nabudúce preto skráťte čas v bubne, znížte otáčky a blúzku vyberte hneď po skončení programu. Pri drahšom kúsku si poznačte, ktorý program fungoval dobre. Tak si vytvoríte vlastnú rutinu pre konkrétnu blúzku a nebudete zakaždým riskovať iný výsledok.</p>
        """
    ),
    "acrylic_paint": clean(
        """
        <h2>Čo robiť, keď po praní zostal tvrdý okraj farby</h2>
        <p>Tvrdý okraj po praní znamená, že časť akrylovej farby ostala ako film na povrchu alebo vo vlákne. V tejto fáze nepomôže pridať vôňu ani textil iba znovu vyprať bez prípravy. Najprv miesto namočte lokálne, nechajte ho zmäknúť a skúste uvoľniť okraj prstami alebo mäkkou handričkou. Cieľom je zmenšiť film bez toho, aby ste poškodili pôvodnú tkaninu.</p>
        <p>Ak je tvrdý okraj na detskom tričku, realisticky počítajte s tým, že výsledok nemusí byť dokonale neviditeľný. Dôležité je zachrániť nositeľnosť a zabrániť tomu, aby sa zvyšky farby ďalej lámali alebo dráždili pokožku. Pri ďalšom tvorení pomôže staršie tričko, zástera a rýchla kontrola rukávov ešte pred tým, než farba úplne zaschne.</p>
        """
    ),
}


def build_expansion(topic):
    config = TOPICS[topic]
    sections_html = "\n".join(
        f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>"
        for title, first, second in config["sections"]
    )
    faq_html = "\n".join(f"<h3>{question}</h3>\n<p>{answer}</p>" for question, answer in config["faq"])
    box_title, box_text = config["box"]
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["intro"][0]}</p>
        <p>{config["intro"][1]}</p>
        {note_card("Rýchla diagnostika pred praním", config["bullets"])}
        <h2>Praktická tabuľka podľa situácie</h2>
        {table(config["table"]["headers"], config["table"]["rows"])}
        {sections_html}
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">{box_title}</h2>
        <p>{box_text}</p>
        </div>
        {product_card(config["product_kind"])}
        <h2>FAQ: praktické otázky</h2>
        {faq_html}
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
        return long
    index = insertion_index(long)
    return long[:index].rstrip() + "\n" + EXPANSIONS[key] + "\n" + long[index:].lstrip()


def insert_deepening(long, key):
    if DEEPENINGS[key]["marker"] in long:
        return long
    anchor = long.find('<div style="border: 1px solid #dbe5de', long.find(MARKERS[key]))
    if anchor == -1:
        anchor = insertion_index(long)
    return long[:anchor].rstrip() + "\n" + build_deepening(key) + "\n" + long[anchor:].lstrip()


def insert_boost(long, key):
    boost = BOOSTS.get(key)
    if not boost or boost in long:
        return long
    anchor = long.find('<div style="border: 1px solid #dbe5de', long.find(MARKERS[key]))
    if anchor == -1:
        anchor = insertion_index(long)
    return long[:anchor].rstrip() + "\n" + boost + "\n" + long[anchor:].lstrip()


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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 09 material and cosmetic articles.")
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
            original_title = article.get("title")
            original_short = article.get("short", "")
            original_url = article.get("url")
            original_long = article["long"]
            article["long"] = insert_boost(
                insert_deepening(insert_expansion(article["long"], config["topic"]), config["topic"]),
                config["topic"],
            )
            if article.get("title") != original_title or article_slug(article) != config["slug"] or article.get("short", "") != original_short:
                raise SystemExit(f"Retrofit attempted to change title, slug, or short field for {config['slug']}")
            if original_url and article.get("url") != original_url:
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
                "wave": "retrofit-wave-09-material-cosmetic-five",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "purpose": "Conservative additive expansion. Titles, slugs, URLs, and short descriptions are preserved.",
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
                "wave": "retrofit-wave-09-material-cosmetic-five",
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
