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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-10-functional-materials-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-10-functional-materials-five-2026-06-16-mcp-results.json"

LEGACY_LINK_REPLACEMENTS = {
    "/n/modal-v-obleceni-co-znamena-preco-je-makky-a-ako-ho-prat": "/n/modal-v-obleceni-co-znamena-preco-je-maekky-a-ako-ho-prat",
}


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany",
        "post_id": "2125",
        "url": "https://www.vevo.sk/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany",
        "topic": "softshell_washing",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-21-2026-06-10-articles.json",
        "slug": "co-je-lyocell-alebo-tencel-priedusnost-jemnost-a-starostlivost",
        "post_id": "2234",
        "url": "https://www.vevo.sk/n/co-je-lyocell-alebo-tencel-priedusnost-jemnost-a-starostlivost",
        "topic": "lyocell_tencel",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-21-2026-06-10-articles.json",
        "slug": "co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie",
        "post_id": "2231",
        "url": "https://www.vevo.sk/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie",
        "topic": "polyamide_nylon",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-21-2026-06-10-articles.json",
        "slug": "modal-v-obleceni-co-znamena-preco-je-maekky-a-ako-ho-prat",
        "legacy_source_slug": "modal-v-obleceni-co-znamena-preco-je-makky-a-ako-ho-prat",
        "post_id": "2233",
        "url": "https://www.vevo.sk/n/modal-v-obleceni-co-znamena-preco-je-maekky-a-ako-ho-prat",
        "topic": "modal_clothing",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-21-2026-06-10-articles.json",
        "slug": "polyamid-vs-polyester-ktory-material-lepsie-znasa-pot-sport-a-caste-pranie",
        "post_id": "2232",
        "url": "https://www.vevo.sk/n/polyamid-vs-polyester-ktory-material-lepsie-znasa-pot-sport-a-caste-pranie",
        "topic": "polyamide_vs_polyester",
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
            <h2 style="margin-top: 0;">Odporúčané riešenie na opatrné testovanie vône</h2>
            <p>Pri jemných materiáloch a športových kúskoch je lepšie začať nižšou intenzitou. Najprv vyriešte čistotu, oplach a sušenie, až potom dolaďte vôňu podľa toho, ako sa materiál nosí.</p>
            <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
            <h3 style="margin-top: 0;">Vevo Essence Sample Set 9x10ml</h3>
            <p>Vzorkový set je praktický, keď chcete porovnať viac vôní bez veľkého balenia a zistiť, čo sedí bielizni, športu alebo jemným tričkám.</p>
            <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1621/vevo-essence-sample-set">Pozrieť vzorkový set</a></p>
            </div>
            <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vzorky/parfum-do-prania-vzorky">Pozrieť vzorky parfumov do prania</a></p>
            </div>
            """
        )
    return clean(
        """
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie na šetrné pranie materiálov</h2>
        <p>Pri funkčných a jemnejších materiáloch má prací produkt pomôcť odstrániť pot, prach a bežné zvyšky bez toho, aby sa problém len prekryl vôňou.</p>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>Vhodný ako univerzálny základ na bežné pranie. Pri softshelle, športovom oblečení a jemných materiáloch vždy rešpektujte štítok výrobcu.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
        </div>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        """
    )


TOPICS = {
    "softshell_washing": {
        "marker": "Detailnejší postup pri praní softshellu bez poškodenia membrány",
        "product_kind": "laundry",
        "intro": [
            "Softshell bunda alebo nohavice sa neperú ako obyčajná mikina. Materiál má často vodoodpudivú povrchovú úpravu, elastické vlákna a pri niektorých kusoch aj membránu alebo laminovanú vrstvu. Pranie preto musí odstrániť pot, prach a blato, ale nesmie zbytočne zaliať povrch avivážou, preťažiť švy ani uzavrieť materiál zvyškami pracieho produktu.",
            "Najdôležitejšie je rozlišovať medzi špinou, zápachom, stratou vodoodpudivosti a skutočným poškodením. Ak bunda po praní premoká alebo je ťažká, nemusí to automaticky znamenať zničenú membránu. Často ide o zvyšky produktu, slabý oplach, špinu pri lemoch alebo nevhodnú aviváž.",
        ],
        "bullets": [
            "<strong>Pred praním:</strong> zapnite zipsy, vyprázdnite vrecká a odstráňte hrubú špinu.",
            "<strong>Bez aviváže:</strong> zmäkčovadlá môžu zhoršiť funkčný povrch a priedušnosť.",
            "<strong>Menšia dávka:</strong> softshell potrebuje priestor na pohyb a oplach.",
            "<strong>Po praní:</strong> sušte bez horúceho radiátora a až potom hodnotíte impregnáciu.",
        ],
        "tables": [
            {
                "title": "Tabuľka podľa typu softshellu",
                "headers": ["Typ kusu", "Najväčšie riziko", "Rozumný postup"],
                "rows": [
                    ("mestská softshell bunda", "zápach pri golieri a manžetách", "lokálne predčistiť, prať šetrne"),
                    ("turistické nohavice", "blato na kolenách a spodnom leme", "nechať zaschnúť, vytriasť, potom prať"),
                    ("detský softshell", "piesok, jedlo a časté pranie", "kontrolovať vrecká a nepoužiť aviváž"),
                    ("softshell s membránou", "zvyšky gélu a slabý oplach", "menšia dávka, dobré vypláchnutie"),
                ],
            },
            {
                "title": "Kontrola po praní",
                "headers": ["Prejav", "Čo môže znamenať", "Ďalší krok"],
                "rows": [
                    ("materiál je ťažký", "zvyšky vody alebo produktu", "nechať doschnúť, pri potrebe pridať oplach nabudúce"),
                    ("voda sa neperlí", "špina alebo oslabená úprava", "najprv test na čistom suchom mieste"),
                    ("golier stále zapácha", "pot a maz neodišli úplne", "lokálne predčistiť pred ďalším praním"),
                    ("povrch je lepkavý", "veľa produktu alebo nevhodná aviváž", "oplach bez aviváže"),
                ],
            },
        ],
        "sections": [
            ("Ako pripraviť softshell bundu pred praním", "Pred praním zapnite všetky zipsy, vyprázdnite vrecká, uvoľnite sťahovanie a skontrolujte suché zipsy. Hrubé blato nenechávajte rozpúšťať v bubne. Lepšie je nechať ho preschnúť, vytriasť alebo jemne odstrániť kefou. Pri detskom softshelle skontrolujte najmä vrecká, lemy rukávov a spodné časti nohavíc.", "Ak je špinavý hlavne golier alebo manžety, ošetrite lokálne len tieto miesta. Celý kus potom perte v dávke, kde má priestor. Softshell natlačený medzi uteráky alebo džínsy sa horšie opláchne a zvyšky gélu môžu vytvoriť pocit ťažkého, menej priedušného materiálu."),
            ("Ako prať softshell bundu a nohavice v práčke", "Ak štítok povoľuje pranie v práčke, voľte šetrnejší program, nižšie otáčky a primerané množstvo pracieho produktu. Nepoužívajte bežnú aviváž. Pri funkčných textíliách je dôležitejší dobrý oplach než silná vôňa. Ak práčka ponúka extra oplach a bunda je hrubšia, môže to pomôcť odstrániť zvyšky produktu.", "Softshell neperte s textíliami, ktoré púšťajú vlákna. Uteráky, deky alebo flís môžu zanechať jemné chĺpky na povrchu a zhoršiť dojem z materiálu. Pri podobných funkčných textíliách je užitočný aj návod <a href=\"/n/kedy-nepouzivat-avivaz-uteraky-sportove-oblecenie-softshell-aj-detska-bielizen\">kedy nepoužívať aviváž</a>."),
            ("Ako sušiť softshell bez poškodenia", "Po praní softshell vyberte z práčky hneď, vytvarujte ho a nechajte sušiť voľne. Horúci radiátor alebo priame intenzívne teplo môže zhoršiť povrch, švy alebo elastické časti. Ak výrobca povoľuje aktiváciu vodoodpudivej úpravy teplom, riaďte sa konkrétnym štítkom, nie všeobecným odhadom.", "Vlhký softshell môže pôsobiť ťažký a menej funkčný. Hodnotenie robte až po úplnom vysušení. Test kvapiek má zmysel až vtedy, keď je materiál čistý a suchý. Ak voda stále tvorí kvapky, impregnáciu netreba riešiť automaticky."),
            ("Ako riešiť detský softshell", "Detský softshell sa často perie častejšie než dospelá turistická bunda, pretože prichádza do kontaktu s pieskom, blatom, jedlom a mokrou trávou. Práve preto je dôležité nerobiť každé pranie agresívne. Hrubú špinu odstráňte pred praním a nepreťažujte bubon. Časté pranie s avivážou môže postupne zhoršiť správanie povrchu.", "Pri detských nohaviciach skontrolujte kolená, zadnú časť a spodný lem. Ak sa špina drží len lokálne, nemusí byť potrebné hneď prať celý komplet na silnom programe. Pri sezónnej starostlivosti nadväzuje aj článok <a href=\"/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit\">ako obnoviť impregnáciu softshellu</a>."),
            ("Ako spoznať chybu v praní", "Ak softshell po praní zapácha, je lepkavý alebo voda na ňom netvorí kvapky, najprv premýšľajte nad procesom prania. Bola dávka preplnená? Použili ste veľa gélu? Bola pridaná aviváž? Ostal kus dlho mokrý v práčke? Tieto chyby sú častejšie než skutočné poškodenie membrány.", "Pri funkčných bundách sa oplatí viesť jednoduchú rutinu: najprv odstrániť hrubú špinu, potom šetrne prať, dobre opláchnuť, vysušiť a až potom riešiť impregnáciu alebo vôňu. Podobnú logiku má aj pranie membrán v návode <a href=\"/n/ako-prat-gore-tex\">ako prať Gore-Tex</a>."),
        ],
        "box": ("Najdôležitejšie pravidlo", "Softshell najprv vyčistite a vysušte, až potom hodnotíte impregnáciu. Nepridávajte aviváž a nepreplňte bubon."),
        "faq": [
            ("Môže ísť softshell do práčky?", "Áno, ak to povoľuje štítok výrobcu. Použite šetrný program, primerané množstvo produktu a bez aviváže."),
            ("Prečo softshell po praní premoká?", "Môže ísť o špinu, zvyšky pracieho produktu alebo oslabenú povrchovú úpravu. Test robte až na čistom a suchom materiáli."),
            ("Treba softshell impregnovat po každom praní?", "Nie. Obnovu riešte až vtedy, keď čistý a suchý materiál prestane odpudzovať vodu."),
        ],
    },
    "lyocell_tencel": {
        "marker": "Detailnejší postup pre lyocell, Tencel a jemné splývavé oblečenie",
        "product_kind": "samples",
        "intro": [
            "Lyocell, často predávaný aj pod značkou Tencel, je obľúbený pre jemnosť, splývavosť a príjemný pocit na pokožke. V domácnosti ho nájdete v tričkách, blúzkach, šatách, spodnej bielizni aj posteľnej bielizni. Pri praní sa správa kultivovanejšie než niektoré citlivé materiály, ale stále potrebuje šetrnú mechaniku a dobré sušenie.",
            "Najčastejší problém nie je samotné namočenie, ale preplnený bubon, trenie s hrubými textíliami, príliš silné odstreďovanie a sušenie v zlom tvare. Lyocell vie vyzerať luxusne, ale ak ho stlačíte medzi uteráky alebo necháte dlho pokrčený v práčke, po vysušení môže pôsobiť unavene a pokrčene.",
        ],
        "bullets": [
            "<strong>Trieďte podľa jemnosti:</strong> neperte lyocell s hrubými uterákmi alebo zipsami.",
            "<strong>Nezabudnite na tvar:</strong> po praní vyhladiť a sušiť bez silného ťahu.",
            "<strong>Vôňu testujte jemne:</strong> materiál je blízko pokožky a nosí sa často v teple.",
            "<strong>Pri posteľnej bielizni:</strong> sledujte hladkosť, záhyby a dôkladné vysušenie.",
        ],
        "tables": [
            {
                "title": "Kde sa lyocell najčastejšie používa",
                "headers": ["Kus", "Prečo je príjemný", "Čo strážiť pri praní"],
                "rows": [
                    ("tričko alebo top", "mäkký dotyk a priedušnosť", "nepreplniť bubon, sušiť bez ťahu"),
                    ("blúzka alebo šaty", "splývavý vzhľad", "nižšie otáčky a rýchle vybratie z práčky"),
                    ("posteľná bielizeň", "hladký dotyk na pokožke", "prať s priestorom a dobre vysušiť"),
                    ("spodná bielizeň", "jemnosť pri nosení", "triediť od drsných materiálov"),
                ],
            },
            {
                "title": "Kontrola po praní",
                "headers": ["Prejav", "Možná príčina", "Praktická oprava"],
                "rows": [
                    ("silné pokrčenie", "dlhé státie v práčke", "vybrať hneď a vyhladiť rukami"),
                    ("vytiahnutý lem", "mokré vešanie pod váhou", "sušiť s oporou a tvarovať"),
                    ("matný povrch", "trenie s hrubými kusmi", "prať oddelene alebo vo vrecku"),
                    ("slabá vôňa po praní", "vlhkosť ostala v záhyboch", "dobre dosušiť a vetrať"),
                ],
            },
        ],
        "sections": [
            ("Ako prať lyocell a Tencel bez unaveného vzhľadu", "Lyocell perte v menšej dávke s podobne jemnými kúskami. Hrubé uteráky, džínsovina, kovové zipsy a suché zipsy môžu povrch zbytočne namáhať. Ak ide o blúzku, top alebo jemnejšie šaty, prací vak pomôže znížiť trenie a zachovať hladší povrch.", "Po praní materiál nenechávajte dlho pokrčený v bubne. Vyberte ho, jemne pretrepte, vyhlaďte švy a sušte tak, aby sa textil nedeformoval. Ak sa podobá viskóze, môže pomôcť aj návod <a href=\"/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost\">čo je viskóza a ako sa o ňu starať</a>."),
            ("Ako sušiť lyocellové tričko, šaty a posteľnú bielizeň", "Pri tričku a šatách je dôležité zabrániť ťahu za mokra. Materiál vyhlaďte rukami a sušte voľne, nie na úzkom vešiaku, ktorý by vytvoril ostré ramená. Pri posteľnej bielizni sledujte, aby sa veľký kus nezamotal a nedržal vlhkosť v záhyboch.", "Ak lyocell sušíte príliš pomaly v nevetranej miestnosti, môže získať zatuchnutý nádych aj po správnom praní. Vôňa tento problém nevyrieši. Najprv treba dobré vyžmýkanie podľa štítku, voľné rozloženie a prúdenie vzduchu."),
            ("Ako pristupovať k vôni pri lyocelli", "Lyocell sa často nosí priamo na pokožke, preto je lepšie začať s jemnejšou intenzitou vône. Príliš ťažká vôňa môže na mäkkom tričku alebo obliečke pôsobiť silnejšie, než čakáte. Ak si nie ste istí, skúšajte vône postupne a na menej citlivých kusoch.", "Pri posteľnej bielizni je dôležitý aj komfort počas spánku. Vôňa má byť príjemná po priblížení, nie rušivá celú noc. Praktické je najprv zvládnuť čistotu a sušenie a až potom dolaďovať parfum do prania alebo vzorky vôní."),
            ("Ako žehliť alebo naparovať lyocell", "Lyocell sa môže krčiť podobne ako iné celulózové materiály. Pri žehlení sa riaďte štítkom a skúšajte nižšiu teplotu z rubu. Ak je materiál veľmi jemný, bezpečnejšie je naparovanie alebo žehlenie cez tenkú látku. Cieľom je uhladiť záhyby, nie materiál presušiť vysokým teplom.", "Najlepšia prevencia pokrčenia je správny koniec prania: nepreplnený bubon, rýchle vybratie a vyhladenie. Ak čakáte až na suchý, silno pokrčený kus, budete potrebovať viac tepla a mechaniky, čo jemnému povrchu nepomáha."),
            ("Ako zaradiť lyocell do domácej rutiny", "Lyocell sa hodí do skupiny jemných materiálov spolu s modalom, viskózou a niektorými hladkými bavlnenými zmesami. Ak si pripravíte samostatnú jemnú dávku, znížite trenie a zlepšíte výsledok. Pri praní väčších kusov, ako sú obliečky, sledujte priestor v bubne a rovnomerné sušenie.", "Ak riešite výber medzi mäkkými materiálmi, nadväzuje článok <a href=\"/n/modal-v-obleceni-co-znamena-preco-je-maekky-a-ako-ho-prat\">modal v oblečení</a>. Pri kombinovaní pracieho gélu a vône pomôže aj návod <a href=\"/n/ako-kombinovat-praci-gel-a-parfum-do-prania\">ako kombinovať prací gél a parfum do prania</a>."),
        ],
        "box": ("Rýchla zásada", "Lyocell perte s podobne jemnými kúskami, nenechávajte ho mokrý pokrčený a vôňu testujte opatrne."),
        "faq": [
            ("Je lyocell to isté ako Tencel?", "Tencel je značka, pod ktorou sa predávajú niektoré lyocellové vlákna. Pri praní vždy rozhoduje štítok konkrétneho výrobku."),
            ("Môže sa lyocell zraziť?", "Pri nevhodnom teple alebo zlom sušení môže zmeniť tvar. Najbezpečnejšie je šetrné pranie a sušenie bez ťahu."),
            ("Ako dodať lyocellu vôňu?", "Najprv musí byť dobre vypraný a vysušený. Vôňu skúšajte v menšej intenzite, najmä pri tričkách a posteľnej bielizni."),
        ],
    },
    "polyamide_nylon": {
        "marker": "Detailnejší postup pre polyamid, nylon a časté pranie",
        "product_kind": "laundry",
        "intro": [
            "Polyamid, často známy aj ako nylon, je pevné syntetické vlákno používané v športovom oblečení, pančuchách, plavkách, bundách, podšívkach a zmesových textíliách. Je odolný a pružný, ale pri praní treba dávať pozor na teplo, trenie, zápach potu a zvyšky pracieho produktu.",
            "Najväčšia výhoda polyamidu je praktickosť. Rýchlo schne, býva ľahký a dobre znáša pohyb. Nevýhodou môže byť zachytávanie pachov pri športe, citlivosť niektorých jemných úpletov na zatrhnutie a zmena pružnosti pri nevhodnom teple. Preto nestačí povedať, že polyamid je odolný. Treba vedieť, aký kus periete.",
        ],
        "bullets": [
            "<strong>Športové veci:</strong> perte skôr, nenechávajte ich dlho vlhké v taške.",
            "<strong>Jemný nylon:</strong> chráňte pred zipsami, suchým zipsom a hrubými švami.",
            "<strong>Plavky:</strong> po chlóre a soli najprv opláchnuť čistou vodou.",
            "<strong>Teplo:</strong> rešpektujte štítok, aby sa nezhoršila pružnosť.",
        ],
        "tables": [
            {
                "title": "Polyamid podľa typu výrobku",
                "headers": ["Výrobok", "Typické riziko", "Ako prať rozumne"],
                "rows": [
                    ("športové tričko", "pot a zápach", "prať skoro a dobre vysušiť"),
                    ("legíny alebo spodná vrstva", "zvyšky potu v elastane", "nepreplniť bubon, bez aviváže pri funkčných kusoch"),
                    ("pančuchy", "zatrhnutie", "použiť ochranné vrecko"),
                    ("plavky", "chlór, soľ a opaľovací krém", "opláchnuť hneď po použití"),
                ],
            },
            {
                "title": "Kontrola výsledku",
                "headers": ["Prejav", "Možná príčina", "Čo upraviť"],
                "rows": [
                    ("zápach po praní", "pot ostal vo vlákne alebo v švoch", "prať skôr, nepreplniť dávku"),
                    ("lepkavý dotyk", "zvyšky produktu alebo aviváže", "pridať oplach, znížiť dávku"),
                    ("zatrhnuté vlákna", "kontakt so zipsom alebo suchým zipsom", "prať vo vrecku a triediť"),
                    ("strata pružnosti", "teplo alebo nevhodné sušenie", "sušiť podľa štítku, nie na radiátore"),
                ],
            },
        ],
        "sections": [
            ("Ako prať polyamidové športové oblečenie", "Športový polyamid perte čo najskôr po nosení. Ak zostane vlhký v taške, pot a baktérie sa držia v švoch, gume a elastických častiach. Pred praním ho vyvetrajte alebo nechajte preschnúť, ak práčku nepúšťate hneď. Potom perte v dávke, kde sa oblečenie môže voľne pohybovať.", "Pri funkčných kusoch vynechajte aviváž, ak ju výrobca neodporúča. Môže vytvoriť film, ktorý zhorší pocit pri nosení a niekedy aj odvod vlhkosti. S dávkovaním pracieho gélu pomôže návod <a href=\"/n/ako-davkovat-praci-gel-podla-tvrdosti-vody-naplne-a-znecistenia\">ako dávkovať prací gél</a>."),
            ("Ako prať nylonové pančuchy, jemné prádlo a podšívky", "Jemný nylon je pevný na ťah, ale ľahko sa zatrhne o kovový zips, suchý zips alebo drsný šev. Pančuchy, jemné tielka a tenké podšívky perte v ochrannom vrecku a oddelene od hrubých vecí. Ak sa na povrchu objaví zatrhnutie, mechanické trenie ho pri ďalšom praní zväčší.", "Pri podšívkach búnd sledujte aj hlavný materiál. Niekedy je nylonová len vnútorná vrstva, zatiaľ čo vonkajšok má inú úpravu. Vtedy sa riaďte najcitlivejšou časťou celého výrobku, nie iba tým, že v zložení vidíte polyamid."),
            ("Ako sa starať o polyamidové plavky", "Plavky z polyamidu často trpia viac mimo práčky než v nej. Chlór, soľ, opaľovací krém a mokré odkladanie v taške môžu zhoršiť pružnosť aj vôňu. Po kúpaní ich opláchnite čistou vodou a nenechávajte ich zrolované vo vlhkom uteráku. Pranie robte podľa štítku a bez horúceho sušenia.", "Ak sa plavky lepia alebo zapáchajú, problém môže byť kombinácia opaľovacieho krému, potu a chlóru. Vtedy nepomôže iba silnejšia vôňa. Najprv treba odstrániť zvyšky z vlákna a plavky úplne vysušiť."),
            ("Ako riešiť zápach v polyamide", "Polyamid môže pri športe držať zápach najmä v miestach, kde je elastan, guma alebo hrubší šev. Ak oblečenie po praní stále zapácha, skontrolujte veľkosť dávky, dávkovanie gélu a čas, ktorý oblečenie strávilo vlhké pred praním. Príliš veľa gélu môže zhoršiť oplach a pach sa vráti po zahriatí tela.", "Súvisiace postupy nájdete pri športových textíliách, napríklad v článku <a href=\"/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu\">ako odstrániť zápach z bežeckých legín</a>. Pri porovnaní s polyesterom pomôže aj článok <a href=\"/n/polyamid-vs-polyester-ktory-material-lepsie-znasa-pot-sport-a-caste-pranie\">polyamid vs polyester</a>."),
            ("Ako sušiť polyamid bez zmeny tvaru", "Polyamid zvyčajne schne rýchlo, takže nepotrebuje agresívne teplo. Horúci radiátor alebo sušička mimo odporúčania štítku môže zhoršiť pružnosť, lepidlá alebo elastické časti. Oblečenie radšej vyhlaďte a nechajte sušiť voľne s prúdením vzduchu.", "Ak ide o ľahké športové kúsky, úplné vysušenie je dôležité aj pre vôňu. Vlhké skladovanie vytvorí pach, ktorý sa potom pri ďalšom nosení vráti rýchlejšie. Vôňa do prania má zmysel až na čistom, dobre vypláchnutom a suchom textile."),
        ],
        "box": ("Rýchla zásada", "Polyamid je odolný, ale nie nezničiteľný. Chráňte ho pred teplom, zatrhnutím a vlhkým skladovaním po športe."),
        "faq": [
            ("Je polyamid to isté ako nylon?", "V bežnom používaní sa nylon označuje ako druh polyamidu. Pri praní rozhoduje konkrétny výrobok a štítok."),
            ("Prečo polyamid po praní zapácha?", "Často zostal pot v švoch alebo bol bubon preplnený. Pomáha skoré pranie, primerané dávkovanie a dobré sušenie."),
            ("Môžem použiť aviváž?", "Pri funkčnom športovom oblečení radšej nie, ak ju výrobca neodporúča. Môže zanechať film."),
        ],
    },
    "modal_clothing": {
        "marker": "Detailnejší postup pre modalové oblečenie a mäkký dotyk",
        "product_kind": "samples",
        "intro": [
            "Modal je mäkký celulózový materiál, ktorý sa často používa na tričká, spodnú bielizeň, pyžamá, domáce oblečenie a jemné zmesi. Ľudia ho majú radi, pretože pôsobí hladko, príjemne a menej hrubo než niektoré bežné materiály. Práve mäkkosť je však dôvod, prečo treba pri praní obmedziť zbytočné trenie a silné sušenie.",
            "Modal väčšinou zvláda bežné nosenie dobre, no môže stratiť pekný dotyk, ak ho periete s drsnými uterákmi, preplníte bubon alebo necháte vlhký kus dlho pokrčený. Pri pyžame a spodnej bielizni sa navyše rieši vôňa, pot a kontakt s pokožkou, takže čistota je dôležitejšia než iba intenzívna parfumácia.",
        ],
        "bullets": [
            "<strong>Najlepšie s jemnými vecami:</strong> modal nedávajte k uterákom a zipsom.",
            "<strong>Po praní vyhladiť:</strong> nenechávať mokrý v bubne.",
            "<strong>Pri pyžame:</strong> sledujte pot, telové krémy a dôkladné sušenie.",
            "<strong>Vôňu voliť jemne:</strong> materiál sa nosí blízko pokožky.",
        ],
        "tables": [
            {
                "title": "Modal podľa typu oblečenia",
                "headers": ["Kus", "Prečo je modal vhodný", "Čo strážiť"],
                "rows": [
                    ("spodná bielizeň", "mäkkosť pri kontakte s pokožkou", "šetrné pranie a dobrý oplach"),
                    ("pyžamo", "príjemný dotyk počas noci", "pot, krémy a úplné vysušenie"),
                    ("tričko", "hladký splývavý vzhľad", "nevešať mokré pod ťahom"),
                    ("domáce oblečenie", "komfort pri častom nosení", "prať pravidelne, bez preplnenia"),
                ],
            },
            {
                "title": "Najčastejšie chyby",
                "headers": ["Chyba", "Dôsledok", "Lepší postup"],
                "rows": [
                    ("pranie s uterákmi", "zbytočné trenie a chĺpky", "prať s jemnými kusmi"),
                    ("dlhé státie v práčke", "pokrčenie a zatuchnutie", "vybrať hneď po praní"),
                    ("silné teplo", "horší dotyk alebo zmena tvaru", "sušiť podľa štítku"),
                    ("priveľa vône", "rušivý pocit pri pokožke", "testovať nižšiu intenzitu"),
                ],
            },
        ],
        "sections": [
            ("Ako prať modalové tričko, pyžamo a spodnú bielizeň", "Modalové kúsky perte s podobne jemnými textíliami. Ak ich hodíte k uterákom, rifliam alebo oblečeniu so zipsami, povrch dostane viac trenia, než potrebuje. Pri spodnej bielizni a pyžame je dôležitý dobrý oplach, pretože materiál sa nosí priamo na pokožke.", "Ak je modal v zmesi s elastanom, rešpektujte aj elastickú časť. Silné teplo alebo agresívne sušenie môže zhoršiť pružnosť. Pri pochybnostiach voľte nižšie otáčky, menšiu dávku a sušenie voľne na vzduchu."),
            ("Ako zachovať mäkkosť modalu", "Mäkkosť modalu nevzniká tým, že pridáte veľa zmäkčovadla. Často ju najlepšie zachováte tým, že materiál zbytočne nedrhnete, dobre ho vypláchnete a nenecháte ho preschnúť do tvrdých záhybov. Príliš veľa produktu môže naopak vytvoriť film a materiál bude pôsobiť menej prirodzene.", "Ak modal po praní nepôsobí tak príjemne ako predtým, skontrolujte dávkovanie, tvrdosť vody, veľkosť náplne a spôsob sušenia. Pri všeobecnom výbere pracieho produktu pomôže článok <a href=\"/n/ako-vybrat-praci-gel-podla-typu-bielizne\">ako vybrať prací gél podľa typu bielizne</a>."),
            ("Ako riešiť pot, krémy a telové oleje", "Pyžamo a domáce oblečenie z modalu často prichádza do kontaktu s potom, telovým mliekom alebo krémom. Ak sa na materiáli objaví hladší film alebo zatuchnutý pach, nejde len o vôňu. Treba odstrániť zvyšky z vlákna, dobre opláchnuť a sušiť tak, aby vlhkosť nezostala v švoch.", "Pri mastnejších zvyškoch pomáha lokálne predčistenie konkrétneho miesta, nie automaticky silnejší program. Modal síce pôsobí prakticky, ale stále je to jemnejší materiál. Pri súvisiacich mastných škvrnách nadväzuje návod <a href=\"/n/ako-odstranit-mastnu-mast-z-uteraka-pyzama-a-tricka\">ako odstrániť mastnú masť z pyžama a trička</a>."),
            ("Ako sušiť modal bez vytiahnutia", "Mokrý modal zbytočne nenaťahujte. Tričká a pyžamá po praní pretrepte, vyhlaďte rukami a sušte bez ťahu. Ak je kus veľmi mokrý a zavesíte ho na úzky vešiak, ramená alebo spodný lem sa môžu vytiahnuť. Jemnejšie kúsky je lepšie sušiť s väčšou oporou.", "Nechávajte medzi kusmi priestor. Modal schne lepšie, keď okolo neho prúdi vzduch. Ak schne príliš pomaly v malej nevetranej miestnosti, môže získať zatuchnutý nádych, ktorý potom vôňa iba prekryje."),
            ("Modal, lyocell a viskóza v jednej domácnosti", "Modal, lyocell a viskóza patria medzi materiály, ktoré sa často nosia pre mäkkosť a splývavosť. Nie sú rovnaké, ale v domácej rutine im prospieva podobný prístup: jemná dávka, primerané otáčky, vybratie hneď po praní a sušenie bez ťahu. Tým si zachováte tvar aj dotyk.", "Ak porovnávate materiály, užitočný je článok <a href=\"/n/co-je-lyocell-alebo-tencel-priedusnost-jemnost-a-starostlivost\">čo je lyocell alebo Tencel</a>. Pri vôňach začnite opatrne a testujte na menšom množstve bielizne, pretože mäkké materiály sa nosia blízko pokožky."),
        ],
        "box": ("Rýchla zásada", "Modal perte ako jemnejší materiál: menej trenia, dobrý oplach, rýchle vybratie z práčky a vôňa skôr jemná než ťažká."),
        "faq": [
            ("Je modal vhodný do práčky?", "Väčšinou áno, ak to povoľuje štítok. Perte ho s jemnými kusmi a bez zbytočného trenia."),
            ("Prečo modal po praní stvrdol?", "Často ide o zvyšky pracieho produktu, preplnený bubon alebo nevhodné sušenie. Pomáha lepší oplach a šetrnejšia dávka."),
            ("Ako voňať modalové pyžamo?", "Najprv musí byť čisté a úplne suché. Vôňu skúšajte jemne, pretože pyžamo je pri pokožke celú noc."),
        ],
    },
    "polyamide_vs_polyester": {
        "marker": "Detailnejšie porovnanie polyamidu a polyesteru pri športe a praní",
        "product_kind": "laundry",
        "intro": [
            "Polyamid a polyester sú syntetické materiály, ktoré sa často stretávajú v športovom oblečení, legínach, tričkách, bundách a spodných vrstvách. Na prvý pohľad môžu vyzerať podobne, ale pri nosení a praní sa správajú odlišne. Polyamid býva hladší, pevný a pružný, polyester je veľmi rozšírený, rýchlo schne a často sa používa v tréningových tričkách.",
            "Pri rozhodovaní nie je otázka iba ktorý materiál je lepší. Dôležité je, či riešite pot, zápach, časté pranie, odolnosť voči oderu, rýchle schnutie alebo jemný dotyk. Zloženie býva navyše často zmesové, napríklad s elastanom, takže starostlivosť musí brať do úvahy celý kus.",
        ],
        "bullets": [
            "<strong>Polyamid:</strong> hladký, pevný, často príjemný na legínach a spodných vrstvách.",
            "<strong>Polyester:</strong> veľmi častý pri športových tričkách a rýchloschnúcich vrstvách.",
            "<strong>Zápach:</strong> rozhoduje aj strih, švy, elastan, čas do prania a sušenie.",
            "<strong>Pranie:</strong> nepreplniť bubon, nepreháňať dávkovanie a sušiť úplne.",
        ],
        "tables": [
            {
                "title": "Porovnanie pri používaní",
                "headers": ["Téma", "Polyamid", "Polyester"],
                "rows": [
                    ("dotyk", "často hladší a mäkší", "závisí od úpletu, môže byť ľahký a suchší"),
                    ("šport", "dobrý pri legínach a priliehavých kusoch", "častý pri tričkách a vrstvách"),
                    ("zápach", "môže držať v švoch a elastane", "často riešený pri potivých tričkách"),
                    ("schnutie", "rýchle, podľa hrúbky", "veľmi rýchle pri tenkých úpletoch"),
                ],
            },
            {
                "title": "Pranie podľa problému",
                "headers": ["Problém", "Čo skontrolovať", "Praktický krok"],
                "rows": [
                    ("zápach po tréningu", "ako dlho bol kus vlhký v taške", "prať skôr a dobre vysušiť"),
                    ("lepkavý dotyk", "dávkovanie gélu a aviváž", "znížiť dávku, pridať oplach"),
                    ("zatrhnutie", "zipsy, suchý zips, hrubé kusy", "prať oddelene alebo vo vrecku"),
                    ("slabá vôňa po praní", "či je textil naozaj čistý", "najprv riešiť pot, až potom vôňu"),
                ],
            },
        ],
        "sections": [
            ("Kedy zvoliť polyamid a kedy polyester", "Ak hľadáte hladké legíny, spodnú vrstvu alebo priliehavé športové oblečenie, polyamid môže pôsobiť príjemnejšie na dotyk. Polyester je veľmi praktický pri tričkách, mikinách a rýchloschnúcich vrstvách. Rozhoduje však aj kvalita úpletu, hrúbka materiálu a podiel elastanu.", "Pri praní nie je vhodné hádzať všetky syntetické materiály do jednej kategórie. Jemné legíny s elastanom potrebujú inú ochranu pred trením než hrubšie polyesterové tričko. Ak chcete rozumieť jednotlivým vláknam, nadväzuje článok <a href=\"/n/co-je-polyamid-alebo-nylon-vlastnosti-odolnost-a-pranie\">čo je polyamid alebo nylon</a>."),
            ("Ako riešiť zápach pri polyamide a polyesteri", "Zápach pri športových syntetikách nevzniká iba z materiálu. Veľkú úlohu hrá pot, baktérie, švy, elastan, preplnený bubon a čas, počas ktorého zostane oblečenie vlhké v taške. Ak po tréningu necháte tričko alebo legíny zavreté v batohu, zápach sa pri ďalšom nosení vráti rýchlejšie.", "Najlepší postup je jednoduchý: vyvetrať alebo presušiť po tréningu, prať skôr, nepreplniť bubon a dobre vysušiť. Vôňa má zmysel až vtedy, keď je pot skutočne odstránený. Pri zápachu v legínach pomôže aj návod <a href=\"/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu\">ako odstrániť zápach z bežeckých legín</a>."),
            ("Ako prať športové oblečenie zo syntetiky", "Športové syntetické oblečenie perte v dávke, ktorá má priestor. Ak je bubon príliš plný, textil sa síce namočí, ale pot a zvyšky gélu sa horšie vyplavia. Priveľa pracieho produktu tiež nepomáha. Môže sa zachytiť v elastických častiach a po zahriatí tela vytvoriť lepkavý alebo zatuchnutý dojem.", "Pri funkčných kúskoch opatrne s avivážou. Ak výrobca neodporúča zmäkčovadlá, radšej ich vynechajte. Viac k tomu nájdete v článku <a href=\"/n/kedy-nepouzivat-avivaz-uteraky-sportove-oblecenie-softshell-aj-detska-bielizen\">kedy nepoužívať aviváž</a>."),
            ("Ako sušiť polyamid a polyester po tréningu", "Oba materiály zvyčajne schnú rýchlo, ale musia mať vzduch. Zrolované tričko na kope s uterákom zostane vlhké v záhyboch a zápach sa ľahko vráti. Po praní textil pretrepte, vyhlaďte a sušte s priestorom. Horúce sušenie používajte len vtedy, keď ho povoľuje štítok.", "Pri legínach alebo elastických tričkách sledujte hlavne gumy, lemy a hrubšie švy. Tam sa vlhkosť drží dlhšie. Ak sú tieto miesta nedosušené, oblečenie môže zapáchať aj vtedy, keď zvyšok kusu pôsobí suchý."),
            ("Ako si vybrať rutinu pre časté pranie", "Ak športujete často, oplatí sa vytvoriť samostatnú rutinu pre syntetické veci. Nečakajte, kým sa nahromadí veľká dávka mokrého oblečenia. Radšej perte menšie dávky, používajte primerané množstvo gélu a po praní okamžite sušte. Tak znížite potrebu agresívnych zásahov.", "Pri výbere vône začnite mierne. Športové oblečenie sa pri pohybe zahrieva a silná vôňa môže byť rušivá. Ak chcete skúšať viac možností, praktickejšie je začať vzorkami a sledovať, ako vôňa pôsobí po tréningu, nie iba hneď po vypraní."),
        ],
        "box": ("Rýchla zásada", "Pri polyamide aj polyesteri rozhoduje viac rutina než jeden silný program: prať skôr, nepreplniť bubon, dobre opláchnuť a úplne vysušiť."),
        "faq": [
            ("Je na šport lepší polyamid alebo polyester?", "Závisí od kusu. Polyamid býva hladší pri legínach, polyester je veľmi častý pri tričkách a rýchloschnúcich vrstvách."),
            ("Ktorý materiál viac zapácha?", "Zápach závisí od potu, strihu, švov, elastanu a prania. Materiál je len jedna časť problému."),
            ("Ako prať syntetické športové veci?", "Perte ich skôr po nosení, nepreplňte bubon, dávkujte primerane a sušte úplne. Pri funkčných kusoch opatrne s avivážou."),
        ],
    },
}


def build_expansion(topic):
    config = TOPICS[topic]
    tables_html = "\n".join(
        f"<h2>{tbl['title']}</h2>\n{table(tbl['headers'], tbl['rows'])}" for tbl in config["tables"]
    )
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
        {note_card("Rýchla praktická diagnostika", config["bullets"])}
        {tables_html}
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


BOOSTS = {
    "softshell_washing": clean(
        """
        <h2>Čo robiť, keď softshell po praní stále nepôsobí dobre</h2>
        <p>Ak je softshell po praní stále ťažký, tuhý alebo menej príjemný, neriešte hneď nový prípravok. Najprv si spätne prejdite celý postup: koľko kusov bolo v bubne, či sa použila aviváž, či bol pridaný extra oplach a ako dlho bunda schla. Funkčné materiály často reagujú citlivo práve na kombináciu malej chyby v dávkovaní a slabého oplachu.</p>
        <p>Pri ďalšom praní skúste menšiu náplň, presnejšie dávkovanie a úplné vysušenie pred hodnotením povrchu. Ak voda na čistom suchom materiáli stále vsiakne do celej plochy, až potom má zmysel riešiť obnovu povrchovej úpravy. Ak sa voda perlí aspoň na časti bundy, problém môže byť lokálna špina alebo opotrebované miesta, nie celý materiál.</p>
        """
    ),
    "lyocell_tencel": clean(
        """
        <h2>Čo robiť, keď lyocell po praní pôsobí pokrčene alebo unavene</h2>
        <p>Ak lyocell po praní vyzerá pokrčene, najprv skontrolujte, či neostal príliš dlho v práčke. Jemné celulózové materiály sa po skončení programu oplatí vybrať hneď, pretrepať a vyhladiť rukami. Veľa záhybov vznikne nie tým, že materiál je nekvalitný, ale tým, že bol mokrý stlačený bez prúdenia vzduchu.</p>
        <p>Pri ďalšom praní znížte množstvo bielizne v bubne a nespájajte lyocell s ťažkými uterákmi. Ak ide o blúzku alebo šaty, sušte ich tak, aby sa švy nenatiahli vlastnou hmotnosťou. Pri posteľnej bielizni nechajte kus voľne rozložený, pretože veľké plochy v záhyboch držia vlhkosť a môžu vytvoriť zatuchnutý dojem.</p>
        """
    ),
    "polyamide_nylon": clean(
        """
        <h2>Čo robiť, keď polyamid po tréningu zapácha aj po praní</h2>
        <p>Ak polyamidové oblečenie po praní stále zapácha, často je problém v tom, čo sa stalo pred praním. Vlhké športové veci zatvorené v taške sa zaparia a pot sa drží v švoch, gumách a elastických častiach. Práčka potom musí riešiť starší pach, nie čerstvý pot. Preto je lepšie veci po tréningu vyvetrať alebo prať skôr.</p>
        <p>Pri ďalšom praní znížte náplň bubna a nepoužívajte viac gélu len preto, že oblečenie zapácha. Priveľa pracieho produktu sa môže horšie vypláchnuť a pri zahriatí tela vznikne lepkavý alebo zatuchnutý pocit. Dôležitý je pohyb textilu vo vode, primerané dávkovanie a úplné vysušenie všetkých lemov.</p>
        """
    ),
    "modal_clothing": clean(
        """
        <h2>Čo robiť, keď modal stratil mäkkosť alebo sviežosť</h2>
        <p>Ak modal po praní nepôsobí tak mäkko, nemusí ísť o trvalé poškodenie. Často je za tým priveľa pracieho produktu, preplnený bubon alebo pomalé sušenie. Modal sa nosí blízko pokožky, preto na ňom ľahko vnímate aj malé zvyšky vo vlákne. Skúste pri ďalšom praní menšiu dávku bielizne a dôkladnejší oplach.</p>
        <p>Pri pyžame a spodnej bielizni sledujte aj telové krémy, oleje a pot. Ak sa tieto zvyšky vrstvia, vôňa ich iba prekryje a po pár hodinách nosenia sa problém vráti. Lepšie je lokálne predčistiť zaťažené miesta, prať jemne a nechať modal úplne vyschnúť. Až potom má zmysel riešiť vôňu ako príjemný doplnok.</p>
        """
    ),
    "polyamide_vs_polyester": clean(
        """
        <h2>Ako vyhodnotiť, ktorý materiál vám v praxi sedí viac</h2>
        <p>Najpresnejší test nie je porovnanie zloženia na štítku, ale správanie po troch až piatich noseniach a praniach. Sledujte, kedy sa objaví zápach, ako rýchlo kus schne, či sa lepí na pokožku a ako vyzerajú švy. Polyamid aj polyester môžu fungovať výborne, ak je dobrý strih, kvalitný úplet a správna pracia rutina.</p>
        <p>Ak sa rozhodujete pri športovom oblečení, nekupujte iba podľa jedného materiálu. Všímajte si aj podiel elastanu, hrúbku, hustotu úpletu a spôsob prania odporúčaný výrobcom. Materiál, ktorý jednému človeku vyhovuje na beh, môže byť pre iného lepší iba na turistiku alebo domáce cvičenie. Domáca rutina potom rozhodne, či si oblečenie udrží sviežosť aj pri častom praní.</p>
        """
    ),
}


FINAL_BOOSTS = {
    "softshell_washing": clean(
        """
        <h2>Malý domáci test pred ďalším praním softshellu</h2>
        <p>Pred ďalším praním si poznačte, ktoré miesta boli najviac špinavé: golier, manžety, kolená alebo spodný lem. Pri ďalšom cykle potom cielene predčistíte len tieto miesta a nemusíte zbytočne zvyšovať intenzitu celého programu.</p>
        """
    ),
    "lyocell_tencel": clean(
        """
        <h2>Malý domáci test pre lyocell pred nosením</h2>
        <p>Po uschnutí prejdite lyocell rukou po švoch a miestach, kde sa látka najviac krčí. Ak je povrch hladký a bez zatuchnutého nádychu, pranie aj sušenie fungovalo dobre. Ak nie, nabudúce pomôže menšia dávka a rýchlejšie vybratie z práčky.</p>
        """
    ),
    "polyamide_nylon": clean(
        """
        <h2>Malý domáci test pre polyamid po tréningu</h2>
        <p>Polyamidové oblečenie ovoňajte až po úplnom vysušení a potom znovu po krátkom zahriatí pri nosení. Ak sa pach vráti až po zahriatí, vo vlákne alebo švoch zostali zvyšky potu a rutina potrebuje lepší oplach, skoršie pranie alebo menšiu náplň.</p>
        """
    ),
    "modal_clothing": clean(
        """
        <h2>Malý domáci test pre modal pred odložením do skrine</h2>
        <p>Modal odkladajte až vtedy, keď je úplne suchý aj v lemoch a švoch. Ak ho zložíte mierne vlhký, mäkký materiál môže získať zatuchnutý nádych a pri ďalšom nosení nebude pôsobiť sviežo, hoci bol vypraný správne.</p>
        """
    ),
}


SOFTSHELL_LIMIT_BOOST = clean(
    """
    <p><strong>Praktická poznámka:</strong> Ak máte viac softshellových kusov v domácnosti, perte spolu iba podobne špinavé a podobne hrubé veci. Detské zablatené nohavice nedávajte do rovnakej dávky s ľahkou mestskou bundou, ak chcete zachovať dobrý oplach aj povrch.</p>
    """
)


def article_slug(article):
    if article.get("link"):
        return article["link"]
    if article.get("slug"):
        return article["slug"]
    if article.get("url"):
        return article["url"].rstrip("/").split("/")[-1]
    return ""


def repair_legacy_links(markup):
    for old, new in LEGACY_LINK_REPLACEMENTS.items():
        markup = markup.replace(old, new)
    return markup


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


def insert_boost(long, key):
    boost = BOOSTS.get(key)
    if not boost or boost in long:
        return long
    anchor = long.find('<div style="border: 1px solid #dbe5de', long.find(MARKERS[key]))
    if anchor == -1:
        anchor = insertion_index(long)
    return long[:anchor].rstrip() + "\n" + boost + "\n" + long[anchor:].lstrip()


def insert_final_boost(long, key):
    boost = FINAL_BOOSTS.get(key)
    if not boost or boost in long:
        return long
    anchor = long.find('<div style="border: 1px solid #dbe5de', long.find(MARKERS[key]))
    if anchor == -1:
        anchor = insertion_index(long)
    return long[:anchor].rstrip() + "\n" + boost + "\n" + long[anchor:].lstrip()


def insert_softshell_limit_boost(long, key):
    if key != "softshell_washing" or SOFTSHELL_LIMIT_BOOST in long:
        return long
    anchor = long.find('<div style="border: 1px solid #dbe5de', long.find(MARKERS[key]))
    if anchor == -1:
        anchor = insertion_index(long)
    return long[:anchor].rstrip() + "\n" + SOFTSHELL_LIMIT_BOOST + "\n" + long[anchor:].lstrip()


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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 10 functional materials articles.")
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
        match_slugs = {config["slug"]}
        if config.get("legacy_source_slug"):
            match_slugs.add(config["legacy_source_slug"])

        for article in rows:
            if article_slug(article) not in match_slugs:
                continue
            original_title = article.get("title")
            original_short = article.get("short", "")
            original_url = article.get("url")
            original_long = article["long"]
            legacy_slug_corrected = False
            if article_slug(article) != config["slug"]:
                article["link"] = config["slug"]
                legacy_slug_corrected = True
            article["long"] = repair_legacy_links(article["long"])
            article["long"] = insert_softshell_limit_boost(
                insert_final_boost(
                    insert_boost(insert_expansion(article["long"], config["topic"]), config["topic"]),
                    config["topic"],
                ),
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
                    "legacy_slug_corrected_to_live_url": legacy_slug_corrected,
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
                "wave": "retrofit-wave-10-functional-materials-five",
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
                "wave": "retrofit-wave-10-functional-materials-five",
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
