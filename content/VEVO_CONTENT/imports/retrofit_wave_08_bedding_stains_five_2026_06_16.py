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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-08-bedding-stains-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-08-bedding-stains-five-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-23-2026-06-11-articles.json",
        "slug": "akryl-vs-vlna-zmolkovanie-teplo-zapach-a-starostlivost",
        "post_id": "2243",
        "url": "https://www.vevo.sk/n/akryl-vs-vlna-zmolkovanie-teplo-zapach-a-starostlivost",
        "topic": "acrylic_wool",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-odstranit-zvratky-z-koberca-oblecenia-a-postelnej-bielizne",
        "post_id": "2162",
        "url": "https://www.vevo.sk/n/ako-odstranit-zvratky-z-koberca-oblecenia-a-postelnej-bielizne",
        "topic": "vomit_textiles",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka",
        "post_id": "2200",
        "url": "https://www.vevo.sk/n/ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka",
        "topic": "sunscreen_oil",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-odstranit-moc-z-matraca-plachty-a-detskeho-pyzama",
        "post_id": "2161",
        "url": "https://www.vevo.sk/n/ako-odstranit-moc-z-matraca-plachty-a-detskeho-pyzama",
        "topic": "urine_mattress",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-vlasove-serum-z-uteraka-a-goliera-kosele",
        "post_id": "2177",
        "url": "https://www.vevo.sk/n/ako-odstranit-vlasove-serum-z-uteraka-a-goliera-kosele",
        "topic": "hair_serum",
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
            <h2 style="margin-top: 0;">Odporúčané riešenie pri citlivom výbere vône</h2>
            <p>Pri materiáloch, ktoré držia pach alebo sú pri tvári a pokožke, je praktické skúšať vône postupne a v nižšej intenzite.</p>
            <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
            <h3 style="margin-top: 0;">Vevo Essence Sample Set 9x10ml</h3>
            <p>Vzorkový set pomôže otestovať viac vôní bez veľkého balenia a bez zbytočne silnej parfumácie hneď pri prvom praní.</p>
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
        <p>Pri škvrnách najprv odstráňte zdroj nečistoty a až potom riešte vôňu. Prací produkt má pomôcť vyprať zvyšky z vlákna, nie prekryť pach.</p>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>Vhodný ako univerzálny základ pri bežnom praní textílií po lokálnom predčistení škvrny. Pri špeciálnych materiáloch vždy rešpektujte štítok.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
        </div>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        """
    )


TOPICS = {
    "acrylic_wool": {
        "marker": "Ako sa akryl a vlna správajú pri nosení",
        "product_kind": "samples",
        "intro": [
            "Akryl a vlna sa v obchode môžu javiť podobne, pretože oba materiály vedia hriať a často sa používajú v svetroch, šáloch, čiapkach alebo dekách. Pri nosení a praní sa však správajú rozdielne. Akryl je syntetické vlákno, vlna je prírodné živočíšne vlákno. Rozdiel pocítite pri teple, pachu, žmolkovaní, statike, sušení aj pri tom, ako materiál reaguje na trenie.",
            "Najväčšia chyba je prať akryl a vlnu rovnakou rutinou. Akryl znesie bežnejšiu starostlivosť, ale môže žmolkovať, elektrizovať a pri teple stratiť pekný povrch. Vlna potrebuje šetrnejší režim, menej trenia a dobré sušenie naležato. Pri oboch materiáloch platí, že vôňa má dopĺňať čistotu, nie maskovať zatuchnutie zo skrine.",
        ],
        "bullets": [
            "<strong>Akryl:</strong> sledujte žmolkovanie, statiku a teplo pri sušení.",
            "<strong>Vlna:</strong> chráňte ju pred trením, žmýkaním a prudkou zmenou teploty.",
            "<strong>Zápach:</strong> najprv vetrajte a riešte skladovanie, až potom perte.",
            "<strong>Vôňa:</strong> pri svetroch používajte jemnejšiu intenzitu než pri uterákoch.",
        ],
        "table": {
            "headers": ["Vlastnosť", "Akryl", "Vlna"],
            "rows": [
                ("Teplo", "hreje podľa hrúbky a pletenia", "vie hriať aj pri nižšej hmotnosti"),
                ("Zápach", "môže držať pach potu alebo skrine", "často stačí vetranie, ak nie je spotená"),
                ("Žmolkovanie", "časté pri trení a lacnejšej priadzi", "závisí od kvality a úpravy vlákna"),
                ("Pranie", "šetrný program podľa štítku", "program na vlnu alebo ručný postup"),
                ("Sušenie", "bez vysokého tepla", "naležato, bez krútenia a radiátora"),
            ],
        },
        "sections": [
            (
                "Ako prať akrylový sveter",
                "Akrylový sveter perte naruby, v menšej dávke a bez drsného trenia s rifľami, uterákmi alebo zipsami. Akryl sa síce často tvári ako nenáročný materiál, ale povrch sa vie rýchlo zmeniť. Pri príliš silnom odstreďovaní, teple alebo sušení na radiátore môže byť sveter tvrdší, elektrizovať alebo začať viac žmolkovať.",
                "Ak je akryl cítiť po skrini, najprv ho vyvetrajte. Ak je spotený, perte ho podľa štítku a dávkujte prací prostriedok primerane. Priveľa produktu môže zostať v pletenine a po vysušení vytvoriť ťažší pocit. Pri vôni do prania začnite jemne, pretože sveter býva blízko tváre a krku.",
            ),
            (
                "Ako prať vlnený sveter bez splstenia",
                "Vlna potrebuje pokojnejší postup. Najviac jej škodí kombinácia tepla, trenia a krútenia. Ak štítok povoľuje pranie, použite program na vlnu, nízku teplotu a nízke otáčky. Pri ručnom praní sveter nestláčajte agresívne a nešúchajte rukáv o rukáv. Po oplachu ho vytlačte cez uterák, nie krútením.",
                "Sušenie je pri vlne rovnako dôležité ako samotné pranie. Mokrá vlna je ťažká a pri zavesení sa môže vytiahnuť. Sušte ju naležato, vytvarovanú do pôvodnej dĺžky a šírky. Ak sveter iba jemne zapácha po nosení, často stačí dôkladné vetranie mimo skrine.",
            ),
            (
                "Prečo svetre zapáchajú po skrini",
                "Zatuchnutý pach svetrov často nevzniká nosením, ale skladovaním. Ak do skrine uložíte mierne vlhký sveter alebo ju preplníte sezónnym oblečením, vzduch necirkuluje a pach sa prenesie aj na čisté kusy. Akryl aj vlna vedia prevziať pach priestoru, v ktorom ležia niekoľko mesiacov.",
                "Pred sezónou svetre vyvetrajte a skontrolujte ich po jednom. Ak zapácha celý sveter rovnako, riešte skriňu. Ak zapácha najmä podpazušie alebo golier, problém je skôr v nosení. Tento rozdiel vám povie, či má zmysel prať, vetrať alebo najprv vyčistiť úložný priestor.",
            ),
            (
                "Ako znížiť žmolkovanie a statiku",
                "Žmolky vznikajú najmä tam, kde sa materiál trie: boky, rukávy, kabelka, pás kabáta alebo miesto pri stole. Akrylové svetre žmolkujú často, ale aj vlna môže vytvárať žmolky podľa kvality vlákna. Perte naruby, nepreplňte bubon a sveter nesušte v horúcom prostredí.",
                "Statika sa zhoršuje v suchom vzduchu a pri syntetike. Pri akryle pomáha šetrné pranie, nepresušovanie a skladovanie bez zbytočného trenia. Pri vlne sa zamerajte skôr na zachovanie tvaru a jemnosti než na silnú parfumáciu. Príliš intenzívna vôňa môže pri pleteninách pôsobiť ťažko.",
            ),
            (
                "Ako vybrať vôňu pri akryle a vlne",
                "Pri svetroch a šáloch používajte jemnejšiu vôňu než pri uterákoch alebo posteľnej bielizni. Textil je blízko nosa, nosí sa dlhšie a zahrieva sa pri tele. Ak je vôňa príliš silná, môže po pár hodinách pôsobiť ťažko, najmä pri hrubšej pletenine alebo uzavretom kabáte.",
                "Najprv musí byť sveter čistý, suchý a bez zatuchnutia. Potom môžete jemne testovať vôňu. Ak má domácnosť citlivý nos, deti alebo migrény, začnite vzorkou a menšou dávkou. Pri vlne sa vždy riaďte štítkom a nepoužívajte postup, ktorý by poškodil vlákno len kvôli vôni.",
            ),
        ],
        "box": ("Kontrola pred uložením svetrov", "Svetre odkladajte až úplne suché a vyvetrané. Ak pach cítite už pri skladaní do skrine, v uzavretom priestore sa zvýrazní."),
        "faq": [
            ("Je akryl horší než vlna?", "Nie automaticky. Akryl je lacnejší a jednoduchší na údržbu, vlna je prirodzenejšia a často lepšie pracuje s teplom. Rozhoduje použitie a kvalita konkrétneho materiálu."),
            ("Môžem prať vlnu v práčke?", "Iba ak to povoľuje štítok a práčka má vhodný program. Pri drahom alebo citlivom svetri je bezpečnejší veľmi šetrný postup."),
            ("Prečo sveter zapácha po skrini?", "Najčastejšie pre vlhkosť, preplnenú skriňu alebo uloženie nedostatočne suchého oblečenia. Najprv vetrajte a skontrolujte úložný priestor."),
        ],
    },
    "vomit_textiles": {
        "marker": "Prečo zvratky riešiť hygienicky a rýchlo",
        "product_kind": "laundry",
        "intro": [
            "Zvratky patria medzi škvrny, pri ktorých nestačí riešiť len viditeľný fľak. Obsahujú tekutinu, zvyšky jedla, kyslý pach a často aj hygienické riziko. Pri koberci, oblečení a posteľnej bielizni je najdôležitejšie konať rýchlo, oddeliť znečistené veci a nešíriť škvrnu do väčšej plochy.",
            "Najčastejšia chyba je okamžite liať veľa vody alebo silno drhnúť. Pri koberci tým zatlačíte zvyšky hlbšie, pri posteľnej bielizni roznesiete pach a pri oblečení môžete poškodiť vlákna. Lepší postup je odstrániť pevné zvyšky, odsávať vlhkosť, predčistiť lokálne a potom prať alebo sušiť podľa typu povrchu.",
        ],
        "bullets": [
            "<strong>Najprv odstráňte pevné zvyšky:</strong> pracujte smerom dovnútra, nie do strán.",
            "<strong>Oddelte znečistený textil:</strong> nedávajte ho medzi suchú bielizeň.",
            "<strong>Nepoužívajte horúce sušenie:</strong> kým pach a fľak nezmiznú.",
            "<strong>Pri koberci:</strong> cieľ je vyčistiť povrch bez premočenia podkladu.",
        ],
        "table": {
            "headers": ["Povrch", "Prvý krok", "Najväčšie riziko"],
            "rows": [
                ("Oblečenie", "odstrániť zvyšky a prepláchnuť zo zadnej strany", "zafixovaný kyslý pach"),
                ("Posteľná bielizeň", "oddeliť vrstvy a prať samostatne", "prenesenie na plachtu alebo matrac"),
                ("Koberec", "odsávať a čistiť lokálne", "premáčanie podkladu"),
                ("Matracový chránič", "prať čo najskôr a dôkladne vysušiť", "vlhkosť uzavretá pod plachtou"),
                ("Detské pyžamo", "predčistiť manžety a golier", "zvyšky v švoch"),
            ],
        },
        "sections": [
            (
                "Ako bezpečne odstrániť zvyšky",
                "Použite papierovú utierku, rukavice alebo starú handričku a pracujte opatrne. Pevné zvyšky zbierajte smerom do stredu škvrny, aby ste ju nerozširovali. Pri koberci nešúchajte povrch agresívne, pretože zvyšky sa dostanú hlbšie medzi vlákna. Pri oblečení a posteľnej bielizni odstráňte všetko, čo sa dá, ešte pred praním.",
                "Ak sa škvrna stala v posteli, skontrolujte aj vrstvy pod ňou: plachtu, matracový chránič a prípadne matrac. Zvratky sa vedia dostať cez jednu vrstvu a pach sa potom vracia aj po vypratí obliečky. Každú zasiahnutú vrstvu riešte samostatne.",
            ),
            (
                "Ako prať oblečenie a posteľnú bielizeň",
                "Prateľné textílie najprv prepláchnite studenšou vodou a predčistite miesto škvrny. Potom perte podľa štítku s dostatočným priestorom v bubne. Veľké kusy, ako obliečky alebo plachty, sa v práčke radi stočia. Ak je vnútri znečistené miesto, nemusí sa dobre vyprať ani opláchnuť.",
                "Pred sušením skontrolujte pach. Ak je cítiť kyslý alebo ťažký tón, nepoužívajte sušičku ani radiátor. Teplo môže zvyšky zvýrazniť. Radšej zopakujte lokálne predčistenie a perte menšiu dávku, aby mal textil priestor na pohyb a oplach.",
            ),
            (
                "Ako čistiť koberec bez premočenia",
                "Pri koberci je cieľom dostať nečistotu z povrchu bez toho, aby ste ju zatlačili do podkladu. Po odstránení pevných zvyškov prikladajte savú handričku a odsávajte vlhkosť tlakom. Čistiaci roztok používajte v malom množstve, postupne a po otestovaní na menej viditeľnom mieste.",
                "Po čistení musí koberec preschnúť aj vnútri. Ak ostane vlhký, pach sa vráti. Pomôže prúdenie vzduchu, otvorené okno alebo ventilátor. Neprekrývajte miesto nábytkom, kým nie je úplne suché. Pri veľkej nehode alebo hrubom koberci je bezpečnejšie profesionálne čistenie.",
            ),
            (
                "Čo robiť pri chorobe v domácnosti",
                "Ak ide o zvracanie počas virózy, textílie oddeľte od bežnej bielizne a manipulujte s nimi hygienicky. Po ošetrení škvrny si umyte ruky, vyvetrajte miestnosť a skontrolujte aj uteráky, pyžamá a prikrývky v okolí. Zápach môže zostať v miestnosti, aj keď hlavný textil už je v práčke.",
                "Pri posteľnej bielizni po chorobe je dôležité sušenie. Čisté, ale pomaly schnúce obliečky môžu zatuchnúť a zhoršiť dojem z celej postele. Súvisiaci postup nájdete aj v návode <a href=\"/n/ako-prat-bielizen-po-chorobe-obliecky-uteraky-a-pyzama\">ako prať bielizeň po chorobe</a>.",
            ),
            (
                "Ako zabrániť návratu pachu",
                "Zvratky majú silný pach, ktorý sa môže držať v švoch, výplni, koberci alebo matracovom chrániči. Preto nekontrolujte len viditeľný fľak. Textil ovoňajte po praní a potom znova po vysušení. Ak sa pach vráti po zahriatí v ruke alebo po navlhčení, zvyšky ešte ostali vo vlákne.",
                "Vôňa do prania môže pomôcť až vtedy, keď je zdroj odstránený. Ak ju pridáte príliš skoro, vznikne zmes parfumácie a kyslého pachu. Pri škvrnách tohto typu je čistota, oplach a sušenie dôležitejšie než intenzita vône.",
            ),
        ],
        "box": ("Kontrola pred sušením", "Pred sušením vždy skontrolujte miesto škvrny čuchom aj dotykom. Ak je cítiť kyslo alebo je látka iná než okolie, ešte nie je hotovo."),
        "faq": [
            ("Môžem dať posteľnú bielizeň rovno do práčky?", "Až po odstránení pevných zvyškov a krátkom predčistení. Inak sa škvrna a pach môžu rozšíriť v celej dávke."),
            ("Ako odstrániť zvratky z koberca?", "Najprv odstráňte zvyšky, odsávajte vlhkosť a čistite lokálne s minimom vody. Koberec musí potom preschnúť do hĺbky."),
            ("Prečo pach ostal aj po praní?", "Zvyšky mohli zostať v švoch, vo výplni alebo sa textil pomaly sušil. Nepoužívajte teplo, kým pach nezmizne."),
        ],
    },
    "sunscreen_oil": {
        "marker": "Prečo opaľovací olej zanecháva mastnú mapu",
        "product_kind": "laundry",
        "intro": [
            "Opaľovací olej na plážovej tunike, uteráku alebo osuške je nepríjemný preto, že nejde iba o bežnú škvrnu od vody. Olejová zložka sa naviaže na vlákno, zachytí piesok, pot, parfum a zvyšky opaľovacieho prípravku. Po bežnom praní môže fľak vyzerať svetlejší, ale mastná mapa sa ukáže až po vysušení.",
            "Pri plážových textíliách je navyše problém v tom, že bývajú mokré, slané a často skončia zatvorené v taške. Olej sa tak mieša s vlhkosťou a pachom z pláže. Najlepší postup je textil najprv vytriasť, odstrániť piesok, predčistiť mastné miesto a až potom prať.",
        ],
        "bullets": [
            "<strong>Najprv piesok:</strong> pred praním ho vytraste, aby nepracoval ako brúsivo.",
            "<strong>Potom olej:</strong> mastnú mapu riešte lokálne pred praním.",
            "<strong>Uterák:</strong> nepreplňte bubon, inak sa mastnota zle vypláchne.",
            "<strong>Tunika:</strong> testujte farbu a jemný materiál na skrytom mieste.",
        ],
        "table": {
            "headers": ["Textil", "Riziko", "Praktický postup"],
            "rows": [
                ("Plážová tunika", "mastná mapa na ľahkej látke", "lokálne predčistiť a prať jemne"),
                ("Uterák", "olej znižuje savosť", "prať s priestorom a dobrým oplachom"),
                ("Osuška", "pach z vlhkej tašky", "nenechať zatvorenú mokrú"),
                ("Plavkový prehoz", "citlivá farba alebo elastan", "test a nízke teplo"),
                ("Taška na pláž", "prenáša olej späť", "vyčistiť aj vnútro tašky"),
            ],
        },
        "sections": [
            (
                "Ako predčistiť mastnú mapu",
                "Mastné miesto najprv nepolievajte veľkým množstvom vody. Olej sa s vodou mieša zle a môže sa rozšíriť do väčšej mapy. Najprv odstráňte piesok a suché nečistoty. Potom naneste malé množstvo pracieho roztoku na škvrnu a jemne ho zapracujte prstami alebo mäkkou handričkou.",
                "Pri jemnej tunike alebo farebnom textile najprv testujte na menej viditeľnom mieste. Niektoré plážové látky sú tenké, farbené alebo zmesové. Silné trenie, horúca voda a agresívny postup môžu poškodiť látku skôr než olejovú škvrnu.",
            ),
            (
                "Ako prať plážové uteráky po oleji",
                "Uterák po opaľovacom oleji perte s dostatočným priestorom v bubne. Hrubé uteráky držia vodu aj produkt, preto potrebujú dobrý pohyb a oplach. Ak ich natlačíte do práčky spolu s ďalšími ťažkými kusmi, mastnota sa nemusí vyplaviť a savosť ostane horšia.",
                "Nepreháňajte dávkovanie. Veľa gélu môže zanechať zvyšky a uterák bude po vysušení tvrdší alebo menej savý. Pri uterákoch je lepšie primerané množstvo, dobrý oplach a rýchle sušenie. Ak mastný tieň ostal, pred sušením zopakujte lokálne ošetrenie.",
            ),
            (
                "Čo robiť s olejom a pieskom naraz",
                "Piesok z textilu pred praním vždy vytraste. Ak sa dostane do práčky s mastnotou, môže sa držať v záhyboch, uterákoch aj tesnení. Pri tunike alebo osuške najprv textil nechajte preschnúť, vytraste ho vonku a až potom riešte mastné miesta.",
                "Plážová taška býva často zabudnutý zdroj problému. Ak je vo vnútri olej, piesok a vlhkosť, čistý uterák sa pri ďalšom použití znovu zašpiní. Občas vyčistite aj vnútro tašky, najmä po dovolenke alebo po dňoch pri vode.",
            ),
            (
                "Prečo mastná škvrna vidno až po vysušení",
                "Kým je textil mokrý, mastná mapa môže splývať s okolím. Po vysušení sa ukáže ako tmavší tieň, tvrdší dotyk alebo menej savé miesto. Preto textil po praní skontrolujte ešte pred sušičkou alebo radiátorom. Teplo môže mastnú stopu zvýrazniť.",
                "Pri svetlej tunike sa pozrite proti dennému svetlu. Pri uteráku skontrolujte dotyk a savosť. Ak miesto odpudzuje vodu alebo pôsobí hladšie než zvyšok, olej ešte ostal. Vtedy má zmysel opakovať predčistenie, nie pridávať parfumáciu.",
            ),
            (
                "Ako predísť škvrnám na pláži",
                "Opaľovací olej nechajte na pokožke chvíľu vsiaknuť, až potom si oblečte tuniku alebo sa utrite do uteráka. Pri deťoch a rýchlom prezliekaní to nie je vždy možné, preto je dobré mať jeden uterák na telo a druhý na sedenie. Znížite tým prenos oleja do celej osušky.",
                "Po príchode domov nenechajte vlhké plážové textílie zatvorené v taške. Vlhkosť, olej a teplo vytvoria pach, ktorý sa potom ťažšie odstraňuje. Textílie vytraste, nechajte preschnúť a perte čo najskôr.",
            ),
        ],
        "box": ("Kontrola pred sušením", "Ak mastná mapa ostala viditeľná alebo je miesto hladšie na dotyk, textil ešte nesušte teplom. Zopakujte lokálne predčistenie."),
        "faq": [
            ("Dá sa opaľovací olej vyprať z uteráka?", "Áno, ale pomáha lokálne predčistenie, nepreplnený bubon a dobrý oplach. Pri zaschnutej škvrne postup zopakujte pred sušením."),
            ("Prečo uterák po oleji menej saje?", "Olej môže obaliť vlákna a zhoršiť savosť. Preto treba odstrániť mastnú zložku, nie ju iba prevoňať."),
            ("Môžem použiť horúcu vodu?", "Riaďte sa štítkom. Pri jemnej tunike, elastane alebo farebnom textile môže horúci postup poškodiť materiál."),
        ],
    },
    "urine_mattress": {
        "marker": "Prečo moč riešiť inak na matraci, plachte a pyžame",
        "product_kind": "laundry",
        "intro": [
            "Moč na matraci, plachte alebo detskom pyžame treba riešiť rýchlo, ale rozdielne podľa povrchu. Plachta a pyžamo sa dajú vyprať. Matrac sa vyprať nedá, preto je cieľom čo najskôr odsať vlhkosť, zabrániť prenikaniu hlbšie a dôkladne sušiť. Ak sa matrac premočí, pach sa môže vracať celé týždne.",
            "Pri detských nehodách je dôležité rozobrať posteľ po vrstvách. Ak vyperiete iba plachtu, ale moč prešiel do chrániča alebo matraca, pach sa vráti. Preto najprv zistite rozsah, potom perte prateľné vrstvy a matrac riešte samostatne bez premočenia.",
        ],
        "bullets": [
            "<strong>Plachta:</strong> vyprať čo najskôr a skontrolovať pach pred sušením.",
            "<strong>Pyžamo:</strong> predprať miesta pri lemoch a švoch.",
            "<strong>Matrac:</strong> odsávať tlakovo, nepreliať vodou.",
            "<strong>Chránič:</strong> sušiť úplne, inak sa pach uzavrie pod plachtou.",
        ],
        "table": {
            "headers": ["Vrstva", "Čo urobiť", "Čomu sa vyhnúť"],
            "rows": [
                ("Pyžamo", "prepláchnuť a prať podľa štítku", "nechať zaschnúť v koši"),
                ("Plachta", "prať samostatne alebo s podobnými kusmi", "sušiť teplom pred kontrolou"),
                ("Matracový chránič", "prať a dosušiť úplne", "uložiť vlhký späť na matrac"),
                ("Matrac", "odsávať vlhkosť a vetrať", "liať veľa vody dovnútra"),
                ("Posteľné okolie", "vyvetrať a skontrolovať prikrývku", "riešiť iba viditeľnú škvrnu"),
            ],
        },
        "sections": [
            (
                "Ako postupovať v prvých minútach",
                "Najprv odstráňte mokré vrstvy z postele. Plachtu, pyžamo a chránič oddeľte od suchej bielizne. Na matrac položte savý uterák alebo papierové utierky a tlačte, nie šúchajte. Cieľom je vytiahnuť čo najviac vlhkosti von, nie zatlačiť ju hlbšie.",
                "Ak je nehoda čerstvá, nepoužívajte hneď veľa vody. Matrac sa potom bude sušiť oveľa dlhšie. Pri prateľných textíliách môžete pracovať s prepláchnutím a praním, pri matraci skôr s odsávaním, lokálnym ošetrením a vetraním.",
            ),
            (
                "Ako prať plachtu a pyžamo",
                "Plachtu a pyžamo prepláchnite alebo predčistite podľa intenzity znečistenia. Pri detskom pyžame skontrolujte lemy, rozkrok, manžety a švy. Práve tam môže zostať pach aj po bežnom praní. Perte s dostatočným priestorom v bubne a nečakajte niekoľko dní.",
                "Pred sušením skontrolujte, či pach zmizol. Ak je textil stále cítiť, zopakujte lokálne ošetrenie. Sušička alebo radiátor môžu zvyškový pach zafixovať. Po vypratí sušte rýchlo a do skrine ukladajte až úplne suché kusy.",
            ),
            (
                "Ako čistiť matrac bez premočenia",
                "Matrac čistite v malých krokoch. Po odsávaní môžete použiť mierne navlhčenú handričku a ošetriť iba povrch. Neprelievajte miesto vodou, pretože vlhkosť sa dostane hlbšie a bude schnúť veľmi dlho. Po každom kroku znovu odsávajte suchou textíliou.",
                "Matrac musí schnúť vo vzduchu. Odokryte posteľ, vetrajte a ak je to možné, zvýšte prúdenie vzduchu. Nedávajte na matrac hneď plachtu ani chránič. Ak pach zostane po úplnom vysušení, problém je hlbšie a domáce čistenie môže mať limit.",
            ),
            (
                "Ako predchádzať návratu pachu",
                "Najlepšia prevencia je prateľný matracový chránič. Zachytí vlhkosť skôr, než sa dostane do matraca. Chránič však musí byť po praní úplne suchý, inak pod plachtou vytvorí vlhké prostredie a pach sa vráti. Pri častých nehodách je praktické mať náhradný chránič.",
                "Ráno po nehode skontrolujte nielen posteľ, ale aj pyžamo, prikrývku a okolie matraca. Moč sa môže preniesť pohybom dieťaťa. Ak ostane jedna vrstva neošetrená, čistá plachta nepomôže.",
            ),
            (
                "Kedy riešiť odborné čistenie",
                "Ak moč prenikol hlboko do matraca, pach sa vracia po každom zahriatí alebo pri vlhkom počasí. Vtedy už nemusí stačiť domáce povrchové čistenie. Pri drahom matraci alebo opakovaných nehodách zvážte profesionálne čistenie a zároveň lepšiu ochranu matraca do budúcna.",
                "Pri textíliách je situácia jednoduchšia: pyžamo, plachta a chránič sa dajú oprať a skontrolovať. Pri matraci je dôležitá rýchlosť. Čím kratšie ostane moč vo vnútri, tým menšia šanca na dlhodobý pach.",
            ),
        ],
        "box": ("Kontrola po vysušení", "Matrac hodnotte až po úplnom vysušení. Ak pach cítiť len pri priblížení nosa alebo po zahriatí rukou, vlhkosť alebo zvyšky ešte ostali vnútri."),
        "faq": [
            ("Môžem matrac preliať vodou?", "Nie je to dobrý nápad. Voda zatlačí moč hlbšie a matrac bude dlho schnúť. Lepšie je odsávať a čistiť lokálne."),
            ("Ako prať pyžamo po nehode?", "Predčistite zasiahnuté miesta, perte podľa štítku a pred sušením skontrolujte pach."),
            ("Prečo pach ostal aj po výmene plachty?", "Moč pravdepodobne prenikol do chrániča alebo matraca. Treba skontrolovať všetky vrstvy postele."),
        ],
    },
    "hair_serum": {
        "marker": "Prečo vlasové sérum robí mastný film",
        "product_kind": "laundry",
        "intro": [
            "Vlasové sérum, olej na končeky alebo stylingový prípravok môže na uteráku a golieri košele zanechať mastný film. Na prvý pohľad nemusí ísť o výraznú škvrnu, ale po vysušení sa ukáže tmavšia mapa, hladší dotyk alebo miesto, ktoré horšie saje vodu. Pri golieri sa do toho pridáva pot, parfum a kožný maz.",
            "Pri týchto škvrnách je dôležité riešiť olejovú zložku ešte pred praním. Ak hodíte uterák alebo košeľu rovno do práčky, sérum sa môže rozptýliť do vlákien a po vysušení zostane mastný tieň. Silnejšia vôňa problém nevyrieši, iba ho prekryje.",
        ],
        "bullets": [
            "<strong>Uterák:</strong> kontrolujte savosť a mastný dotyk.",
            "<strong>Golier:</strong> riešte kombináciu séra, potu a krému.",
            "<strong>Pred praním:</strong> lokálne ošetriť mastné miesto.",
            "<strong>Pred sušením:</strong> skontrolovať mapu pri dennom svetle.",
        ],
        "table": {
            "headers": ["Miesto", "Ako sa škvrna prejaví", "Čo pomáha"],
            "rows": [
                ("Uterák na vlasy", "menej saje a má mastný dotyk", "lokálne predčistenie a dobrý oplach"),
                ("Golier košele", "tmavší lem alebo lesklé miesto", "predčistiť pred praním"),
                ("Obliečka na vankúš", "mastná mapa pri vlasoch", "prať skôr a nepreplniť bubon"),
                ("Šatka", "zmena lesku na jemnej látke", "testovať na skrytom mieste"),
                ("Župan", "pach kozmetiky a mastnota", "prať s priestorom a sušiť vzdušne"),
            ],
        },
        "sections": [
            (
                "Ako predčistiť uterák po vlasovom sére",
                "Najprv nájdite miesto, kde uterák prišiel do kontaktu s vlasmi. Mastný film je často viditeľný až pri bočnom svetle alebo na dotyk. Naneste malé množstvo pracieho roztoku na konkrétne miesto a jemne ho zapracujte. Cieľom je uvoľniť olejovú zložku, nie premočiť celý uterák.",
                "Uteráky perte s dostatočným priestorom v bubne. Ak ich periete veľa naraz, mastnota a prací produkt sa horšie vypláchnu. Pri uterákoch sledujte aj savosť. Ak po praní odpudzujú vodu, môže v nich zostať sérum alebo prebytok pracieho prostriedku.",
            ),
            (
                "Ako odstrániť sérum z goliera košele",
                "Golier košele zachytáva viac vecí naraz: vlasové sérum, pot, krém, parfum aj kožný maz. Preto ho pred praním skontrolujte samostatne. Pri bielej košeli môže byť problém žltkastý alebo sivý lem, pri tmavej košeli skôr lesklá mapa. Lokálne ošetrenie je dôležitejšie než silnejší program.",
                "Pri jemných košeliach testujte na skrytom mieste a nešúchajte golier agresívne. Trením môžete poškodiť vlákno alebo vytvoriť svetlejšiu stopu. Po praní kontrolujte golier ešte pred žehlením. Teplo zo žehličky môže zvyškovú mastnotu zvýrazniť.",
            ),
            (
                "Prečo škvrna ostane aj po praní",
                "Olejové a silikónové zložky z vlasových prípravkov sa môžu držať na povrchu vlákna. Ak sa pred praním neuvoľnia, bežný program ich nemusí odstrániť úplne. Po vysušení potom vidíte mapu alebo cítite hladší film. Pri uterákoch sa navyše zhorší savosť.",
                "Ak sa škvrna po praní vráti, nepoužívajte hneď sušičku ani žehličku. Zopakujte lokálne predčistenie a perte menšiu dávku. Pri obliečkach na vankúš môže pomôcť prať ich častejšie, najmä ak používate vlasové oleje večer.",
            ),
            (
                "Ako predchádzať mastným mapám",
                "Vlasové sérum nechajte vo vlasoch chvíľu vstrebať, až potom si oblečte košeľu, šatku alebo ľahnite na čistú obliečku. Pri uteráku môžete mať jeden vyhradený na vlasové oleje a nepoužívať ho ako bežný uterák na tvár. Znížite tým prenos mastnoty na ďalšie textílie.",
                "Ak používate sérum denne, sledujte golier, obliečky a uteráky ako jednu skupinu. Mastnota sa môže hromadiť postupne a až po niekoľkých praniach si všimnete, že textil horšie saje alebo je na dotyk iný. Prevencia je jednoduchšia než opakované odmasťovanie.",
            ),
            (
                "Ako kombinovať čistotu a vôňu",
                "Pri kozmetických škvrnách má vôňa zmysel až po odstránení filmu. Ak pridáte parfumáciu na mastný zvyšok, výsledok môže pôsobiť ťažko. Uterák alebo košeľa majú po praní pôsobiť neutrálne čisto, bez mastného dotyku. Až potom má zmysel pridať jemnú vôňu.",
                "Pri obliečkach a uterákoch zároveň sledujte citlivosť pokožky. Textil prichádza do kontaktu s tvárou a vlasmi, preto je lepšie pracovať s primeranou dávkou, dobrým oplachom a úplným vysušením než s prehnanou intenzitou vône.",
            ),
        ],
        "box": ("Kontrola pred žehlením alebo sušením", "Golier a uterák kontrolujte ešte pred teplom. Ak je miesto mastné alebo lesklé, teplo môže stopu zvýrazniť."),
        "faq": [
            ("Dá sa vlasové sérum vyprať z uteráka?", "Áno, ale najlepšie po lokálnom predčistení mastného miesta. Pri uteráku potom skontrolujte savosť."),
            ("Prečo je golier po praní stále mastný?", "Na golieri sa mieša sérum, pot, krém a kožný maz. Bežný program bez predčistenia nemusí stačiť."),
            ("Môžem škvrnu len prevoňať?", "Nie spoľahlivo. Najprv odstráňte mastný film, až potom pridajte vôňu na čistý textil."),
        ],
    },
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


SECOND_PASS = {
    "acrylic_wool": clean(
        f"""
        <h2>Krátky test pred sezónnym nosením</h2>
        <p>Pred prvým nosením po sezóne položte sveter na vzduch, prejdite rukou po miestach trenia a skontrolujte pach v podpazuší, na golieri a v strede chrbta. Ak zapácha celý kus rovnako, riešte skriňu a vetranie. Ak zapácha lokálne, ide skôr o zvyšky nosenia.</p>
        {table(["Kontrola", "Čo sledovať", "Čo urobiť"], [
            ("Pach celého svetra", "skriňa, vlhkosť, sezónne skladovanie", "vetrať a skontrolovať úložný priestor"),
            ("Pach v podpazuší", "nosenie a pot", "lokálne ošetriť alebo šetrne prať"),
            ("Žmolky na bokoch", "trenie od kabáta alebo kabelky", "odžmolkovať a prať naruby"),
            ("Vytiahnutý tvar", "zlé sušenie alebo vešanie", "sušiť naležato a skladovať zložené"),
        ])}
        <p>Ak je sveter čistý, ale fádny alebo zatuchnutý po skrini, nezačínajte silnou vôňou. Najprv vetrajte, potom riešte pranie podľa štítku a až nakoniec jemnú vôňu. Pri akryle aj vlne sa oplatí menej intenzívny výsledok, ktorý nebude pri krku rušivý.</p>
        """
    ),
    "vomit_textiles": clean(
        f"""
        <h2>Rozhodovanie podľa toho, kde nehoda vznikla</h2>
        <p>Inak sa postupuje pri zvratkoch na hladkej plachte, inak pri hrubom koberci a inak pri detskom pyžame s manžetami. Rozdiel je v tom, či viete textil vyprať celý, či môže vlhkosť prejsť do výplne a či sa zvyšky držia v švoch. Práve tieto detaily rozhodujú, či sa pach vráti po vysušení.</p>
        {table(["Miesto nehody", "Čo skontrolovať navyše", "Kedy postup zopakovať"], [
            ("Detská posteľ", "plachtu, chránič, pyžamo a prikrývku", "ak pach ostal po vysušení"),
            ("Koberec pri posteli", "spodnú vrstvu a okraj škvrny", "ak miesto cítiť po navlhčení"),
            ("Oblečenie", "švy, golier, vrecká a vrstvy látky", "ak látka ostala tvrdá alebo kyslá"),
            ("Posteľná bielizeň", "vnútro obliečky a zipsovú časť", "ak sa pach vráti v skrini"),
            ("Matracový chránič", "či neprepustil tekutinu nižšie", "ak matrac pod ním zapácha"),
        ])}
        <h2>Čo nerobiť pri silnom zápachu</h2>
        <p>Pri silnom zápachu je lákavé použiť veľa vône, horúcu vodu alebo agresívne drhnutie. Pri zvratkoch to však často nepomôže. Horúce sušenie môže zvyšky zvýrazniť, trenie rozšíri škvrnu a veľa parfumácie vytvorí len ťažkú zmes pachov. Lepší je pokojný postup: odstrániť zvyšky, predčistiť, vyprať, skontrolovať a až potom sušiť.</p>
        <p>Ak ide o detskú izbu, vetrajte aj miestnosť a skontrolujte textílie v okolí. Pach sa môže držať v plyšovej hračke, deke alebo koberci, aj keď hlavná posteľná bielizeň už je čistá. Keď riešite celý priestor naraz, výsledok je stabilnejší.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Domáci kontrolný test</h2>
        <p>Po vyčistení nechajte miesto úplne vyschnúť, potom ho jemne navlhčite čistou vodou na malej ploche. Ak sa kyslý pach vráti, zvyšky ešte ostali v textílii alebo podklade.</p>
        </div>
        """
    ),
    "sunscreen_oil": clean(
        f"""
        <h2>Rozhodovanie podľa typu opaľovacieho produktu</h2>
        <p>Opaľovací olej, krém a samoopaľovací prípravok sa na textile nesprávajú rovnako. Olej zanecháva mastnú mapu, krém môže vytvoriť svetlý film a samoopaľovací produkt často pridá farbivo. Pri plážovej tunike a uteráku je preto dobré vedieť, čo škvrnu spôsobilo, lebo jeden univerzálny postup nemusí stačiť.</p>
        {table(["Produkt", "Typická stopa", "Najlepší prvý krok"], [
            ("Opaľovací olej", "mastná tmavšia mapa", "lokálne odmasťovať pred praním"),
            ("Opaľovací krém", "biely alebo žltkastý film", "uvolniť zvyšok pred praním"),
            ("Samoopaľovací prípravok", "farebný hnedý odtieň", "riešiť aj pigment, nielen mastnotu"),
            ("Olej s parfumom", "mastnota aj vôňa", "najprv odstrániť olejový základ"),
            ("Plážová zmes", "olej, soľ, pot a piesok", "vytriasť, predčistiť, prať s priestorom"),
        ])}
        <h2>Ako skontrolovať uterák po praní</h2>
        <p>Uterák po opaľovacom oleji kontrolujte inak než tričko. Dôležitá nie je iba farba, ale aj savosť. Ak miesto odpudzuje vodu alebo je hladšie než okolie, olej ešte zostal vo vlákne. Takýto uterák môže po ďalšom použití zapáchať rýchlejšie, najmä ak sa znovu zabalí vlhký do tašky.</p>
        <p>Pri plážovej tunike sledujte najmä okraje, zaväzovanie, ramená a miesta, kde sa látka dotýka pokožky po natretí. Tenké látky ukážu mastnú mapu pri bočnom svetle. Ak škvrna ostala, neopravujte ju teplom ani žehlením. Najprv zopakujte lokálne predčistenie.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Dovolenková rutina</h2>
        <p>Po návrate z pláže vytraste piesok, rozložte vlhké veci a mastné miesta predčistite ešte pred veľkým praním. Ušetríte tým uteráky, práčku aj ďalšie oblečenie v dávke.</p>
        </div>
        """
    ),
    "urine_mattress": clean(
        f"""
        <h2>Rozhodovanie podľa toho, ako hlboko sa moč dostal</h2>
        <p>Pri moči je zásadné rozlíšiť povrchovú nehodu od premočenia. Povrchová nehoda na plachte alebo pyžame sa dá vyriešiť praním. Ak tekutina prešla cez chránič do matraca, cieľom je hlavne odsávanie, sušenie a zabránenie uzavretiu vlhkosti. Čím hlbšie sa moč dostane, tým dlhšie sa môže pach vracať.</p>
        {table(["Rozsah", "Ako ho spoznáte", "Čo urobiť"], [
            ("Len pyžamo", "plachta je suchá", "prepláchnuť a vyprať pyžamo"),
            ("Plachta a pyžamo", "vlhká vrchná vrstva", "prať oba kusy a skontrolovať chránič"),
            ("Chránič zasiahnutý", "vlhkosť pod plachtou", "prať chránič samostatne a dosušiť"),
            ("Matrac povrchovo", "vlhký povrch, ale nie hlboké premočenie", "odsávať a vetrať"),
            ("Matrac hlboko", "pach sa vracia po vysušení", "zvážiť odborné čistenie"),
        ])}
        <h2>Nočná nehoda: praktický postup ráno</h2>
        <p>Ak sa nehoda zistí až ráno, začnite oddelením vrstiev. Nedávajte mokré pyžamo a plachtu do zatvoreného koša spolu s uterákmi. Najprv ich prepláchnite alebo nechajte pripravené na rýchle pranie. Matrac nechajte odkrytý, aby vlhkosť neostala pod čistou plachtou.</p>
        <p>Pri opakovaných nehodách sa oplatí mať systém: náhradné pyžamo, druhý matracový chránič a miesto na rýchle predčistenie. Nejde len o pohodlie, ale aj o to, aby moč nestihol zaschnúť v švoch a vrstvách textilu.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Kontrola pred ustlaním</h2>
        <p>Matrac prikryte až vtedy, keď je suchý na dotyk aj bez pachu. Ak ho zakryjete predčasne, zvyšková vlhkosť sa uzavrie pod chráničom a pach sa vráti.</p>
        </div>
        """
    ),
    "hair_serum": clean(
        f"""
        <h2>Rozhodovanie podľa typu vlasového prípravku</h2>
        <p>Vlasové sérum, olej, krém na kučery a leave-in kondicionér môžu mať rozdielne zloženie, ale na textile často vytvárajú podobný problém: mastný alebo hladký film. Pri uteráku sa prejaví stratou savosti, pri golieri leskom alebo tmavšou mapou. Ak je prípravok parfumovaný, pach môže pretrvať aj po praní.</p>
        {table(["Prípravok", "Typická stopa", "Čo spraviť pred praním"], [
            ("Vlasový olej", "mastná tmavšia mapa", "lokálne uvoľniť olejový film"),
            ("Silikónové sérum", "hladký povrch a horšia savosť", "predčistiť a dobre opláchnuť"),
            ("Krém na kučery", "mastnota aj zvyšky stylingu", "ošetriť golier alebo uterák lokálne"),
            ("Leave-in kondicionér", "mäkký, klzký film", "neprať v preplnenom bubne"),
            ("Parfumovaný produkt", "vôňa drží pri golieri", "najprv odstrániť film, až potom vôňu"),
        ])}
        <h2>Ako kontrolovať golier po praní</h2>
        <p>Golier po praní skontrolujte ešte pred žehlením. Pozrite sa naň pri dennom svetle a prejdite prstami po vnútornej strane. Ak je hladší, tmavší alebo lesklejší než zvyšok košele, zvyšok séra tam stále je. Žehlením by ste ho mohli zvýrazniť.</p>
        <p>Pri uteráku spravte jednoduchý test savosti. Kvapnite trochu vody na miesto, ktoré bolo mastné. Ak sa voda drží na povrchu alebo steká inak než inde, uterák nie je úplne čistý. Potrebuje lokálne predčistenie a pranie s dobrým oplachom.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Kúpeľňová rutina</h2>
        <p>Ak vlasové oleje používate často, vyhraďte si jeden uterák na vlasy a perte ho pravidelnejšie. Znížite tým prenos mastnoty na uteráky na tvár, obliečky a golier oblečenia.</p>
        </div>
        """
    ),
}


THIRD_PASS = {
    "vomit_textiles": clean(
        """
        <h2>Praktický záver pri veľkej nehode</h2>
        <p>Ak bola nehoda väčšia, nesnažte sa všetko vyriešiť jedným praním a jedným čistením koberca. Rozdeľte si prácu na vrstvy: najprv pevné zvyšky, potom prateľný textil, potom koberec alebo matrac a nakoniec vetranie miestnosti. Tak znížite riziko, že sa pach presunie z jedného miesta na druhé.</p>
        <p>Pri detských textíliách si zapíšte, ktoré vrstvy boli zasiahnuté. Ráno alebo pri únave sa ľahko zabudne na chránič, deku alebo koberec pri posteli. Ak ostane jedna vrstva neošetrená, bude pôsobiť, že pranie zlyhalo, hoci problém zostal mimo práčky.</p>
        """
    ),
    "sunscreen_oil": clean(
        """
        <h2>Praktický záver po návrate z pláže</h2>
        <p>Plážové textílie po oleji neodkladajte vlhké do koša ani do kufra. Najprv ich rozložte, vytraste piesok a skontrolujte miesta, ktoré sa dotýkali natretej pokožky. Uterák, tunika a plavkový prehoz často držia olej na rôznych miestach, preto ich netreba hodnotiť ako jednu rovnakú dávku bielizne.</p>
        <p>Ak je mastná mapa stále viditeľná po praní, nesušte ju teplom. Lokálne ošetrenie pred ďalším praním je bezpečnejšie než opakované bežné pranie celej dávky, pri ktorom sa olej môže rozšíriť na ďalšie textílie.</p>
        """
    ),
    "urine_mattress": clean(
        """
        <h2>Praktický záver pri opakovaných nehodách</h2>
        <p>Ak sa nehody opakujú, najväčší rozdiel urobí pripravený systém. Majte po ruke náhradné pyžamo, suchú plachtu a druhý matracový chránič. V noci potom nemusíte improvizovať a ráno viete presne, ktoré vrstvy treba vyprať a ktoré iba vetrať.</p>
        <p>Pri matraci sledujte vývoj pachu počas dňa. Ak sa ráno zdá suchý, ale večer po zahriatí miestnosti znova zapácha, vlhkosť alebo zvyšky sú hlbšie. Vtedy už nestačí dať čistú plachtu navrch; treba matrac ďalej sušiť alebo riešiť odborné čistenie.</p>
        """
    ),
    "hair_serum": clean(
        """
        <h2>Praktický záver pri kozmetike na textile</h2>
        <p>Pri vlasovom sére si všímajte opakovanie. Jeden fľak na golieri vyriešite lokálne, ale ak sa mastné mapy objavujú každý týždeň, treba zmeniť rutinu: nechať sérum vstrebať, používať samostatný uterák na vlasy a častejšie prať obliečku na vankúš.</p>
        <p>Textil pri tvári a vlasoch hodnotíme prísnejšie než bežné tričko. Má byť čistý na dotyk, bez mastného filmu a bez ťažkej zmesi kozmetiky a vône. Až potom dáva zmysel jemné prevoňanie pri praní.</p>
        """
    ),
}


FOURTH_PASS = {
    "vomit_textiles": clean(
        """
        <h2>Mini test pred návratom textilu do používania</h2>
        <p>Textil po zvratkoch vráťte do používania až vtedy, keď je bez pachu aj po úplnom vysušení. Pri obliečkach a pyžame skontrolujte švy a zipsy, pri koberci okraj pôvodnej škvrny. Ak miesto zapácha po navlhčení čistou vodou, problém ešte nie je odstránený.</p>
        """
    ),
    "sunscreen_oil": clean(
        """
        <h2>Mini test mastnej mapy</h2>
        <p>Po vysušení kvapnite na podozrivé miesto trochu vody. Ak sa kvapka správa inak než na okolitej látke alebo uterák horšie saje, olejový film ešte ostal. Vtedy je lepšie zopakovať lokálne predčistenie než prať celú dávku silnejším programom.</p>
        """
    ),
    "urine_mattress": clean(
        """
        <h2>Mini test pred čistou plachtou</h2>
        <p>Pred ustlaním položte na matrac suchú dlaň a skontrolujte pach zblízka. Ak miesto pôsobí chladnejšie, vlhkejšie alebo stále zapácha, ešte ho nezakrývajte. Čistá plachta by iba uzavrela zvyškovú vlhkosť a pach by sa vrátil.</p>
        """
    ),
    "hair_serum": clean(
        """
        <h2>Mini test mastného filmu</h2>
        <p>Pri uteráku skúste savosť, pri košeli sledujte lesk goliera pod bočným svetlom. Ak je miesto hladké, tmavšie alebo odpudzuje vodu, sérum ešte ostalo vo vlákne. Pred žehlením alebo sušením teplom zopakujte lokálne predčistenie.</p>
        """
    ),
}


FIFTH_PASS = {
    "sunscreen_oil": clean(
        """
        <h2>Kontrola pred ďalším plážovým dňom</h2>
        <p>Pred ďalším použitím uteráka alebo tuniky skontrolujte, či miesto po oleji neodpudzuje vodu a či textil po zohriatí v ruke nezapácha. Ak olej ostal vo vlákne, pri ďalšom kontakte so slnkom, potom a vlhkosťou sa mapa zvýrazní. Čistý plážový textil má byť savý, suchý a bez mastného filmu.</p>
        """
    ),
    "urine_mattress": clean(
        """
        <h2>Kontrola počas nasledujúceho dňa</h2>
        <p>Po nehode nestačí matrac skontrolovať raz. Pach sa môže vrátiť až po niekoľkých hodinách, keď sa miestnosť oteplí alebo sa matrac znovu prikryje. Nechajte ho dlhšie odkrytý, vetrajte a až potom vráťte chránič, plachtu a prikrývku. Pri opakovaných nehodách je práve trpezlivé sušenie najdôležitejšia časť postupu.</p>
        """
    ),
    "hair_serum": clean(
        """
        <h2>Kontrola pri opakovanom používaní séra</h2>
        <p>Ak sa mastné mapy stále vracajú, sledujte čas aplikácie séra. Keď si hneď po nanesení oblečiete košeľu alebo ľahnete na čistú obliečku, produkt sa prenesie priamo do textilu. Pomáha nechať vlasy chvíľu voľne, používať menej produktu a prať uterák na vlasy oddelene od uterákov na tvár.</p>
        """
    ),
}


SIXTH_PASS = {
    "urine_mattress": clean(
        """
        <h2>Posledná kontrola pred spaním</h2>
        <p>Večer matrac ešte raz ovoňajte a skontrolujte rukou. Ak je miesto suché, teplé ako okolie a bez pachu, môžete posteľ znova ustlať. Ak máte pochybnosť, nechajte matrac odkrytý dlhšie a použite náhradné miesto na spanie.</p>
        """
    ),
}


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
    updated = long
    if MARKERS[key] not in updated:
        index = insertion_index(updated)
        updated = updated[:index].rstrip() + "\n" + EXPANSIONS[key] + "\n" + updated[index:].lstrip()
    second_pass = SECOND_PASS.get(key)
    if second_pass:
        second_marker = re.search(r"<h2>(.*?)</h2>", second_pass).group(1)
        if second_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + second_pass + "\n" + updated[index:].lstrip()
    third_pass = THIRD_PASS.get(key)
    if third_pass:
        third_marker = re.search(r"<h2>(.*?)</h2>", third_pass).group(1)
        if third_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + third_pass + "\n" + updated[index:].lstrip()
    fourth_pass = FOURTH_PASS.get(key)
    if fourth_pass:
        fourth_marker = re.search(r"<h2>(.*?)</h2>", fourth_pass).group(1)
        if fourth_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + fourth_pass + "\n" + updated[index:].lstrip()
    fifth_pass = FIFTH_PASS.get(key)
    if fifth_pass:
        fifth_marker = re.search(r"<h2>(.*?)</h2>", fifth_pass).group(1)
        if fifth_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + fifth_pass + "\n" + updated[index:].lstrip()
    sixth_pass = SIXTH_PASS.get(key)
    if sixth_pass:
        sixth_marker = re.search(r"<h2>(.*?)</h2>", sixth_pass).group(1)
        if sixth_marker not in updated:
            index = insertion_index(updated)
            updated = updated[:index].rstrip() + "\n" + sixth_pass + "\n" + updated[index:].lstrip()
    return updated


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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 08 bedding and stain articles.")
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
            article["long"] = insert_expansion(article["long"], config["topic"])
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
                "wave": "retrofit-wave-08-bedding-stains-five",
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
                "wave": "retrofit-wave-08-bedding-stains-five",
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
