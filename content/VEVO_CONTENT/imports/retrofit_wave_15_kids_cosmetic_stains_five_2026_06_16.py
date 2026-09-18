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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-15-kids-cosmetic-stains-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-15-kids-cosmetic-stains-five-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-odstranit-zivicu-z-nohavic-bundy-a-detskeho-oblecenia",
        "post_id": "2140",
        "url": "https://www.vevo.sk/n/ako-odstranit-zivicu-z-nohavic-bundy-a-detskeho-oblecenia",
        "topic": "resin",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-odstranit-zinkovu-mast-z-detskeho-body-a-prebalovacej-podlozky",
        "post_id": "2218",
        "url": "https://www.vevo.sk/n/ako-odstranit-zinkovu-mast-z-detskeho-body-a-prebalovacej-podlozky",
        "topic": "zinc_ointment",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-dostat-piesok-z-detskych-sortiek-a-tricka-po-plazi-pred-pranim",
        "post_id": "2213",
        "url": "https://www.vevo.sk/n/ako-dostat-piesok-z-detskych-sortiek-a-tricka-po-plazi-pred-pranim",
        "topic": "sand",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-vyprat-granatove-jablko-z-oblecenia-bez-ruzovych-map",
        "post_id": "2160",
        "url": "https://www.vevo.sk/n/ako-vyprat-granatove-jablko-z-oblecenia-bez-ruzovych-map",
        "topic": "pomegranate",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-odstranit-jod-a-dezinfekciu-z-oblecenia-bez-zvaecsenia-flaku",
        "post_id": "2198",
        "url": "https://www.vevo.sk/n/ako-odstranit-jod-a-dezinfekciu-z-oblecenia-bez-zvaecsenia-flaku",
        "topic": "iodine",
    },
]


def clean(markup):
    return textwrap.dedent(markup).strip()


def table(headers, rows):
    head = "".join(f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{h}</th>' for h in headers)
    body = "\n".join(
        "<tr>" + "".join(f'<td style="border: 1px solid #e5e5e5; padding: 10px;">{c}</td>' for c in row) + "</tr>"
        for row in rows
    )
    return (
        '<table style="width: 100%; border-collapse: collapse; margin: 20px 0;">\n'
        f"<thead><tr>{head}</tr></thead>\n<tbody>\n{body}\n</tbody>\n</table>"
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
        <h2 style="margin-top: 0;">Odporúčané riešenie pre šetrné pranie po predčistení</h2>
        <p>{config["recommendation_intro"]}</p>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; margin: 14px 0;">
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>{config["product_text"]}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
        </div>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Kategória pracie gély</h3>
        <p>Pri škvrnách, ktoré sa najprv predčisťujú a až potom perú, je užitočné mať doma šetrný prací gél. Dôležité je primerané dávkovanie a dobrý oplach.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        </div>
        </div>
        """
    )


TOPICS = {
    "resin": {
        "marker": "Detailnejší postup na živicu, lepkavý zvyšok a outdoorové oblečenie",
        "problem": "živica je lepkavá zmes, ktorá sa drží na povrchu látky a pri trení sa vie roztiahnuť do väčšej mapy",
        "main_textile": "nohavice, bundu a detské oblečenie po lese",
        "avoid": "horúcu vodu, sušičku a agresívne šúchanie, kým je živica mäkká",
        "diagnosis": [
            "<strong>Najprv stuhnúť:</strong> mäkkú živicu sa nesnažte hneď rozmazať vodou.",
            "<strong>Odobrať objem:</strong> tupou hranou odstráňte čo najviac povrchovej vrstvy.",
            "<strong>Skontrolovať materiál:</strong> bunda, softshell a zmesový textil nemusia zniesť rovnaký postup ako bavlna.",
            "<strong>Prať až po predčistení:</strong> samotný cyklus môže živicu rozotrieť do okolia.",
        ],
        "state_rows": [
            ("mäkká živica", "nechať stuhnúť a odobrať", "nevtláčať do vlákna"),
            ("zaschnutý zvyšok", "uvoľňovať po malých častiach", "pozor na vytrhnutie vlákien"),
            ("mastný tieň", "predčistiť lokálne", "kontrola pred sušením"),
            ("škvrna na bunde", "riadiť sa štítkom", "membrána a úprava sú citlivé"),
        ],
        "textile_rows": [
            ("bavlnené nohavice", "odobrať živicu a lokálne predčistiť", "bavlna znesie viac než jemné zmesi"),
            ("softshell alebo bunda", "bez horúcej vody a agresívnych rozpúšťadiel", "môže sa poškodiť membrána alebo povrch"),
            ("detské tepláky", "pracovať jemne pri kolenách a švoch", "úplet sa dá vydrať"),
            ("batoh alebo poťah", "nepremáčať výstuž bez kontroly", "viac vrstiev schne pomaly"),
        ],
        "sections": [
            ("Ako odstrániť živicu z nohavíc", "Nohavice nechajte tak, aby živica najprv stuhla. Potom ju opatrne odoberte tupou hranou, nie nechtom cez celú látku. Ak zostane lepkavý alebo mastný tieň, riešte ho lokálne pred praním a až potom vložte nohavice do práčky.", "Pri pracovných a záhradných veciach sa živica často kombinuje s hlinou, trávou alebo potom. Vtedy pomôže pred praním najprv odstrániť pevné zvyšky a nepreplniť bubon."),
            ("Ako postupovať pri bunde a softshelli", "Pri bunde je najväčšie riziko poškodenie povrchovej úpravy. Nepoužívajte horúcu vodu ani neoverené rozpúšťadlá na veľkej ploche. Najprv testujte na skrytom mieste a sledujte, či sa nemení farba alebo povrch látky.", "Pri funkčnom oblečení nadväzuje článok <a href=\"/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany\">ako prať softshell bundu a nohavice</a>. Živica je samostatný problém, ale konečné pranie stále musí rešpektovať materiál."),
            ("Živica na detskom oblečení po lese", "Detské oblečenie po lese býva znečistené na kolenách, rukávoch a zadnej časti nohavíc. Pred praním každý kus otočte a skontrolujte lemy. Malý kúsok živice v záhybe sa pri praní môže rozotrieť na väčšiu plochu.", "Ak je na oblečení aj blato, najprv ho nechajte vyschnúť a vykefujte. Živicu riešte až ako lepkavú lokálnu škvrnu."),
            ("Prečo živicu netreba hneď drhnúť", "Živica sa pri tlaku a teple správa ako lepkavá hmota. Silné trenie ju môže vtlačiť hlbšie do väzby látky a rozšíriť ju za pôvodný okraj. Preto najprv znížte objem a až potom riešte zvyšný tieň.", "Tento postup sa podobá iným hmotám na textile. Pri detských tvorivých škvrnách pomôže aj návod <a href=\"/n/ako-odstranit-plastelinu-z-teplakov-koberca-a-potahu\">ako odstrániť plastelínu z teplákov</a>."),
            ("Kontrola pred finálnym sušením", "Po praní skontrolujte miesto pri dennom svetle. Ak zostal mastný alebo lesklý tieň, nesušte horúco. Teplo môže zvyšok zafixovať a ďalšie čistenie bude ťažšie.", "Pri bunde a outdoorových nohaviciach je lepšie sušiť voľne podľa štítku a až po kontrole riešiť prípadnú impregnáciu alebo ďalšiu starostlivosť."),
        ],
        "depth": [
            ("Keď je živica už stará", "Stará živica býva tvrdšia a krehkejšia, ale zároveň môže držať pevnejšie v štruktúre látky. Postupujte pomaly, po malých častiach a nesnažte sa ju odlúpnuť tak, že natiahnete celý úplet.", "Ak ide o drahú bundu alebo technický materiál, je lepšie zastaviť sa skôr a nezničiť povrch domácom experimentom."),
            ("Živica a zvyšný pach lesa", "Po odstránení živice môže textil stále voňať po lese, dreve alebo vlhkosti. Vôňa do prania má zmysel až vtedy, keď už v látke nie je lepkavý zvyšok.", "Ak oblečenie po praní zapácha skôr zatuchnuto, riešte sušenie a čistotu práčky, nie iba intenzitu vône."),
        ],
        "faq": [
            ("Môžem živicu hneď vyprať?", "Nie je to ideálne. Najprv odoberte povrchový zvyšok a predčistite miesto lokálne."),
            ("Čo ak je živica na softshelli?", "Postupujte podľa štítku a testujte opatrne. Chráňte membránu aj povrchovú úpravu."),
            ("Prečo ostal mastný tieň?", "Časť živice zostala vo vlákne. Pred ďalším sušením ju riešte lokálne."),
        ],
        "recommendation_intro": "Pri živici je dôležité najprv odstrániť lepkavý objem. Prací gél má zmysel pri následnom lokálnom predčistení a praní, nie ako náhrada mechanického odstránenia.",
        "product_text": "Vhodný základ na následné pranie po odstránení živice. Pri bundách, softshelli a funkčných materiáloch vždy rešpektujte štítok.",
        "links": [
            ("/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany", "Ako prať softshell bundu a nohavice bez poškodenia membrány"),
            ("/n/ako-odstranit-plastelinu-z-teplakov-koberca-a-potahu", "Ako odstrániť plastelínu z teplákov, koberca a poťahu"),
            ("/n/ako-vyprat-stare-skvrny-kompletny-sprievodca-pre-ciste-oblecenie", "Ako vyprať staré škvrny"),
        ],
    },
    "zinc_ointment": {
        "marker": "Detailnejší postup na zinkovú masť, biely film a detské textílie",
        "problem": "zinková masť spája mastný základ a biely minerálny film, ktorý sa vie rozmazať do väčšej plochy",
        "main_textile": "detské body, podbradník a prebaľovaciu podložku",
        "avoid": "silné trenie, horúce sušenie a pranie bez odstránenia prebytku masti",
        "diagnosis": [
            "<strong>Najprv zotrieť prebytok:</strong> masť nevtierajte hlbšie do látky.",
            "<strong>Riešiť mastnotu aj biely film:</strong> nejde iba o obyčajnú farebnú škvrnu.",
            "<strong>Detské body:</strong> po predčistení je dôležitý dobrý oplach pri pokožke.",
            "<strong>Prebaľovacia podložka:</strong> skontrolujte povrchovú úpravu a vrstvenie.",
        ],
        "state_rows": [
            ("čerstvá vrstva", "jemne zotrieť tupou hranou alebo papierom", "nešúchať do strán"),
            ("biely film", "lokálne predčistiť", "zvyšok býva v povrchu vlákna"),
            ("mastná mapa", "zamerať sa na odmasťovanie", "kontrola pred sušením"),
            ("podložka s vrstvou", "nepremáčať bez štítku", "môže mať nepriepustnú úpravu"),
        ],
        "textile_rows": [
            ("detské body", "zotrieť, predčistiť a dobre opláchnuť", "kontakt s pokožkou je dlhý"),
            ("podbradník", "riešiť lemy a švy", "masť sa drží v okrajoch"),
            ("prebaľovacia podložka", "čistiť podľa materiálu povrchu", "nie každá ide do práčky"),
            ("pyžamo", "prať s menšou dávkou", "zvyšky sa lepšie opláchnu"),
        ],
        "sections": [
            ("Ako odstrániť zinkovú masť z detského body", "Najprv odstráňte prebytok masti bez rozotierania. Použite tupú hranu alebo savý papier a pracujte od okraja ku stredu. Potom miesto lokálne predčistite malým množstvom pracieho gélu a nechajte krátko pôsobiť.", "Pri detskom body je dôležitý oplach. Zvyšky produktu aj zvyšky masti sú pri pokožke, preto nepreháňajte množstvo pracieho prostriedku a nekompenzujte problém silnou vôňou."),
            ("Ako čistiť prebaľovaciu podložku", "Prebaľovacia podložka môže byť textilná, vrstvená alebo s nepriepustnou úpravou. Najprv skontrolujte štítok. Ak nejde do práčky, čistite len povrchovo a nenechajte vodu preniknúť do výplne.", "Ak je podložka prateľná, odstráňte prebytok masti pred praním. Inak sa mastný film môže preniesť do ďalších kusov alebo zostať v lemoch."),
            ("Prečo zinková masť zanecháva bielu mapu", "Zinková masť býva hustá a obsahuje bielu minerálnu zložku v mastnom základe. Preto po bežnom praní môže zostať nielen mastná mapa, ale aj bledý povlak. Treba riešiť obe časti škvrny.", "Pri podobných mastných škvrnách nadväzuje článok <a href=\"/n/ako-odstranit-arasidove-maslo-z-tricka-obrusu-a-detskej-mikiny\">ako odstrániť arašidové maslo</a>."),
            ("Čo ak škvrna ostala po praní", "Ak po praní zostal biely alebo mastný tieň, nedávajte textil do sušičky. Miesto znovu lokálne predčistite a perte podľa štítku. Teplo môže zvyšky stabilizovať a potom sa odstraňujú ťažšie.", "Pri farebnom detskom oblečení testujte postup na menej viditeľnom mieste, aby ste nepoškodili farbu látky."),
            ("Ako prať detské veci po mastiach a krémoch", "Detské veci znečistené masťou neperte v preplnenom bubne. Voda a prací roztok sa musia dostať ku škvrne a následne sa musia dobre vypláchnuť. Pri body, pyžame a podbradníku sledujte hlavne lemy a vrstvy.", "Pri krémoch pre dospelých je postup podobný, ale materiály môžu byť citlivejšie. K téme nadväzuje <a href=\"/n/ako-odstranit-krem-na-ruky-z-rukavov-svetra-a-deky\">ako odstrániť krém na ruky z rukávov svetra</a>."),
        ],
        "depth": [
            ("Zinková masť v lemoch a pri patentkách", "Pri body sa masť často drží pri patentkách, lemoch a švoch. Tieto miesta pred praním prejdite prstami a skontrolujte, či nie sú stále klzké alebo biele.", "Ak je zvyšok v leme, bežné pranie ho nemusí vypláchnuť, najmä pri plnom bubne."),
            ("Citlivá pokožka a oplach", "Pri detských textíliách je dôležité, aby po praní nezostala mastnota ani prebytok pracieho produktu. Textil môže vyzerať čisto, ale pri dotyku pôsobiť klzko alebo tuhšie.", "Vtedy pomôže skôr opakovaný oplach a primeraná dávka než ďalšia vrstva vône."),
        ],
        "faq": [
            ("Prečo zinková masť nejde dole na prvýkrát?", "Obsahuje mastný základ aj bielu zložku. Treba odstrániť prebytok a predčistiť lokálne."),
            ("Môžem detské body po masti dať do sušičky?", "Až keď škvrna zmizne. Teplo môže mastný zvyšok zafixovať."),
            ("Ako čistiť prebaľovaciu podložku?", "Podľa štítku a vrstvenia. Nie každá podložka znesie pranie alebo premáčanie."),
        ],
        "recommendation_intro": "Pri zinkovej masti je cieľom najprv odstrániť prebytok a až potom prať. Pri detských textíliách má veľký význam aj dôkladný oplach.",
        "product_text": "Vhodný na následné pranie detského body, pyžama alebo podbradníka po lokálnom predčistení. Dávkujte primerane a sledujte oplach.",
        "links": [
            ("/n/ako-odstranit-arasidove-maslo-z-tricka-obrusu-a-detskej-mikiny", "Ako odstrániť arašidové maslo z trička, obrusu a detskej mikiny"),
            ("/n/ako-odstranit-krem-na-ruky-z-rukavov-svetra-a-deky", "Ako odstrániť krém na ruky z rukávov svetra a deky"),
            ("/n/ako-vyprat-detsku-vyzivu-a-mrkvove-prikrmy-z-podbradnikov", "Ako vyprať detskú výživu a mrkvové príkrmy z podbradníkov"),
        ],
    },
    "sand": {
        "marker": "Detailnejší postup na piesok po pláži, šortky a ochranu práčky",
        "problem": "piesok nie je klasická škvrna, ale pevné zrnká, ktoré mechanicky zaťažujú textil aj práčku",
        "main_textile": "detské šortky, tričko a plážové oblečenie",
        "avoid": "vkladanie piesku priamo do práčky bez vytrasenia a vysušenia",
        "diagnosis": [
            "<strong>Najprv vysušiť:</strong> suchý piesok sa z textilu uvoľňuje ľahšie než mokrý.",
            "<strong>Vytriasť mimo práčky:</strong> hrubý piesok nepatrí do bubna ani filtra.",
            "<strong>Skontrolovať vrecká:</strong> detské šortky často držia piesok v záhyboch.",
            "<strong>Riešiť soľ a opaľovací produkt:</strong> po pláži nejde iba o zrnká piesku.",
        ],
        "state_rows": [
            ("suchý piesok", "vytriasť a vykefovať", "najľahšia fáza"),
            ("mokrý piesok", "nechať preschnúť", "nevtláčať do látky"),
            ("piesok vo vreckách", "otočiť a vysypať", "chráni práčku"),
            ("soľ a opaľovací krém", "opláchnuť a prať podľa štítku", "môžu nechať mapy"),
        ],
        "textile_rows": [
            ("detské šortky", "skontrolovať vrecká a lemy", "piesok sa drží v švoch"),
            ("tričko", "vytriasť a prať s podobnými farbami", "vlhký piesok môže drhnúť vlákna"),
            ("plážové pareo", "jemný program podľa štítku", "ľahká látka sa ľahko vytiahne"),
            ("uterák", "najprv dôkladne vytriasť", "froté drží veľa zŕn"),
        ],
        "sections": [
            ("Ako dostať piesok z detských šortiek", "Šortky najprv nechajte preschnúť a potom ich vytraste vonku. Otočte vrecká, prejdite lemy a skontrolujte švy. Ak v nich zostane piesok, práčka ho síce čiastočne odplaví, ale zbytočne zaťažuje filter, bubon a odtok.", "Po vytrasení šortky perte podľa štítku. Pri farebných plážových veciach nepreplňte bubon, aby sa soľ, pot a zvyšky opaľovacieho produktu dobre vypláchli."),
            ("Ako vyprať tričko po pláži", "Tričko po pláži môže obsahovať piesok, soľ, pot a opaľovací krém. Najprv ho vytraste, potom prípadne krátko opláchnite a až potom perte. Ak má škvrny od opaľovacieho produktu, riešte ich lokálne pred praním.", "Pri podobnej mastnej plážovej škvrne nadväzuje článok <a href=\"/n/ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka\">ako vyprať opaľovací olej z plážovej tuniky a uteráka</a>."),
            ("Prečo piesok nepatrí priamo do práčky", "Piesok je mechanická nečistota. Nezmizne ako rozpustná škvrna, ale prechádza cez bubon, tesnenie, filter a odtok. Malé množstvo práčka zvládne, ale opakované pranie piesočnatých vecí ju zbytočne zaťažuje.", "Po plážovej sezóne sa oplatí skontrolovať aj filter a tesnenie práčky, najmä ak periete uteráky, šortky a plážové veci často."),
            ("Piesok, soľ a zatuchnutie", "Ak necháte mokré plážové veci v taške, piesok sa prilepí, soľ zaschne a textil môže zatuchnúť. Vôňa do prania potom iba prekrýva problém. Lepší postup je veci hneď vybrať, vytriasť a nechať preschnúť.", "Pri zatuchnutí oblečenia nadväzuje článok <a href=\"/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia\">prečo oblečenie zapácha po praní</a>."),
            ("Ako prať plážové veci po dovolenke", "Po dovolenke rozdeľte oblečenie na piesočnaté, mastné od opaľovacích produktov a bežne spotené. Najprv odstráňte piesok, potom riešte škvrny a až následne perte celú dávku.", "Ľahké šatky, pareá a tuniky perte šetrnejšie než uteráky. Viac k jemným plážovým kúskom nájdete v článku <a href=\"/n/ako-prat-plazove-pareo-satku-a-lahku-tuniku-po-dovolenke\">ako prať plážové pareo a tuniku</a>."),
        ],
        "depth": [
            ("Kontrola filtra po plážovej sezóne", "Ak sa doma často perú plážové veci, občas skontrolujte filter práčky. Piesok, vlasy a drobné zvyšky sa môžu hromadiť a zhoršiť odtok.", "Nejde o paniku po jednom praní, ale o dobrú údržbu pri opakovanom zaťažení."),
            ("Detské oblečenie a vrecká plné piesku", "Deti často nosia piesok vo vreckách, kapucni alebo záhyboch šortiek. Pred praním preto nestačí tričko a šortky iba hodiť do koša.", "Krátka kontrola vreciek chráni práčku aj ostatné oblečenie v dávke."),
        ],
        "faq": [
            ("Môžem dať piesočnaté veci rovno do práčky?", "Najprv ich vytraste a nechajte preschnúť. Hrubý piesok zbytočne zaťažuje práčku."),
            ("Ako dostať piesok z vreciek?", "Otočte vrecká naruby, vytraste ich a prejdite švy pred praním."),
            ("Prečo plážové veci zapáchajú?", "Často zostali mokré v taške spolu so soľou, pieskom a opaľovacím produktom."),
        ],
        "recommendation_intro": "Pri piesku je najdôležitejšia mechanická príprava pred praním. Prací gél pomáha až potom, keď už v textile nie je hrubý piesok.",
        "product_text": "Vhodný na následné pranie plážových tričiek, šortiek a bežnej bielizne po vytrasení piesku. Pri mastných opaľovacích škvrnách predčistite lokálne.",
        "links": [
            ("/n/ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka", "Ako vyprať opaľovací olej z plážovej tuniky a uteráka"),
            ("/n/ako-prat-plazove-pareo-satku-a-lahku-tuniku-po-dovolenke", "Ako prať plážové pareo, šatku a ľahkú tuniku po dovolenke"),
            ("/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia", "Prečo oblečenie zapácha po praní"),
        ],
    },
    "pomegranate": {
        "marker": "Detailnejší postup na granátové jablko, ružové mapy a ovocný pigment",
        "problem": "granátové jablko zanecháva výraznú šťavu s pigmentom, ktorý sa rýchlo vpije do svetlého textilu",
        "main_textile": "tričko, blúzku, obrus a detské oblečenie",
        "avoid": "sušenie na teple a žehlenie, kým je viditeľný ružový tieň",
        "diagnosis": [
            "<strong>Čerstvá šťava:</strong> oplachujte z rubovej strany, aby pigment odchádzal von.",
            "<strong>Zaschnutá mapa:</strong> najprv zvlhčiť a uvoľniť, nie drhnúť nasucho.",
            "<strong>Svetlý textil:</strong> kontrolovať pri dennom svetle pred sušením.",
            "<strong>Jemná blúzka:</strong> chrániť tvar a farbu, neísť hneď agresívne.",
        ],
        "state_rows": [
            ("čerstvý fľak", "oplach z rubu a lokálne predčistenie", "rýchlosť pomáha"),
            ("zaschnutý pigment", "zvlhčiť a postup opakovať", "nešúchať nasucho"),
            ("ružový tieň po praní", "nesušiť horúco, predčistiť znova", "teplo fixuje"),
            ("škvrna na jemnom textile", "testovať na skrytom mieste", "materiál je rovnako dôležitý ako pigment"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "oplach z rubu a predčistenie", "vlákno rýchlo nasaje šťavu"),
            ("blúzka", "šetrný režim podľa štítku", "viskóza a zmesi menia tvar"),
            ("obrus", "pracovať od okraja ku stredu", "škvrna sa rozširuje do väzby"),
            ("detské oblečenie", "kontrola pred sušičkou", "pigment sa rád ukáže až po praní"),
        ],
        "sections": [
            ("Ako vyprať granátové jablko z trička", "Tričko otočte naruby a škvrnu oplachujte z rubu. Nechajte vodu vytláčať pigment von, nie cez celú látku do väčšej mapy. Potom použite lokálne predčistenie a perte podľa farby a štítku.", "Ak je tričko svetlé, kontrolujte ho pred sušičkou. Ružový tieň je ľahšie riešiť pred teplom než po ňom."),
            ("Granátové jablko na obruse", "Pri obruse sa škvrna často rozleje do väčšej plochy. Pracujte od okraja ku stredu a podložte savú vrstvu. Ak je obrus ľanový alebo z jemnej zmesi, postupujte šetrnejšie.", "Pri podobných ovocných škvrnách nadväzuje článok <a href=\"/n/ako-vyprat-ovocne-skvrny-z-jahod-cucoriedok-a-malin\">ako vyprať ovocné škvrny z jahôd, čučoriedok a malín</a>."),
            ("Prečo vznikajú ružové mapy", "Šťava z granátového jablka obsahuje výrazné farbivá a vodnatú časť, ktorá sa rýchlo vpije do textilu. Ak sa škvrna len rozotrie, pigment sa presunie do väčšej plochy a po praní zostane jemný ružový tieň.", "Preto pomáha oplach z rubu, savý podklad a kontrola pred sušením. Podobný princíp má aj škvrna od čerešní; pozrite si <a href=\"/n/ako-odstranit-ceresne-z-detskeho-tricka-a-letnych-siat\">ako odstrániť čerešne z detského trička</a>."),
            ("Čo ak škvrna už prešla práčkou", "Ak škvrna po praní nezmizla, nepokračujte žehlením ani sušičkou. Znovu ju navlhčite, predčistite a perte podľa štítku. Pri bielom textile môžete mať viac možností než pri farebnej blúzke.", "Ak ide o jemný materiál, radšej opakujte mierny postup než použiť silný zásah, ktorý poškodí povrch alebo farbu."),
            ("Ako prať detské oblečenie po ovocí", "Detské veci po ovocí kontrolujte pred praním. Šťava môže byť na rukávoch, pri golieri alebo na spodnom okraji trička. Menšia dávka v bubne a dobrý oplach pomôžu viac než preplnená práčka.", "Pri starších škvrnách pomôže všeobecný návod <a href=\"/n/ako-vyprat-stare-skvrny-kompletny-sprievodca-pre-ciste-oblecenie\">ako vyprať staré škvrny</a>."),
        ],
        "depth": [
            ("Granátové jablko na bielom textile", "Na bielom textile sa aj slabý ružový tieň ukáže výrazne. Preto po praní skontrolujte látku pri dennom svetle a nespoliehajte sa len na mokrý vzhľad.", "Ak tieň zostal, riešte ho pred sušením. Po teple býva pigment odolnejší."),
            ("Rozdiel medzi ovocnou a mastnou škvrnou", "Granátové jablko je hlavne pigmentová a vodnatá škvrna. Nepotrebuje rovnaký postup ako mastná masť alebo olej.", "Ak sa na textile kombinuje ovocie s dezertom, najprv odoberte pevné zvyšky a potom posúďte, či riešite pigment, tuk alebo oboje."),
        ],
        "faq": [
            ("Čo pomáha na granátové jablko najviac?", "Rýchly oplach z rubu, lokálne predčistenie a kontrola pred sušením."),
            ("Prečo ostal ružový tieň?", "Pigment sa úplne neuvoľnil alebo sa škvrna zafixovala teplom."),
            ("Môžem použiť rovnaký postup na čerešne?", "Princíp je podobný, ale vždy sledujte materiál a farbu textilu."),
        ],
        "recommendation_intro": "Pri granátovom jablku ide najmä o pigment. Produkt používajte po oplachu a lokálnom predčistení, aby pranie dokončilo čistenie bez zafixovania ružovej mapy.",
        "product_text": "Vhodný na následné pranie tričiek, obrusov a detskej bielizne po oplachu ovocnej šťavy. Pri jemných materiáloch najprv rešpektujte štítok.",
        "links": [
            ("/n/ako-vyprat-ovocne-skvrny-z-jahod-cucoriedok-a-malin", "Ako vyprať ovocné škvrny z jahôd, čučoriedok a malín"),
            ("/n/ako-odstranit-ceresne-z-detskeho-tricka-a-letnych-siat", "Ako odstrániť čerešne z detského trička a letných šiat"),
            ("/n/ako-vyprat-stare-skvrny-kompletny-sprievodca-pre-ciste-oblecenie", "Ako vyprať staré škvrny"),
        ],
    },
    "iodine": {
        "marker": "Detailnejší postup na jód, dezinfekciu a škvrny po ošetrení",
        "problem": "jód a niektoré dezinfekcie farbia intenzívne a pri rozotieraní sa škvrna môže zväčšiť",
        "main_textile": "tričko, pyžamo, uterák a oblečenie po ošetrení",
        "avoid": "silné trenie, teplo a miešanie neoverených chemických postupov",
        "diagnosis": [
            "<strong>Nerozotierať:</strong> pigment sa môže rozšíriť do väčšej mapy.",
            "<strong>Podložiť savou vrstvou:</strong> cieľ je odobrať farbu z textilu, nie ju zatlačiť ďalej.",
            "<strong>Kontrolovať materiál:</strong> biela bavlna, farebné tričko a jemná blúzka potrebujú inú opatrnosť.",
            "<strong>Bez experimentov na pokožke:</strong> článok rieši textil po ošetrení, nie zdravotný postup.",
        ],
        "state_rows": [
            ("čerstvá kvapka", "odsávať a nešúchať", "zmenšuje riziko rozšírenia"),
            ("väčší fľak", "pracovať od okraja ku stredu", "podložiť savou vrstvou"),
            ("škvrna po praní", "nesušiť horúco", "zopakovať lokálne"),
            ("jemný textil", "testovať a postupovať mierne", "riziko zmeny farby"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "podložiť, odoberať pigment, prať podľa štítku", "bavlna rýchlo saje"),
            ("pyžamo", "riešiť škvrnu a dobrý oplach", "dlhý kontakt s pokožkou"),
            ("uterák", "predčistiť lokálne", "froté drží tekutinu hlbšie"),
            ("farebné oblečenie", "test stálosti farby", "silný zásah môže vytvoriť svetlú mapu"),
        ],
        "sections": [
            ("Ako odstrániť jód z oblečenia bez zväčšenia fľaku", "Textil podložte savou handričkou a škvrnu nešúchajte do strán. Pri čerstvej kvapke je cieľom odobrať čo najviac farby do podkladu. Pracujte od okraja ku stredu a priebežne posúvajte čistú časť handričky.", "Až potom miesto lokálne predčistite a perte podľa štítku. Ak zostane žltý alebo hnedý tieň, nesušte horúco a postup zopakujte."),
            ("Dezinfekcia na pyžame alebo uteráku", "Po ošetrení sa dezinfekcia môže dostať na pyžamo, uterák alebo posteľnú bielizeň. Pri uteráku je problém savosť froté, pri pyžame kontakt s pokožkou. V oboch prípadoch pomáha neodkladať mokrý kus v koši a škvrnu pred praním skontrolovať.", "Pri uterákoch sledujte aj dávkovanie a oplach. Priveľa produktu alebo aviváže môže zhoršiť savosť a pocit čistoty."),
            ("Prečo sa fľak zväčší", "Pri jóde a farebnej dezinfekcii sa škvrna zväčší najmä vtedy, keď ju začnete trieť vodorovne po látke. Pigment sa presunie mimo pôvodné miesto a vznikne mapa. Preto pomáha savý podklad a práca po malých krokoch.", "Podobný princíp platí pri zvýrazňovači alebo iných atramentových škvrnách. Nadväzuje článok <a href=\"/n/ako-odstranit-zvyraznovac-z-rukava-mikiny-a-skolskeho-tricka\">ako odstrániť zvýrazňovač z rukáva mikiny</a>."),
            ("Čo ak škvrna zostala po praní", "Ak škvrna zostala po praní, nesušte ju teplom. Znovu podložte savou vrstvou, predčistite lokálne a perte podľa materiálu. Pri farebnom textile najprv testujte, aby ste nevytvorili vyblednutý kruh.", "Pri starých alebo už fixovaných škvrnách môže byť výsledok obmedzený. Vtedy je lepšie zachovať látku než ju poškodiť agresívnym postupom."),
            ("Bezpečnostná poznámka pri dezinfekcii", "Nemiešajte rôzne chemické produkty naslepo. Niektoré kombinácie nie sú vhodné pre textil ani pre domáce prostredie. Ak neviete, čo bolo na látke použité, postupujte opatrne: odobrať prebytok, opláchnuť podľa štítku a až potom prať.", "Pri citlivom alebo drahom textile je čistiareň rozumnejšia než silné domáce experimentovanie."),
        ],
        "depth": [
            ("Jód na bielom a farebnom textile", "Na bielom textile je škvrna výrazná, ale postup môže byť odlišný než pri farebnom tričku. Farebné oblečenie najprv testujte na skrytom mieste, aby ste nepoškodili pôvodnú farbu.", "Pri bielom textile sa nespoliehajte iba na vyššiu teplotu. Teplo bez predčistenia môže zvyšok zafixovať."),
            ("Keď je škvrna pri posteľnej bielizni", "Ak sa jód alebo dezinfekcia dostane na posteľnú bielizeň, skontrolujte aj druhú vrstvu látky. Tekutina sa môže prepíjať a po zložení obliečky sa škvrna prenesie ďalej.", "Pred praním je lepšie riešiť konkrétne miesto než prať celú veľkú dávku s neviditeľne rozpitým pigmentom."),
        ],
        "faq": [
            ("Prečo sa škvrna od jódu zväčšila?", "Pravdepodobne sa rozotrela do strán. Nabudúce ju podložte a pracujte od okraja ku stredu."),
            ("Môžem miešať čistiace prípravky na dezinfekciu?", "Nie naslepo. Držte sa bezpečného postupu podľa štítku a materiálu."),
            ("Čo ak škvrna ostala po praní?", "Nesušte horúco. Zopakujte lokálne predčistenie a kontrolu pri dennom svetle."),
        ],
        "recommendation_intro": "Pri jóde a dezinfekcii je najdôležitejšie nerozšíriť pigment. Prací produkt používajte až po opatrnom odobratí farby a lokálnom predčistení.",
        "product_text": "Vhodný na následné pranie tričiek, uterákov a bežných textílií po opatrnom predčistení farebnej škvrny. Pri jemných látkach najprv testujte.",
        "links": [
            ("/n/ako-odstranit-zvyraznovac-z-rukava-mikiny-a-skolskeho-tricka", "Ako odstrániť zvýrazňovač z rukáva mikiny a školského trička"),
            ("/n/casto-kladene-otazky-skvrny-na-obleceni-po-prani", "Škvrny na oblečení po praní"),
            ("/n/ako-vyprat-stare-skvrny-kompletny-sprievodca-pre-ciste-oblecenie", "Ako vyprať staré škvrny"),
        ],
    },
}


def build_related_links(links):
    items = "\n".join(f'<li><a href="{href}">{label}</a></li>' for href, label in links)
    items += '\n<li><a href="/n/ako-vybrat-praci-gel-podla-typu-bielizne">Ako vybrať prací gél podľa typu bielizne</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


def build_expansion(topic):
    config = TOPICS[topic]
    state_table = table(["Stav problému", "Čo urobiť", "Poznámka"], config["state_rows"])
    textile_table = table(["Textil", "Postup", "Prečo"], config["textile_rows"])
    sections = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["sections"])
    depth = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["depth"])
    faq = "\n".join(f"<h3>{question}</h3>\n<p>{answer}</p>" for question, answer in config["faq"])
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["problem"].capitalize()}. Preto sa neoplatí začínať iba hlavným pracím cyklom. Najprv treba zistiť, či riešite pevný zvyšok, mastnotu, pigment, soľ, piesok alebo kombináciu viacerých problémov.</p>
        <p>Pri textile ako {config["main_textile"]} je rozhodujúci aj materiál a konštrukcia. Detské veci môžu mať potlač, lemy, elastan alebo viac vrstiev. Najväčšie riziko je zafixovať škvrnu nevhodným prvým krokom: {config["avoid"]}.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu škvrny alebo znečistenia</h2>
        {state_table}
        <h2>Postup podľa typu textilu</h2>
        {textile_table}
        {sections}
        <h2>Odbornejší pohľad: najprv mechanika, potom chémia, až potom pranie</h2>
        <p>Pri škvrnách v domácnosti často rozhoduje poradie krokov. Pevné zvyšky treba najprv odstrániť mechanicky, pigmenty odoberať opatrne, mastné filmy predčistiť lokálne a až potom prať celý kus. Ak sa poradie preskočí, prací cyklus môže problém rozšíriť alebo skryť iba dočasne.</p>
        <p>Praktické databázy škŕn odporúčajú posudzovať typ škvrny, typ textilu a kontrolu pred sušením. Užitočný odborný zdroj k princípom domáceho predčistenia je <a rel="noopener" href="https://extension.illinois.edu/global/stain-solutions" target="_blank">Illinois Extension Stain Solutions</a>.</p>
        {depth}
        <h2>Kontrolný checklist pred praním</h2>
        <p>Pred praním skontrolujte, či na látke nezostal objem škvrny, či miesto nie je lepkavé alebo mastné, či sa pigment nerozpíja do okolia a či štítok povoľuje zvolený program. Tento krok trvá krátko, ale často rozhoduje o výsledku.</p>
        <p>Pri detských a outdoorových veciach skontrolujte aj vrecká, švy, lemy, rukávy a miesta pri zapínaní. Práve tam sa držia zvyšky, ktoré potom bežný cyklus nemusí vypláchnuť.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po praní zostal slabý tieň, klzký pocit alebo tvrdšie miesto, nesušte textil horúco. Zopakujte lokálne predčistenie a perte podľa štítku. Opakovaný mierny postup býva bezpečnejší než jeden agresívny zásah.</p>
        <p>Ak látka púšťa farbu, mení povrch alebo ide o drahší kus, zastavte domáce experimentovanie skôr. Cieľom je zachovať oblečenie použiteľné, nie odstrániť škvrnu za cenu poškodenia materiálu.</p>
        <h2>Ako predísť zafixovaniu pri sušení</h2>
        <p>Mokrá látka môže vyzerať čistejšie, než v skutočnosti je. Pigment, mastnota alebo lepkavý film sa často ukážu až po preschnutí. Preto kontrolujte miesto pri dennom svetle a sušičku použite až vtedy, keď je výsledok čistý.</p>
        <p>Ak máte pochybnosť, nechajte kus vyschnúť voľne bez tepla a potom sa rozhodnite, či treba ďalšie lokálne predčistenie. Tento postup je pomalší, ale výrazne znižuje riziko trvalej mapy.</p>
        <h2>Domáca rutina pri opakovaných škvrnách</h2>
        <p>Ak sa podobná škvrna objavuje pravidelne, nespoliehajte sa na náhodné riešenie pri každom praní. Vytvorte si jednoduchú rutinu: rýchla kontrola pred košom na bielizeň, odstránenie pevných zvyškov, lokálne predčistenie, pranie v primerane plnom bubne a kontrola pred sušením. Pri deťoch, výletoch, pláži a domácom ošetrovaní je práve tento systém dôležitejší než silnejší zásah na poslednú chvíľu.</p>
        <p>Do jednej dávky nedávajte spolu kusy s nevyriešenou mastnotou, pigmentom a pieskom. Každý typ znečistenia sa správa inak a môže ovplyvniť ostatné oblečenie. Ak najprv vyriešite najproblematickejšie miesto, prací cyklus potom dokončí bežnú hygienu a sviežosť oveľa spoľahlivejšie.</p>
        <p>Pri opakovaných škvrnách si všímajte aj to, kedy problém vzniká: pri jedle, pri hraní vonku, pri ošetrovaní pokožky alebo pri balení mokrých vecí do tašky. Prevencia potom nie je abstraktná rada, ale konkrétny zvyk pred praním.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Rýchla zásada</h2>
        <p>Najprv odstráňte konkrétny problém zo škvrny a až potom perte celý kus. Vôňa, aviváž ani dlhý program nenahradia predčistenie, ak v látke zostal pigment, mastnota, piesok alebo lepkavý zvyšok.</p>
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 15 kids/cosmetic stain articles.")
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
                "wave": "retrofit-wave-15-kids-cosmetic-stains-five",
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
            mcp_updates.append({"post_id": item["post_id"], "slug": item["slug"], "url": item["url"], "mcp_result": result.get("result", result)})
            time.sleep(args.sleep)

    MCP_RESULTS.write_text(
        json.dumps(
            {
                "project": "VEVO_CONTENT",
                "wave": "retrofit-wave-15-kids-cosmetic-stains-five",
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
