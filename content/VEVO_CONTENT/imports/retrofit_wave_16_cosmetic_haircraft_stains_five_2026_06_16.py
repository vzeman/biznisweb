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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-16-cosmetic-haircraft-stains-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-16-cosmetic-haircraft-stains-five-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-odstranit-ruz-z-kosele-salu-a-latkovej-servitky",
        "post_id": "2148",
        "url": "https://www.vevo.sk/n/ako-odstranit-ruz-z-kosele-salu-a-latkovej-servitky",
        "topic": "lipstick",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-prat-oblecenie-po-kadernictve-od-vlasov-farby-a-lakov",
        "post_id": "2183",
        "url": "https://www.vevo.sk/n/ako-prat-oblecenie-po-kadernictve-od-vlasov-farby-a-lakov",
        "topic": "hair_salon",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-odstranit-podkladovy-krem-z-goliera-bluzky-a-kosele",
        "post_id": "2147",
        "url": "https://www.vevo.sk/n/ako-odstranit-podkladovy-krem-z-goliera-bluzky-a-kosele",
        "topic": "foundation",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-plastelinu-z-teplakov-koberca-a-potahu",
        "post_id": "2169",
        "url": "https://www.vevo.sk/n/ako-odstranit-plastelinu-z-teplakov-koberca-a-potahu",
        "topic": "plasticine",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-krem-na-ruky-z-rukavov-svetra-a-deky",
        "post_id": "2178",
        "url": "https://www.vevo.sk/n/ako-odstranit-krem-na-ruky-z-rukavov-svetra-a-deky",
        "topic": "hand_cream",
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
        <h2 style="margin-top: 0;">Odporúčané riešenie pre pranie po lokálnom predčistení</h2>
        <p>{config["recommendation_intro"]}</p>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; margin: 14px 0;">
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>{config["product_text"]}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
        </div>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Kategória pracie gély</h3>
        <p>Pri kozmetických, mastných a pigmentových škvrnách je dobré mať doma šetrný prací gél a používať ho až po rozumnom lokálnom predčistení.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        </div>
        </div>
        """
    )


TOPICS = {
    "lipstick": {
        "marker": "Detailnejší postup na rúž, vosky, oleje a pigment na textile",
        "problem": "rúž kombinuje vosky, oleje a výrazný pigment, preto sa pri trení ľahko rozmaže do väčšej plochy",
        "main_textile": "košeľu, šál a látkovú servítku",
        "avoid": "horúcu vodu, silné trenie a sušičku pred kontrolou farebného tieňa",
        "diagnosis": [
            "<strong>Najprv nevtierať:</strong> rúž sa pri tlaku rozmaže do vlákien.",
            "<strong>Riešiť dve vrstvy:</strong> mastný voskový základ aj farebný pigment.",
            "<strong>Podložiť savou vrstvou:</strong> pri tenkej košeli sa škvrna môže prepíjať.",
            "<strong>Kontrola pred sušením:</strong> mastný tieň sa často ukáže až po praní.",
        ],
        "state_rows": [
            ("čerstvá stopa", "odobrať prebytok bez trenia", "pracovať od okraja ku stredu"),
            ("mastný obrys", "lokálne predčistiť", "vôňa nepomôže bez odmastenia"),
            ("pigment po praní", "nesušiť horúco, zopakovať lokálne", "teplo fixuje zvyšok"),
            ("jemný šál", "testovať na skrytom mieste", "farba a tvar môžu byť citlivé"),
        ],
        "textile_rows": [
            ("košeľa", "podložiť a predčistiť golier alebo manžetu", "tenká látka sa prepije"),
            ("šál", "bez krútenia a drsnej kefky", "jemný materiál mení tvar"),
            ("látková servítka", "pracovať od okraja", "pigment sa rozširuje do väzby"),
            ("sako alebo kabát", "zvážiť čistiareň", "konštrukcia môže byť neprateľná"),
        ],
        "sections": [
            ("Ako odstrániť rúž z košele", "Košeľu podložte čistou savou handričkou a prebytok rúžu odoberte bez vtierania. Ak je škvrna na golieri alebo manžete, pracujte pomaly, pretože látka je tam často viac namáhaná a znečistená aj potom alebo parfumom.", "Po lokálnom predčistení perte košeľu podľa štítku. Ak zostane mastný alebo farebný tieň, nesušte ju horúco a postup zopakujte."),
            ("Rúž na šáli a jemnej látke", "Šál môže byť z viskózy, hodvábu, polyesteru alebo jemnej zmesi. Pri takýchto materiáloch je nebezpečné silné trenie aj dlhé máčanie. Najprv testujte na menej viditeľnom mieste a pracujte po malých krokoch.", "Ak je šál drahý alebo má nestálu farbu, čistiareň je bezpečnejšia než agresívny domáci zásah."),
            ("Rúž na látkovej servítke", "Pri servítke býva škvrna často kombinovaná s jedlom, mastnotou alebo vínom. Najprv odoberte rúž, potom riešte mastnú časť a až následne perte celý kus. Ak je servítka biela, kontrolujte aj slabý ružový tieň.", "Pri podobných pigmentovo-mastných škvrnách nadväzuje článok <a href=\"/n/ako-odstranit-podkladovy-krem-z-goliera-bluzky-a-kosele\">ako odstrániť podkladový krém z goliera</a>."),
            ("Prečo rúž nejde dole ako obyčajná farba", "Rúž je navrhnutý tak, aby držal na perách, preto obsahuje zložky, ktoré odpudzujú bežnú vodu a zároveň nesú pigment. Samotný oplach nemusí stačiť. Najprv treba uvoľniť mastný film a až potom riešiť zvyšok farby.", "Podobný princíp platí pri maskare alebo make-upe. Súvisiaci postup je v článku <a href=\"/n/ako-odstranit-maskaru-z-uteraka-zupanu-a-bielej-osusky\">ako odstrániť maskaru z uteráka</a>."),
            ("Kontrola výsledku po praní", "Po praní skontrolujte miesto pri dennom svetle. Rúž môže zanechať slabý mastný kruh bez výraznej farby, ktorý sa naplno ukáže až po vysušení. Ak ho vidíte, textil ešte nedávajte do sušičky.", "Opakované jemné predčistenie je bezpečnejšie než prudké zvyšovanie teploty, najmä pri košeliach a šáloch."),
        ],
        "depth": [
            ("Rúž na bielom a farebnom textile", "Na bielom textile je pigment viditeľný okamžite, no farebný textil môže byť zradný: pigment rúžu zanikne v potlači a ostane iba mastná mapa. Preto kontrolujte aj dotyk, nie iba farbu.", "Pri farebných kusoch si najprv overte stálosť farby. Silný zásah môže vytvoriť svetlejšiu stopu okolo pôvodnej škvrny."),
            ("Keď je rúž zmiešaný s parfumom alebo jedlom", "Na golieri alebo servítke sa rúž často stretne s parfumom, krémom alebo tukom z jedla. Vtedy sa škvrna správa komplexnejšie a treba riešiť najprv mastný film.", "Ak ostáva aj vôňa, pridajte ju až na čistý textil. Vôňa nemá prekryť zvyšok vosku alebo tuku."),
        ],
        "faq": [
            ("Môžem rúž hneď vyprať?", "Lepšie je najprv odobrať prebytok a lokálne predčistiť mastno-pigmentovú časť."),
            ("Prečo ostal po rúži mastný kruh?", "Vosky a oleje sa neuvoľnili úplne. Miesto pred sušením predčistite znova."),
            ("Ako postupovať pri jemnom šáli?", "Testujte na skrytom mieste, netrite agresívne a pri drahom kúsku zvážte čistiareň."),
        ],
        "recommendation_intro": "Pri rúži je dôležité najprv uvoľniť voskovo-mastnú časť a pigment. Prací gél má pomôcť následnému praniu, nie prekryť zvyšok škvrny.",
        "product_text": "Vhodný na následné pranie košieľ, servítok a bežných textílií po lokálnom predčistení rúžu. Pri jemných šáloch vždy rozhoduje štítok.",
        "links": [
            ("/n/ako-odstranit-podkladovy-krem-z-goliera-bluzky-a-kosele", "Ako odstrániť podkladový krém z goliera blúzky a košele"),
            ("/n/ako-odstranit-maskaru-z-uteraka-zupanu-a-bielej-osusky", "Ako odstrániť maskaru z uteráka, županu a bielej osušky"),
            ("/n/ako-odstranit-lak-na-nechty-z-textilu-bez-rozmazania-skvrny", "Ako odstrániť lak na nechty z textilu"),
        ],
    },
    "hair_salon": {
        "marker": "Detailnejší postup na oblečenie po kaderníctve, vlasy, farbu a lak",
        "problem": "oblečenie po kaderníctve môže mať naraz drobné vlasy, vlasovú farbu, lak, sérum aj pach salónu",
        "main_textile": "tričko, golier košele, šatku a vrchné vrstvy po kaderníctve",
        "avoid": "prať všetko naraz bez vytrasenia vlasov a kontroly farebných škvŕn",
        "diagnosis": [
            "<strong>Najprv vlasy:</strong> drobné vlasy odstráňte pred praním, inak sa nalepia na ďalšie kusy.",
            "<strong>Farba je samostatný problém:</strong> pigment neprekrývajte vôňou.",
            "<strong>Lak a sérum:</strong> môžu vytvoriť film na golieri alebo šatke.",
            "<strong>Pach salónu:</strong> riešte až po odstránení zvyškov produktov.",
        ],
        "state_rows": [
            ("drobné vlasy", "vytriasť, prejsť valčekom alebo kefou", "pred praním"),
            ("vlasová farba", "lokálne riešiť pigment", "nesušiť horúco"),
            ("lak na vlasy", "uvoľniť film", "môže byť tuhý na dotyk"),
            ("salónny pach", "vetrať a prať primerane", "vôňa až na čistý textil"),
        ],
        "textile_rows": [
            ("tričko", "vytriasť vlasy a prať naruby", "vlasy sa držia na úplete"),
            ("košeľa", "skontrolovať golier", "tam sa drží lak a sérum"),
            ("šatka", "šetrne podľa štítku", "jemná látka drží vôňu"),
            ("uterák", "prať oddelene pri väčšom znečistení", "froté zachytí vlasy aj produkty"),
        ],
        "sections": [
            ("Ako pripraviť oblečenie po kaderníctve pred praním", "Oblečenie najprv vytraste mimo práčky. Drobné ostrihané vlasy sa v mokrom praní môžu nalepiť na úplet, uteráky alebo tmavé veci. Potom skontrolujte golier, ramená, šatku a miesta, kde sa dotýkali vlasové produkty.", "Ak je na látke viditeľná farba, riešte ju ako pigmentovú škvrnu ešte pred praním."),
            ("Ako odstrániť vlasy z trička a košele", "Drobné vlasy odstráňte valčekom, kefou alebo vytrasením. Pri tmavom oblečení je rozdiel viditeľný hneď. Až potom perte s podobnými materiálmi. Ak vlasy zostanú v práčke, môžu sa preniesť na ďalšiu dávku.", "Pri podobnom probléme so zvieracími chlpmi nadväzuje článok <a href=\"/n/ako-odstranit-chlpy-z-oblecenia-pri-prani-ked-mate-psa-alebo-macku\">ako odstrániť chlpy z oblečenia pri praní</a>."),
            ("Vlasová farba na oblečení", "Vlasová farba je rizikovejšia než bežný pach salónu. Ak je škvrna čerstvá, nešúchajte ju do strán. Podložte savú vrstvu, odoberajte pigment a pred praním ju riešte lokálne.", "Pri staršej alebo už zafixovanej farbe nemusí byť domáci výsledok úplný. Je lepšie postupovať opatrne než poškodiť látku."),
            ("Lak, sérum a film na golieri", "Lak na vlasy alebo sérum môžu na golieri vytvoriť tuhší alebo mastný film. Ten sa správa inak než samotné vlasy. Lokálne ho uvoľnite a až potom perte podľa štítku.", "K téme nadväzujú návody <a href=\"/n/ako-odstranit-lak-na-vlasy-z-goliera-kosele-a-satky\">ako odstrániť lak na vlasy</a> a <a href=\"/n/ako-odstranit-vlasove-serum-z-uteraka-a-goliera-kosele\">ako odstrániť vlasové sérum</a>."),
            ("Ako odstrániť pach salónu bez prevoňania naslepo", "Pach salónu býva kombinácia lakov, farieb, parfumácie a vlhkosti. Najprv oblečenie vyvetrajte a odstráňte zvyšky produktov. Potom perte v primerane veľkej dávke, aby sa textil dobre opláchol.", "Ak pach zostane aj po praní, skontrolujte sušenie a čistotu práčky. Silnejšia vôňa nemusí vyriešiť zvyšky produktu vo vlákne."),
        ],
        "depth": [
            ("Kadernícky plášť nestačí vždy", "Aj keď ste mali ochranný plášť, drobné vlasy a aerosól z produktov sa môžu dostať na golier, šatku alebo ramená. Preto má zmysel rýchla kontrola hneď po príchode domov.", "Čím skôr odlíšite vlasy, pigment a film, tým jednoduchšie nastavíte pranie."),
            ("Čo prať oddelene", "Ak je oblečenie výrazne znečistené farbou alebo lakom, neperte ho s jemnou bielizňou a uterákmi. Najprv riešte lokálne miesta a až potom ho pridajte k vhodnej dávke.", "Tým znížite riziko prenosu pigmentu, vlasov alebo tuhého filmu na ďalšie textílie."),
        ],
        "faq": [
            ("Treba oblečenie po kaderníctve prať hneď?", "Ak je na ňom farba, lak alebo veľa vlasov, áno, ale najprv ho vytraste a predčistite problémové miesta."),
            ("Ako odstrániť drobné vlasy?", "Pred praním použite vytrasenie, kefu alebo valček. Práčka nemá nahradiť tento krok."),
            ("Prečo oblečenie stále vonia po salóne?", "Môže v ňom zostať film z lakov, séra alebo farby, prípadne schlo príliš pomaly."),
        ],
        "recommendation_intro": "Pri oblečení po kaderníctve najprv odstráňte vlasy a lokálne zvyšky produktov. Prací gél potom pomôže vyprať textil bez zbytočného prevoňania problému.",
        "product_text": "Vhodný na následné pranie tričiek, košieľ a šatiek po vytrasení vlasov a lokálnom predčistení. Pri jemných šatkách sledujte štítok.",
        "links": [
            ("/n/ako-odstranit-lak-na-vlasy-z-goliera-kosele-a-satky", "Ako odstrániť lak na vlasy z goliera košele a šatky"),
            ("/n/ako-odstranit-vlasove-serum-z-uteraka-a-goliera-kosele", "Ako odstrániť vlasové sérum z uteráka a goliera košele"),
            ("/n/ako-odstranit-chlpy-z-oblecenia-pri-prani-ked-mate-psa-alebo-macku", "Ako odstrániť chlpy z oblečenia pri praní"),
        ],
    },
    "foundation": {
        "marker": "Detailnejší postup na podkladový krém, mastný pigment a golier",
        "problem": "podkladový krém je mastno-pigmentová škvrna, ktorá sa často usádza na golieri, pri výstrihu a na manžetách",
        "main_textile": "golier blúzky, košeľu a svetlé tričko",
        "avoid": "rozotieranie po golieri, sušičku pred kontrolou a príliš veľa produktu bez oplachu",
        "diagnosis": [
            "<strong>Mastný základ:</strong> najprv uvoľnite film, až potom riešte pigment.",
            "<strong>Golier je rizikový:</strong> kombinuje make-up, pot, parfum a maz.",
            "<strong>Svetlá blúzka:</strong> kontrolujte okraje škvrny pri dennom svetle.",
            "<strong>Jemný materiál:</strong> netrite kefkou bez testu.",
        ],
        "state_rows": [
            ("čerstvá stopa", "odsávať a nevtierať", "hlavne pri golieri"),
            ("mastný okraj", "predčistiť lokálne", "bežné pranie nemusí stačiť"),
            ("pigment po praní", "nesušiť horúco", "zopakovať predčistenie"),
            ("starší nános", "riešiť po vrstvách", "golier býva znečistený opakovane"),
        ],
        "textile_rows": [
            ("blúzka", "šetrne podľa štítku", "materiál môže byť viskóza alebo zmes"),
            ("košeľa", "golier predčistiť naruby aj z líca", "nános býva v ohybe"),
            ("tričko", "predčistiť výstrih", "úplet drží mastný film"),
            ("sako", "neprať doma bez štítku", "konštrukcia a podšívka sú citlivé"),
        ],
        "sections": [
            ("Ako odstrániť podkladový krém z goliera blúzky", "Golier podložte a škvrnu nešúchajte do strán. Podkladový krém sa často dostane do ohybu látky, kde sa zmieša s potom a parfumom. Najprv uvoľnite mastný film, potom riešte pigment a až následne perte celú blúzku.", "Pri viskózovej alebo jemnej blúzke sledujte štítok a nekrúťte mokrý materiál. Ak je blúzka drahá, postup testujte na vnútornej strane."),
            ("Podkladový krém na košeli", "Košeľa znesie viac než jemná blúzka, ale golier je stále namáhaný. Pred praním ho prejdite lokálne a košeľu perte naruby, aby sa trenie rozložilo. Ak zostal béžový tieň, nenechávajte ho zafixovať teplom.", "Pri podobných škvrnách nadväzuje článok <a href=\"/n/ako-odstranit-ruz-z-kosele-salu-a-latkovej-servitky\">ako odstrániť rúž z košele</a>."),
            ("Prečo make-up na golieri drží", "Make-up má držať na pokožke a vyrovnávať tón pleti. Preto obsahuje pigment a zložky, ktoré sa na textil lepia lepšie než obyčajný prach. Golier navyše zachytáva maz, pot a parfum.", "Bežné pranie bez predčistenia môže škvrnu zosvetliť, ale mastný film ostane. Ten sa potom prejaví ako sivší alebo béžový okraj."),
            ("Ako predísť opakovanému nánosu na golieri", "Ak sa škvrna opakuje, pomôže nechať make-up chvíľu usadiť pred obliekaním, používať šál opatrne a golier kontrolovať pred každým praním. Pri bielych košeliach je lepšie riešiť slabý nános pravidelne než čakať na výrazný kruh.", "Tento zvyk znižuje potrebu agresívneho čistenia a predlžuje životnosť golierov."),
            ("Kontrola po praní a žehlení", "Košeľu alebo blúzku nežehlete, kým je na golieri viditeľný mastný alebo béžový tieň. Žehlenie môže zvyšok zafixovať. Skontrolujte látku pri dennom svetle a podľa potreby predčistite znova.", "Pri škvrnách od parfumov na jemných látkach pomôže súvisiaci článok <a href=\"/n/ako-odstranit-parfumovy-flak-z-oblecenia-a-jemnych-latok\">ako odstrániť parfumový fľak</a>."),
        ],
        "depth": [
            ("Podkladový krém a biele košele", "Na bielej košeli nemusí byť problém iba farba make-upu, ale aj sivnutie goliera od opakovaného nánosu. Ak sa golier pred praním nerieši, znečistenie sa vrství.", "Pravidelné jemné predčistenie je šetrnejšie než nárazové silné čistenie po viacerých noseniach."),
            ("Podkladový krém na syntetike", "Syntetické tričká a elastické blúzky môžu držať mastný film inak než bavlna. Pri praní pomáha nepreplniť bubon a nepoužiť zbytočne krátky program, ktorý škvrnu len navlhčí.", "Ak textil po praní stále pôsobí klzko, problém je skôr v zvyšku filmu než vo vôni."),
        ],
        "faq": [
            ("Prečo make-up ostal na golieri po praní?", "Mastný film sa neuvoľnil pred cyklom alebo bol bubon príliš plný."),
            ("Môžem blúzku hneď žehliť?", "Nie, ak vidíte béžový tieň. Najprv ho riešte lokálne."),
            ("Ako často predčisťovať golier?", "Pri bielych košeliach a blúzkach radšej pravidelne po nosení, nie až po výraznom nánose."),
        ],
        "recommendation_intro": "Pri podkladovom kréme treba najprv uvoľniť mastno-pigmentový film. Až potom má zmysel bežné pranie celej košele alebo blúzky.",
        "product_text": "Vhodný na následné pranie košieľ, tričiek a bežných textílií po lokálnom predčistení make-upu. Pri jemnej blúzke najprv sledujte štítok.",
        "links": [
            ("/n/ako-odstranit-ruz-z-kosele-salu-a-latkovej-servitky", "Ako odstrániť rúž z košele, šálu a látkovej servítky"),
            ("/n/ako-odstranit-parfumovy-flak-z-oblecenia-a-jemnych-latok", "Ako odstrániť parfumový fľak z oblečenia a jemných látok"),
            ("/n/ako-odstranit-maskaru-z-uteraka-zupanu-a-bielej-osusky", "Ako odstrániť maskaru z uteráka, županu a bielej osušky"),
        ],
    },
    "plasticine": {
        "marker": "Detailnejší postup na plastelínu, mastný film a detské tvorenie",
        "problem": "plastelína je hmota s voskovo-mastnou zložkou a farbivom, takže po odstránení objemu môže zostať mastný alebo farebný tieň",
        "main_textile": "tepláky, koberec a čalúnený poťah",
        "avoid": "horúcu vodu, násilné vtieranie a pranie bez odstránenia hmoty",
        "diagnosis": [
            "<strong>Najprv objem:</strong> plastelínu odoberte mechanicky, nie praním.",
            "<strong>Mastný zvyšok:</strong> po hmote často zostane film.",
            "<strong>Koberec a poťah:</strong> nepremačajte výplň bez kontroly.",
            "<strong>Farebný tieň:</strong> riešte až po odstránení hmoty.",
        ],
        "state_rows": [
            ("mäkká plastelína", "odobrať tupou hranou", "nevtláčať do vlákna"),
            ("zaschnutý zvyšok", "uvoľňovať po častiach", "pozor na vytrhnutie vlasu"),
            ("mastný film", "lokálne predčistiť", "kontrola pred sušením"),
            ("farbivo", "riešiť ako pigment", "až po odstránení objemu"),
        ],
        "textile_rows": [
            ("tepláky", "odobrať hmotu a prať naruby", "úplet sa dá vydrať"),
            ("koberec", "pracovať povrchovo", "výplň nesmie zbytočne premoknúť"),
            ("poťah", "podľa štítku a výplne", "nie každý poťah je prateľný"),
            ("detské tričko", "kontrola potlače", "netrieť tvrdou kefkou"),
        ],
        "sections": [
            ("Ako odstrániť plastelínu z teplákov", "Tepláky najprv natiahnite tak, aby ste videli štruktúru úpletu, a plastelínu odoberajte tupou hranou. Neťahajte ju cez látku do strán. Keď je objem preč, riešte mastný alebo farebný zvyšok lokálne.", "Po predčistení perte tepláky naruby a bez preplnenia bubna. Ak zostane mastný tieň, nesušte horúco."),
            ("Plastelína v koberci", "Pri koberci pracujte po malých častiach a nepremačajte ho. Najprv odstráňte hmotu z povrchu. Ak zostane film, použite len primerané množstvo čistiaceho roztoku a priebežne odsávajte vlhkosť.", "Ak je koberec citlivý alebo ide o veľkú škvrnu, je bezpečnejšie profesionálne čistenie."),
            ("Plastelína na čalúnenom poťahu", "Poťah môže mať výplň, ktorá dlho schne. Preto neaplikujte veľa vody naraz. Najprv zistite, či je poťah snímateľný a prateľný. Ak nie, čistite iba povrchovo a opatrne.", "Pri podobnej lepkavej hmote nadväzuje článok <a href=\"/n/ako-odstranit-sliz-z-detskeho-tricka-a-deky-bez-lepkavych-zvyskov\">ako odstrániť sliz z detského trička a deky</a>."),
            ("Prečo plastelína necháva mastný tieň", "Plastelína má držať tvar a byť tvárna, preto často obsahuje mastnejšie alebo voskové zložky. Tie sa môžu oddeliť od farebnej hmoty a zostať v textile aj po odstránení viditeľného kúsku.", "Preto sa škvrna rieši v dvoch krokoch: najprv hmota, potom mastný film a pigment."),
            ("Ako predísť prenosu plastelíny do práčky", "Do práčky nedávajte tepláky s nalepenými kúskami plastelíny. Hmota sa môže rozotrieť na ďalšie oblečenie alebo zostať v záhyboch. Pred praním skontrolujte vrecká, kolená a spodné lemy.", "Pri tvorení s deťmi súvisí aj článok <a href=\"/n/ako-odstranit-lepidlo-z-oblecenia-po-tvoreni-s-detmi\">ako odstrániť lepidlo z oblečenia</a>."),
        ],
        "depth": [
            ("Farebná plastelína na svetlom textile", "Na svetlom textile je pigment viditeľný aj vtedy, keď hmota už zmizla. Kontrolujte látku pred sušením a nenechajte zvyšok zafixovať teplom.", "Ak sa farba rozpila do okolia, pracujte od okraja ku stredu a podložte savú vrstvu."),
            ("Detské tvorenie a pranie po aktivitách", "Po tvorení s deťmi sa často mieša plastelína, lepidlo, farby a omrvinky. Oblečenie pred praním nehoďte do jednej dávky bez kontroly.", "Najprv oddeľte hmoty od pigmentov a mastných zvyškov. Pranie bude účinnejšie a šetrnejšie."),
        ],
        "faq": [
            ("Môžem plastelínu vyprať rovno v práčke?", "Nie je to vhodné. Najprv odstráňte hmotu mechanicky a až potom perte."),
            ("Prečo zostal mastný fľak?", "V textile zostala voskovo-mastná zložka plastelíny. Treba ju predčistiť lokálne."),
            ("Ako postupovať pri koberci?", "Nepremáčajte ho. Odoberte hmotu a čistite povrchovo po malých úsekoch."),
        ],
        "recommendation_intro": "Pri plastelíne má prací produkt zmysel až po odstránení hmoty. Najprv dostaňte z látky objem, potom riešte mastný film a pigment.",
        "product_text": "Vhodný na následné pranie teplákov, tričiek a bežných textílií po mechanickom odstránení plastelíny. Pri koberci a poťahu sledujte materiál.",
        "links": [
            ("/n/ako-odstranit-sliz-z-detskeho-tricka-a-deky-bez-lepkavych-zvyskov", "Ako odstrániť sliz z detského trička a deky"),
            ("/n/ako-odstranit-lepidlo-z-oblecenia-po-tvoreni-s-detmi", "Ako odstrániť lepidlo z oblečenia po tvorení s deťmi"),
            ("/n/ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu", "Ako vyprať voskovky z peračníka a textilného obalu"),
        ],
    },
    "hand_cream": {
        "marker": "Detailnejší postup na krém na ruky, mastné rukávy a deky",
        "problem": "krém na ruky zanecháva mastný film, ktorý sa často prenesie na rukávy, sveter, deku alebo pléd",
        "main_textile": "rukávy svetra, deku, pléd a pyžamo",
        "avoid": "sušičku pred kontrolou a pranie bez lokálneho odmastenia",
        "diagnosis": [
            "<strong>Mastný film:</strong> škvrna môže byť viditeľná až po vysušení.",
            "<strong>Rukávy:</strong> krém sa hromadí pri manžetách a lakťoch.",
            "<strong>Deka:</strong> hrubší textil drží mastnotu hlbšie.",
            "<strong>Jemný sveter:</strong> postup musí rešpektovať materiál.",
        ],
        "state_rows": [
            ("čerstvý krém", "odsajte prebytok", "netrieť do väčšej mapy"),
            ("mastný rukáv", "lokálne predčistiť", "hlavne manžety"),
            ("deka alebo pléd", "riešiť povrch aj hĺbku vlákna", "nepreplniť práčku"),
            ("sveter", "podľa materiálu", "vlna a kašmír sú citlivé"),
        ],
        "textile_rows": [
            ("bavlnená mikina", "predčistiť manžety", "znesie viac mechaniky"),
            ("vlnený sveter", "šetrne podľa štítku", "nekrútiť a nesušiť horúco"),
            ("deka", "prať s priestorom v bubne", "hrúbka potrebuje oplach"),
            ("pyžamo", "riešiť kontakt s pokožkou", "zvyšky produktu môžu dráždiť"),
        ],
        "sections": [
            ("Ako odstrániť krém na ruky z rukávov", "Rukávy najprv skontrolujte pri manžetách a lakťoch. Krém sa tam hromadí postupne a nemusí vyzerať ako výrazná škvrna. Miesto lokálne predčistite a až potom perte celý kus.", "Ak ide o bavlnenú mikinu, postup je jednoduchší. Pri svetri rozhoduje materiál a štítok."),
            ("Krém na ruky na svetri", "Sveter nečistite ako obyčajné tričko. Ak je vlnený, kašmírový alebo jemný, používajte minimum mechaniky a nesušte ho horúco. Mastný film sa dá riešiť, ale poškodený tvar svetra sa opravuje ťažko.", "Pri vlnených veciach nadväzuje článok <a href=\"/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni\">ako prať vlnený sveter</a>."),
            ("Krém na deke alebo pléde", "Deka drží mastnotu hlbšie než tenké tričko. Ak je škvrna na mieste, kde si často krémujete ruky, môže ísť o opakovaný nános. Pred praním miesto predčistite a deku perte tak, aby mala v bubne priestor na oplach.", "Pri vlnených dekách je postup citlivejší; pomôže článok <a href=\"/n/ako-prat-vlneny-pled-a-deku-bez-zrazenia\">ako prať vlnený pléd a deku</a>."),
            ("Prečo krém po praní stále vidno", "Krém na ruky obsahuje zložky, ktoré majú zostať na pokožke. Na textile preto môžu vytvoriť film, ktorý bežný program iba čiastočne uvoľní. Ak bol bubon preplnený alebo dávka krátka, film zostane.", "Riešením je lokálne predčistenie, primerané dávkovanie a dobrý oplach."),
            ("Ako predísť mastným rukávom", "Po krémovaní nechajte ruky chvíľu vstrebať, najmä pred obliekaním svetra, pyžama alebo pred prikrytím dekou. Pri domácich textíliách sa mastný nános tvorí postupne, preto ho riešte skôr, než vznikne tmavšia mapa.", "Pri podobných mastiach nadväzuje článok <a href=\"/n/ako-odstranit-zinkovu-mast-z-detskeho-body-a-prebalovacej-podlozky\">ako odstrániť zinkovú masť</a>."),
        ],
        "depth": [
            ("Krém a parfumované produkty", "Niektoré krémy majú výraznú vôňu, ktorá sa na deke alebo svetri drží dlho. Ak textil nevonia čisto, najprv riešte mastný film a sušenie, až potom pridávajte vôňu do prania.", "Silná vôňa môže zakryť problém na prvý deň, ale mastný zvyšok zostane v látke."),
            ("Opakovaný nános na manžetách", "Ak sa mastnota vracia stále na rovnakom mieste, nejde o jednorazovú škvrnu, ale o rutinu používania. Manžety a rukávy preto kontrolujte pred praním pravidelne.", "Krátke lokálne predčistenie po niekoľkých noseniach je šetrnejšie než silné čistenie po dlhom nánose."),
        ],
        "faq": [
            ("Prečo krém na ruky zanechá mastný fľak?", "Obsahuje zložky, ktoré ostávajú na pokožke a na textile vytvoria film."),
            ("Môžem sveter prať horúco, aby sa krém uvoľnil?", "Nie bez štítku. Pri vlne a jemných materiáloch hrozí zrazenie alebo deformácia."),
            ("Ako čistiť deku od krému?", "Predčistite miesto, nepreplňte bubon a sušte podľa materiálu."),
        ],
        "recommendation_intro": "Pri kréme na ruky je najdôležitejšie odstrániť mastný film ešte pred hlavným praním. Produkt má pomôcť čistote, nie prekryť mastnotu vôňou.",
        "product_text": "Vhodný na následné pranie rukávov, pyžám a bežných textílií po lokálnom predčistení. Pri vlne, kašmíre a dekách sledujte štítok.",
        "links": [
            ("/n/ako-odstranit-zinkovu-mast-z-detskeho-body-a-prebalovacej-podlozky", "Ako odstrániť zinkovú masť z detského body a prebaľovacej podložky"),
            ("/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni", "Ako prať vlnený sveter, keď zapácha po nosení"),
            ("/n/ako-prat-vlneny-pled-a-deku-bez-zrazenia", "Ako prať vlnený pléd a deku bez zrazenia"),
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
    sections = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["sections"])
    depth = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["depth"])
    faq = "\n".join(f"<h3>{question}</h3>\n<p>{answer}</p>" for question, answer in config["faq"])
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["problem"].capitalize()}. Preto sa neoplatí začínať iba hlavným pracím cyklom. Najprv treba rozlíšiť, či riešite pigment, mastný film, pevný zvyšok, produktový nános alebo kombináciu viacerých vrstiev.</p>
        <p>Pri textile ako {config["main_textile"]} rozhoduje aj materiál a konštrukcia. Golier, manžeta, šál, deka alebo poťah neznášajú rovnaké trenie a teplotu. Najväčšie riziko je zafixovať škvrnu nevhodným prvým krokom: {config["avoid"]}.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu škvrny</h2>
        {state_table}
        <h2>Postup podľa typu textilu</h2>
        {textile_table}
        {sections}
        <h2>Odbornejší pohľad: prečo kozmetické a tvorivé škvrny potrebujú predčistenie</h2>
        <p>Kozmetika, vlasové produkty, krémy a modelovacie hmoty sú navrhnuté tak, aby držali na pokožke, vlasoch alebo v tvare. Na textile preto často vytvárajú film, ktorý sa pri bežnom praní nemusí úplne uvoľniť. Ak sa škvrna iba namočí a roztočí v bubne, môže sa rozšíriť alebo zostať ako bledý tieň.</p>
        <p>Praktické databázy škvŕn odporúčajú posudzovať typ škvrny, typ textilu a kontrolu pred sušením. Užitočný odborný zdroj k princípom domáceho predčistenia je <a rel="noopener" href="https://extension.illinois.edu/global/stain-solutions" target="_blank">Illinois Extension Stain Solutions</a>.</p>
        {depth}
        <h2>Kontrolný checklist pred praním</h2>
        <p>Pred praním skontrolujte, či na látke nezostal objem škvrny, či miesto nie je mastné alebo lepkavé, či sa pigment nerozpíja a či štítok povoľuje zvolený program. Pri golieri, rukávoch, šáloch a dekách sledujte aj rubovú stranu, lemy a švy.</p>
        <p>Do jednej dávky nedávajte kusy s nevyriešenou mastnotou, pigmentom a pevnými zvyškami. Každý typ znečistenia sa správa inak a môže ovplyvniť ostatné oblečenie. Ak najprv vyriešite najproblematickejšie miesto, pranie potom dokončí bežnú hygienu a sviežosť spoľahlivejšie.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po praní zostal slabý tieň, klzký pocit, tvrdší povrch alebo pigmentový okraj, nesušte textil horúco. Zopakujte lokálne predčistenie a perte podľa štítku. Opakovaný mierny postup býva bezpečnejší než jeden agresívny zásah.</p>
        <p>Ak látka púšťa farbu, mení povrch alebo ide o drahší kus, zastavte domáce experimentovanie skôr. Cieľom je zachovať oblečenie použiteľné, nie odstrániť škvrnu za cenu poškodenia materiálu.</p>
        <h2>Ako predísť zafixovaniu pri sušení</h2>
        <p>Mokrá látka môže vyzerať čistejšie, než v skutočnosti je. Pigment, mastnota alebo film z produktu sa často ukážu až po preschnutí. Preto kontrolujte miesto pri dennom svetle a sušičku použite až vtedy, keď je výsledok čistý.</p>
        <p>Ak máte pochybnosť, nechajte kus vyschnúť voľne bez tepla a potom sa rozhodnite, či treba ďalšie lokálne predčistenie. Tento postup je pomalší, ale znižuje riziko trvalej mapy.</p>
        <h2>Domáca rutina pri opakovaných škvrnách</h2>
        <p>Ak sa podobná škvrna opakuje, vytvorte si jednoduchú rutinu: rýchla kontrola pred košom na bielizeň, odstránenie povrchových zvyškov, lokálne predčistenie, pranie v primerane plnom bubne a kontrola pred sušením. Pri kozmetike, kaderníctve a detskom tvorení je práve poradie krokov rozhodujúce.</p>
        <p>Všímajte si aj to, kedy problém vzniká: pri obliekaní po make-upe, po kaderníctve, pri tvorení s deťmi alebo pri večernom krémovaní. Prevencia potom nie je všeobecná rada, ale konkrétny zvyk pred praním.</p>
        <h2>Čo sledovať po druhom praní</h2>
        <p>Ak sa škvrna po druhom šetrnom praní stále vracia, sledujte, či ide o farbu, mastnotu alebo zmenu povrchu látky. Farebný tieň potrebuje iný prístup než klzký film a vydratý povrch už nie je škvrna, ale poškodenie materiálu. Táto kontrola pomáha rozhodnúť, či má zmysel ďalšie predčistenie alebo je lepšie postup zastaviť.</p>
        <p>Pri opakovaných kozmetických a tvorivých škvrnách sa oplatí upraviť aj bežné návyky: obliekať košeľu až po usadení make-upu, po kaderníctve vytriasť oblečenie hneď doma, pri tvorení používať zásteru a pri krémovaní nechať ruky chvíľu vstrebať. Tak sa zníži počet tvrdých zásahov pri praní.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Rýchla zásada</h2>
        <p>Najprv odstráňte konkrétny problém zo škvrny a až potom perte celý kus. Vôňa, aviváž ani dlhý program nenahradia predčistenie, ak v látke zostal pigment, mastnota, film alebo pevný zvyšok.</p>
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 16 cosmetic/hair/craft stain articles.")
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
                "wave": "retrofit-wave-16-cosmetic-haircraft-stains-five",
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
                "wave": "retrofit-wave-16-cosmetic-haircraft-stains-five",
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
