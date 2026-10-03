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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-23-pets-paprika-hair-residue-holes-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-23-pets-paprika-hair-residue-holes-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-prat-textilie-v-domacnosti-so-psom-pocas-plznutia",
        "post_id": "2164",
        "url": "https://www.vevo.sk/n/ako-prat-textilie-v-domacnosti-so-psom-pocas-plznutia",
        "topic": "dog_textiles",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-cervenu-papriku-z-tricka-a-kuchynskej-utierky",
        "post_id": "2173",
        "url": "https://www.vevo.sk/n/ako-odstranit-cervenu-papriku-z-tricka-a-kuchynskej-utierky",
        "topic": "red_paprika",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-odstranit-chlpy-z-oblecenia-pri-prani-ked-mate-psa-alebo-macku",
        "post_id": "2163",
        "url": "https://www.vevo.sk/n/ako-odstranit-chlpy-z-oblecenia-pri-prani-ked-mate-psa-alebo-macku",
        "topic": "pet_hair",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-odstranit-biele-smuhy-od-pracieho-prasku-z-cierneho-oblecenia",
        "post_id": "2186",
        "url": "https://www.vevo.sk/n/ako-odstranit-biele-smuhy-od-pracieho-prasku-z-cierneho-oblecenia",
        "topic": "white_streaks",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-predist-dierkam-v-trickach-po-prani-a-suseni",
        "post_id": "2187",
        "url": "https://www.vevo.sk/n/ako-predist-dierkam-v-trickach-po-prani-a-suseni",
        "topic": "shirt_holes",
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
        <h2 style="margin-top: 0;">Odporúčané riešenie pre šetrné pranie</h2>
        <p>{config["recommendation_intro"]}</p>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; margin: 14px 0;">
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>{config["product_text"]}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
        </div>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Kategória pracie gély</h3>
        <p>Pri chlpovom znečistení, pigmentových škvrnách, zvyškoch prášku a citlivých tričkách pomáha primeraná dávka, nepreplnený bubon a kontrola pred sušením.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        </div>
        </div>
        """
    )


def build_related_links(links):
    items = "\n".join(f'<li><a href="{href}">{label}</a></li>' for href, label in links)
    items += '\n<li><a href="/n/ako-vybrat-praci-gel-podla-typu-bielizne">Ako vybrať prací gél podľa typu bielizne</a></li>'
    items += '\n<li><a href="/n/casto-kladene-otazky-skvrny-na-obleceni-po-prani">Škvrny na oblečení po praní</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


TOPICS = {
    "dog_textiles": {
        "marker": "Detailnejší postup na textílie v domácnosti so psom počas pĺznutia",
        "problem": "textílie v domácnosti so psom počas pĺznutia neriešia iba chlpy, ale aj prach, kožný maz, pach, pelech, deky, poťahy a práčku, do ktorej sa chlpy opakovane vracajú",
        "scope": "dekách, pelechu, poťahoch na sedačke, návlekoch, uterákoch po kúpaní psa, koberčekoch, obliečkach a oblečení majiteľa",
        "avoid": "hádzať silno chlpaté deky rovno do práčky, preplniť bubon, prať pelech s osobnou bielizňou a sušiť textil, kým je v ňom pach alebo zvyšky chlpov",
        "diagnosis": [
            "<strong>Najprv mechanicky odstrániť chlpy:</strong> práčka nie je vysávač a chlpy sa môžu presunúť na ďalšiu dávku.",
            "<strong>Pelech perte oddelene:</strong> drží pach, mastnotu a viac chlpov než bežná deka.",
            "<strong>Kontrolujte filter a tesnenie:</strong> chlpy často ostanú v práčke, nie iba v texte.",
            "<strong>Vôňa nie je riešenie pachu:</strong> najprv odstrániť zdroj, potom prípadne jemne prevoňať čistý textil.",
        ],
        "state_rows": [
            ("deka plná chlpov", "vyklepať, povysávať alebo použiť valček", "pred práčkou"),
            ("pach pelechu", "prať oddelene a úplne vysušiť", "vlhkosť pach vracia"),
            ("chlpy v práčke", "vyčistiť tesnenie a filter", "prenášajú sa ďalej"),
            ("obliečky so psom", "prať častejšie a s priestorom", "kontakt s pokožkou"),
        ],
        "textile_rows": [
            ("psí pelech", "oddelené pranie podľa štítku", "pach a chlpy"),
            ("deka na sedačke", "pred praním odchlpovať", "menej chlpov v bubne"),
            ("uterák po kúpaní psa", "samostatne a dobre vysušiť", "vlhkosť a pach"),
            ("poťah na vankúš", "zapnúť zips a nepreplniť", "chlpy v rohoch"),
        ],
        "sections": [
            ("Ako pripraviť psie deky pred praním", "Deku najprv vytraste vonku, povysávajte alebo prejdite valčekom. Čím viac chlpov odstránite pred praním, tým menej ich skončí v tesnení práčky a na ďalšej dávke oblečenia. Pri silnom pĺznutí sa oplatí tento krok nepreskakovať.", "Až potom vyberte program podľa štítku. Pri veľkých dekách nepreplňte bubon, aby sa textil mohol oplachovať."),
            ("Ako prať pelech pre psa", "Pelech perte oddelene od osobnej bielizne. Ak má snímateľný poťah, perte poťah samostatne a výplň riešte podľa štítku. Najrizikovejšia je vlhkosť vo výplni: ak pelech nevyschne úplne, pach sa rýchlo vráti.", "Pri pelechu je dôležitý aj čas sušenia. Vlhký pelech nedávajte späť na podlahu ani do kúta bez prúdenia vzduchu."),
            ("Textílie zo sedačky a spálne", "Ak pes spáva na sedačke alebo v posteli, poťahy a obliečky perte častejšie, ale rozumne rozdelené. Veľké kusy môžu uzavrieť menšie textílie a zhoršiť oplach. Chlpy sa držia najmä v švoch, rohoch a pri zipsoch.", "Pri posteľnej bielizni myslite aj na pokožku a dýchanie. Textil má byť čistý, nie iba prevoňaný."),
            ("Ako zabrániť prenosu chlpov na ďalšie pranie", "Po dávke psích textílií skontrolujte tesnenie, dvierka, filter a bubon. Ak v práčke ostanú mokré chumáče chlpov, prenesú sa na tmavé tričká alebo uteráky. Pri silnom pĺznutí pomôže aj krátky prázdny oplach podľa možností práčky.", "Súvisiaci detailný návod je <a href=\"/n/ako-odstranit-chlpy-z-oblecenia-pri-prani-ked-mate-psa-alebo-macku\">ako odstrániť chlpy z oblečenia pri praní</a>."),
            ("Pach psa, vlhkosť a sušenie", "Pach sa často nevracia pre slabý prací produkt, ale pre vlhkosť. Hrubé deky, pelechy a uteráky schnú pomaly. Ak ich zložíte mierne vlhké, vznikne zatuchnutý pach zmiešaný so psím pachom.", "Sušte úplne, s prúdením vzduchu. Pri textíliách, ktoré schnú dlho, je lepšie prať menšie dávky."),
        ],
        "depth": [
            ("Prečo práčka sama neodstráni všetky chlpy", "Chlpy sú mechanické nečistoty. Časť sa uvoľní, časť sa zachytí na textile a časť ostane v práčke. Prací gél odstráni mastnotu a pachové zložky, ale nenahradí predchádzajúce vyklepanie alebo odchlpovanie.", "Preto je najlepšia kombinácia mechanického odstránenia chlpov, primeranej dávky a následnej kontroly práčky."),
            ("Domácnosť so psom a prací kalendár", "Počas pĺznutia má zmysel rozdeliť textílie podľa rizika: pelech, sedačkové deky, uteráky po kúpaní a obliečky. Nemusia ísť všetky naraz. Menšie dávky sa lepšie operú a rýchlejšie vysušia.", "Pravidelnosť je účinnejšia než občasné agresívne pranie veľkej hromady chlpov."),
        ],
        "expert_title": "Odbornejší pohľad: chlpy, kožný maz a mikroprostredie textilu",
        "expert_p1": "Textílie používané psom zachytávajú chlpy, prach, kožný maz, sliny a vlhkosť. Pri praní preto nejde len o estetiku chlpov na povrchu, ale aj o látky zachytené v švoch a vo výplni. Tie môžu po zahriatí alebo zvlhnutí znovu zapáchať.",
        "expert_p2": "Najväčšiu kontrolu máte pred praním a po praní: odstrániť čo najviac chlpov, prať v rozumnej dávke, vyčistiť práčku a textil úplne vysušiť.",
        "checklist": "Pred praním skontrolujte množstvo chlpov, typ textilu, výplň pelechu, zipsy, veľkosť dávky, filter práčky, tesnenie a to, či textil môže úplne vyschnúť ešte v ten deň.",
        "rule": "Pri psích textíliách platí: najprv chlpy mechanicky preč, potom prať oddelene a nakoniec vyčistiť práčku.",
        "recommendation_intro": "Prací gél pomáha s bežnou špinou a pachom, ale pri psích textíliách musí ísť spolu s odchlpovaním a dobrým sušením.",
        "product_text": "Vhodný na šetrné pranie prateľných diek, poťahov a textílií v domácnosti so psom podľa štítku, najmä pri primeranej dávke a dobrom oplachu.",
        "links": [
            ("/n/ako-odstranit-chlpy-z-oblecenia-pri-prani-ked-mate-psa-alebo-macku", "Ako odstrániť chlpy z oblečenia pri praní"),
            ("/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia", "Prečo textílie zapáchajú po praní"),
            ("/n/ako-casto-prat-postelne-pradlo", "Ako často prať posteľné prádlo"),
        ],
        "faq": [
            ("Môžem prať psí pelech s bežným oblečením?", "Radšej nie. Pelech drží viac chlpov, pachu a vlhkosti než bežná bielizeň."),
            ("Ako dostať chlpy z deky pred praním?", "Vytriasť, povysávať alebo použiť valček. Práčka nemá nahradiť mechanické odchlpovanie."),
            ("Prečo textílie so psom stále zapáchajú?", "Často nepreschli úplne alebo sa v nich drží kožný maz a vlhkosť. Pomáha menšia dávka, dobrý oplach a úplné sušenie."),
        ],
    },
    "red_paprika": {
        "marker": "Detailnejší postup na červenú papriku z trička a kuchynskej utierky",
        "problem": "červená paprika a paprikové jedlá môžu zanechať farebný pigment, mastnotu a niekedy aj kyslú alebo slanú zložku, preto bežné pranie bez predčistenia nemusí stačiť",
        "scope": "bavlnenom tričku, kuchynskej utierke, zástere, obruse, detskom oblečení a svetlej bavlne po varení alebo jedle",
        "avoid": "drhnutie červenej škvrny do strán, horúcu vodu na začiatku, sušičku pred kontrolou a pranie špinavej utierky s jemnou bielizňou",
        "diagnosis": [
            "<strong>Paprika je pigmentová škvrna:</strong> červený alebo oranžový tieň sa musí riešiť pred sušením.",
            "<strong>Jedlo môže byť mastné:</strong> guláš, omáčka alebo dressing rieši farbu aj tuk.",
            "<strong>Utierka má viac vrstiev špiny:</strong> často obsahuje aj olej, omáčku a zvyšky čistenia kuchyne.",
            "<strong>Teplo odložte:</strong> sušička a žehlenie patria až po kontrole výsledku.",
        ],
        "state_rows": [
            ("čerstvá paprika", "odobrať prebytok a prepláchnuť", "bez trenia"),
            ("mastná omáčka", "riešiť pigment aj tuk", "dva problémy"),
            ("kuchynská utierka", "predprať oddelene", "kombinovaná špina"),
            ("biele tričko", "kontrola pred sušením", "tieň sa zvýrazní"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "preplach z rubu a lokálne predčistenie", "pigment v úplete"),
            ("kuchynská utierka", "oddeliť od jemnej bielizne", "mastnota a pach"),
            ("obrus", "nežehliť pred výsledkom", "teplo fixuje tieň"),
            ("zástera", "predčistiť viac miest", "varenie vytvára kombinované škvrny"),
        ],
        "sections": [
            ("Ako odstrániť čerstvú papriku z trička", "Najprv odstráňte zvyšky jedla tupou hranou alebo papierovou utierkou. Netrite škvrnu do strán. Prepláchnite ju z rubovej strany studenšou vodou, aby sa pigment tlačil von z vlákna, nie hlbšie do neho.", "Potom použite malé množstvo pracieho gélu na lokálne predčistenie a perte podľa štítku."),
            ("Papriková omáčka, guláš a mastnota", "Ak je paprika súčasťou omáčky, často riešite aj olej alebo masť. Po oplachu môže farba zoslabnúť, ale mastný okraj zostane. Preto sledujte nielen červený tieň, ale aj tmavšiu mapu po vyschnutí.", "Pri mastnote nadväzuje návod <a href=\"/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku\">ako odstrániť mastný dressing z obrusu</a>."),
            ("Paprika na kuchynskej utierke", "Kuchynská utierka býva savá a často už obsahuje mastnotu, čistiace zvyšky alebo pach. Pri paprike ju neperte s jemnou bielizňou. Najprv predčistite najvýraznejšie miesto a potom perte s kuchynskými textíliami.", "Ak utierka po praní stále zapácha, problém môže byť aj v nedostatočnom sušení alebo preplnenom bubne."),
            ("Červený tieň po praní", "Ak po praní zostal červený alebo oranžový tieň, nesušte textil horúco. Skontrolujte miesto pri dennom svetle a postup zopakujte mierne. Agresívne bielenie na farebnom tričku môže poškodiť okolie škvrny.", "Pri bielej bavlne postupujte podľa štítku a nerobte náhodné kombinácie prípravkov."),
            ("Paprika verzus kari a horčica", "Paprika, kari aj horčica sú výrazné farebné škvrny, ale každá má iné zloženie. Kari často obsahuje kurkumu, horčica žlté pigmenty a paprika červené farbivá s jedlom alebo olejom. Preto je najbezpečnejšie začať odobratím prebytku a predčistením, nie teplom.", "Súvisiace návody sú <a href=\"/n/ako-vyprat-kari-a-kurkumu-z-bavlneneho-tricka-bez-zlteho-tiena\">kari a kurkuma</a> a <a href=\"/n/ako-odstranit-skvrny-od-horcice-z-tricka-obrusu-a-utierky\">škvrny od horčice</a>."),
        ],
        "depth": [
            ("Pigment a mastnota v jednej škvrne", "Červená paprika v jedle je zriedka čistý prášok. Často je v tuku, omáčke alebo šťave. Pigment môže zostať ako farebný tieň a tuk ako tmavšia mapa. Preto sa oplatí po praní kontrolovať farbu aj dotyk látky.", "Ak miesto pôsobí mastne, samotný oplach nestačil."),
            ("Prečo kuchynské textílie prať samostatne", "Utierky, zástery a obrusy majú iné zaťaženie než tričká. Zachytávajú jedlo, olej, pach aj čistiace zvyšky. Keď ich zmiešate s jemnou bielizňou, môžete preniesť pach alebo škvrny do dávky, ktorá ich nepotrebuje.", "Triedenie pri kuchynských škvrnách chráni výsledok celej práčky."),
        ],
        "expert_title": "Odbornejší pohľad: farebné zložky jedla a fixácia teplom",
        "expert_p1": "Pri farebných škvrnách z jedla rozhoduje poradie krokov. Najprv treba odstrániť prebytok, potom pracovať s pigmentom a až nakoniec pridať teplo. Ak sa škvrna najprv zahreje, môže sa s vláknom spojiť pevnejšie.",
        "expert_p2": "Červená paprika je prakticky náročná najmä preto, že sa často objavuje v mastnom jedle. Jeden postup musí riešiť farbu aj tuk bez poškodenia farby samotného textilu.",
        "checklist": "Pred praním skontrolujte, či je na textile zvyšok jedla, mastný okraj, farebný tieň, rub látky, potlač, stálosť farby a to, či textil patrí ku kuchynským alebo osobným veciam.",
        "rule": "Pri červenej paprike najprv odobrať prebytok, prepláchnuť z rubu, predčistiť lokálne a sušiť až po kontrole.",
        "recommendation_intro": "Prací gél používajte po lokálnom predčistení. Pri paprike je dôležité nevytvoriť väčšiu mapu a nefixovať pigment teplom.",
        "product_text": "Vhodný na následné pranie tričiek, záster a kuchynských textílií po odstránení zvyškov paprikovej škvrny.",
        "links": [
            ("/n/ako-vyprat-kari-a-kurkumu-z-bavlneneho-tricka-bez-zlteho-tiena", "Ako vyprať kari a kurkumu"),
            ("/n/ako-odstranit-skvrny-od-horcice-z-tricka-obrusu-a-utierky", "Ako odstrániť horčicu"),
            ("/n/ako-odstranit-sojovu-omacku-z-kosele-obrusu-a-prestierania", "Ako odstrániť sójovú omáčku"),
        ],
        "faq": [
            ("Ide červená paprika vyprať z trička?", "Často áno, ak škvrnu neriešite teplom a najprv odstránite prebytok jedla."),
            ("Čo ak je paprika v olejovej omáčke?", "Riešte pigment aj mastnotu. Po oplachu sledujte farebný tieň aj mastnú mapu."),
            ("Môžem dať utierku do sušičky?", "Až po kontrole, že škvrna zmizla. Teplo môže zvyškový pigment alebo mastnotu zafixovať."),
        ],
    },
    "pet_hair": {
        "marker": "Detailnejší postup na chlpy z oblečenia pri praní",
        "problem": "chlpy z oblečenia pri praní sú mechanický problém, nie len otázka pracieho prostriedku, preto sa musia riešiť pred praním, počas triedenia aj po praní v práčke",
        "scope": "čiernych nohaviciach, mikine, tričku, legínach, uterákoch, dekách, posteľnej bielizni a oblečení nosenom pri psovi alebo mačke",
        "avoid": "prať chlpaté oblečenie bez predchádzajúceho odchlpovania, preplniť bubon, miešať tmavé hladké veci s chlpovými dekami a ignorovať tesnenie práčky",
        "diagnosis": [
            "<strong>Chlp treba odstrániť mechanicky:</strong> prací gél ho nerozpustí.",
            "<strong>Čierne oblečenie ukáže všetko:</strong> aj malé množstvo chlpov je po praní viditeľné.",
            "<strong>Práčka môže chlpy presúvať:</strong> z jednej dávky na druhú cez tesnenie a filter.",
            "<strong>Sušenie nie je oprava:</strong> ak sú chlpy nalepené po praní, najprv riešte príčinu v dávke.",
        ],
        "state_rows": [
            ("tmavé tričko", "valček pred praním aj po vysušení", "viditeľné chlpy"),
            ("mikina s vlasom", "prať naruby a oddelene", "zachytáva chlpy"),
            ("deka od psa", "vyklepať a prať samostatne", "veľa chlpov"),
            ("chlpy v práčke", "vyčistiť tesnenie a filter", "prenos na ďalšiu dávku"),
        ],
        "textile_rows": [
            ("čierne nohavice", "odchlpiť pred praním", "chlpy sa zvýraznia"),
            ("fleece mikina", "oddeliť od chlpových diek", "povrch zachytáva vlákna"),
            ("posteľná bielizeň", "prať s priestorom", "chlpy v rohoch"),
            ("uterák", "neprať s tmavým hladkým textilom", "prenáša vlákna"),
        ],
        "sections": [
            ("Ako odstrániť chlpy pred praním", "Použite valček, gumovú rukavicu, kefku alebo krátke povysávanie podľa typu textilu. Cieľ je odstrániť čo najviac chlpov ešte pred bubnom. Ak dáte oblečenie plné chlpov rovno do práčky, časť sa len presunie inde.", "Pri tmavých nohaviciach a mikinách je tento krok najviditeľnejší, pretože chlpy kontrastujú s farbou."),
            ("Ako prať oblečenie plné chlpov", "Nedávajte do jednej dávky tmavé tričká, psí pelech, uteráky a fleece. Triedenie podľa povrchu je pri chlpoch rovnako dôležité ako triedenie podľa farby. Bubon nepreplňte, aby sa chlpy mohli odplaviť.", "Použite primerané dávkovanie a dobrý oplach. Viac gélu chlpy nerozpustí."),
            ("Čo robiť, keď chlpy ostanú po praní", "Ak je oblečenie po praní stále chlpaté, najprv ho nechajte vyschnúť a potom použite valček alebo kefku. Mokré chlpy sa často len rozmazávajú po povrchu. Skontrolujte aj práčku, či nezostali chumáče v tesnení.", "Pri opakovanom probléme zmeňte triedenie dávok a odstráňte chlpy pred praním."),
            ("Práčka, filter a tesnenie", "Po praní psích alebo mačacích textílií skontrolujte gumové tesnenie pri dvierkach. Chlpy sa tam držia spolu s vodou a zvyškami pracieho prostriedku. Podľa návodu k práčke kontrolujte aj filter.", "Ak práčka zapácha, môže ísť o kombináciu vlhkosti, chlpov a usadenín."),
            ("Ako znížiť chlpy v domácnosti pri praní", "Počas pĺznutia perte textílie častejšie, ale v menších dávkach. Majte samostatnú deku na sedačku alebo posteľ a pravidelne ju odchlpte. Menej chlpov na vstupe znamená menej chlpov v práčke.", "Súvisiaci návod je <a href=\"/n/ako-prat-textilie-v-domacnosti-so-psom-pocas-plznutia\">ako prať textílie v domácnosti so psom počas pĺznutia</a>."),
        ],
        "depth": [
            ("Chlpy verzus vlákna z vreckovky", "Chlpy a kúsky papierovej vreckovky vyzerajú po praní podobne, ale vznikajú inak. Chlp pochádza zo zvieraťa a drží sa povrchu, papier sa rozpadne vo vode a zachytí sa po celej dávke. Pri oboch pomáha mechanické odstránenie, ale prevencia je iná.", "Pri papieri pomôže návod <a href=\"/n/ako-dostat-kusky-papierovej-vreckovky-z-ciernych-nohavic-a-mikiny\">ako dostať kúsky papierovej vreckovky z oblečenia</a>."),
            ("Prečo chlpy držia na syntetike a fleeci", "Niektoré povrchy zachytávajú chlpy viac, najmä fleece, česaná mikina, statická syntetika alebo tmavé hladké oblečenie. Pranie môže povrch zvlhčiť a chlpy dočasne prilepiť. Preto je dôležité aj sušenie a finálne odchlpovanie po vyschnutí.", "Ak sa chlpy vracajú stále, problém je skôr v triedení a príprave dávky než v pracom produkte."),
        ],
        "expert_title": "Odbornejší pohľad: mechanické častice a prenos medzi dávkami",
        "expert_p1": "Chlpy sú mechanické častice. Prací prostriedok odstráni špinu, mastnotu a pach, ale samotný chlp chemicky nerozpustí. Pohyb v bubne ho môže uvoľniť, no zároveň sa môže zachytiť na inom textile alebo v práčke.",
        "expert_p2": "Preto má proces tri časti: odstrániť chlpy pred praním, správne roztriediť dávku a po praní skontrolovať práčku. Vynechanie jednej časti vedie k tomu, že chlpy sa len presúvajú.",
        "checklist": "Pred praním skontrolujte množstvo chlpov, farbu textilu, povrch, fleece alebo úplet, veľkosť dávky, psie deky v koši, tesnenie práčky a to, či sa chlpy už nepreniesli na predchádzajúcu dávku.",
        "rule": "Pri chlpoch platí: odchlpiť pred praním, nepreplniť bubon, nemiešať psie deky s čiernym oblečením a po praní skontrolovať práčku.",
        "recommendation_intro": "Prací gél pomáha s čistotou a pachom, ale pri chlpoch musí ísť spolu s mechanickým odstránením a správnym triedením.",
        "product_text": "Vhodný na bežné pranie oblečenia v domácnosti so zvieratami podľa štítku, keď sú chlpy pred praním čo najviac odstránené.",
        "links": [
            ("/n/ako-prat-textilie-v-domacnosti-so-psom-pocas-plznutia", "Ako prať textílie so psom počas pĺznutia"),
            ("/n/ako-dostat-kusky-papierovej-vreckovky-z-ciernych-nohavic-a-mikiny", "Kúsky papierovej vreckovky z čiernych nohavíc"),
            ("/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia", "Prečo oblečenie zapácha po praní"),
        ],
        "faq": [
            ("Prečo ostali chlpy na oblečení po praní?", "Chlpy sa chemicky nerozpúšťajú. Treba ich odstrániť mechanicky a nepreplniť bubon."),
            ("Mám prať psie deky s oblečením?", "Radšej nie. Psie deky perte samostatne alebo s podobnými textíliami."),
            ("Ako vyčistiť práčku po chlpatej dávke?", "Skontrolujte tesnenie, dvierka a podľa návodu aj filter. Pri potrebe pustite oplachový program."),
        ],
    },
    "white_streaks": {
        "marker": "Detailnejší postup na biele šmuhy od pracieho prášku z čierneho oblečenia",
        "problem": "biele šmuhy na čiernom oblečení často nie sú poškodená farba, ale zvyšky nerozpusteného prášku, prebytočného prostriedku, minerálov z vody alebo slabého oplachu v preplnenom bubne",
        "scope": "čiernom tričku, tmavých nohaviciach, mikine, legínach, rifliach, športovej syntetike a tmavom bavlnenom oblečení",
        "avoid": "pridať ešte viac pracieho prostriedku, sušiť šmuhy horúco, preplniť bubon a používať krátky program s práškom pri nízkej teplote",
        "diagnosis": [
            "<strong>Najprv zistiť, či je to zvyšok:</strong> biela šmuha často zmizne po oplachu alebo navlhčení.",
            "<strong>Preplnený bubon je častá príčina:</strong> textil sa neprepláchne rovnomerne.",
            "<strong>Prášok potrebuje čas a vodu:</strong> pri krátkom studenom programe môže zostať viditeľný.",
            "<strong>Gél tiež dávkovať mierne:</strong> priveľa gélu môže zanechať film, aj keď nie práškové zrnká.",
        ],
        "state_rows": [
            ("biele pruhy po praní", "extra oplach alebo prepranie bez dávky", "zvyšky produktu"),
            ("drsný povrch", "skontrolovať dávkovanie a vodu", "minerály alebo prášok"),
            ("čierne športové veci", "nepreplniť a dobre opláchnuť", "syntetika ukáže film"),
            ("opakované šmuhy", "zmeniť program a dávku", "nie iba produkt"),
        ],
        "textile_rows": [
            ("čierne tričko", "prať naruby a s menšou dávkou", "viditeľný povrch"),
            ("tmavé rifle", "dostatok vody a oplachu", "hrubší textil"),
            ("legíny", "bez prebytku gélu", "film a elastan"),
            ("mikina", "neprať v natlačenej dávke", "záhyby držia zvyšky"),
        ],
        "sections": [
            ("Ako odstrániť biele šmuhy po praní", "Ak sú šmuhy čerstvé, najprv skúste textil vytriasť alebo pretrieť vlhkou čistou handrou. Ak ostávajú, pomôže extra oplach alebo krátke prepranie bez ďalšej dávky pracieho prostriedku. Nepridávajte viac prášku.", "Pred sušením skontrolujte, či sa šmuhy stratili. Horúce sušenie môže povlak zvýrazniť."),
            ("Prečo prášok ostáva na čiernom oblečení", "Prací prášok potrebuje dostatok vody, času a pohybu. Ak je program krátky, teplota nízka, bubon plný alebo dávka vysoká, časť sa nemusí rovnomerne rozpustiť a vypláchnuť. Na čiernom textile je to viditeľné hneď.", "Pri tmavom oblečení býva praktickejšie použiť primerane dávkovaný gél."),
            ("Prací gél verzus prášok pri tmavom oblečení", "Gél znižuje riziko zrniek prášku, ale stále môže zanechať film, ak ho nalejete priveľa alebo preplníte bubon. Rozdiel nie je len produkt, ale celý proces: dávka, voda, čas, oplach a veľkosť náplne.", "K porovnaniu nadväzuje článok <a href=\"/n/praci-gel-alebo-praci-prasok-kedy-co-funguje-lepsie-a-preco\">prací gél alebo prací prášok</a>."),
            ("Tvrdá voda a biele stopy", "Tvrdšia voda môže zvýšiť riziko povlaku alebo tvrdšieho pocitu z bielizne. Ak sa šmuhy opakujú, sledujte dávkovanie podľa tvrdosti vody a neperte tmavé veci v natlačenom bubne. Niekedy pomôže extra oplach.", "Ak je textil po praní lepkavý alebo tvrdý, problém nemusí byť farba, ale zvyšky v látke."),
            ("Čierne oblečenie po praní: kontrola pred sušením", "Tmavé veci skontrolujte ešte vlhké aj po čiastočnom preschnutí. Niektoré šmuhy sa ukážu až keď látka schne. Ak ich vidíte, nesušte horúco, ale opláchnite alebo preperte bez ďalšej dávky.", "Pri čiernom denime pomáha aj návod <a href=\"/n/ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu\">ako prať tmavé džínsy</a>."),
        ],
        "depth": [
            ("Šmuha alebo vyblednutie", "Biela šmuha od zvyškov prostriedku často mení intenzitu po navlhčení a dá sa opláchnuť. Vyblednutie farby je trvalejšie a kopíruje miesta trenia, slnka alebo opotrebovania. Toto rozlíšenie je dôležité, aby ste zbytočne nemenili celý prací režim.", "Ak šmuha zmizne po oplachu, nejde o poškodenú farbu."),
            ("Prečo krátky program nie vždy šetrí textil", "Krátky program môže byť praktický pri málo znečistenej dávke, ale pri prášku, tmavých kusoch a plnom bubne nemusí stačiť na rozpustenie a oplach. Výsledkom je textil, ktorý je praný, ale nie dobre vypláchnutý.", "Šetrnosť nie je len kratší čas. Je to správna kombinácia náplne, vody, pohybu a oplachu."),
        ],
        "expert_title": "Odbornejší pohľad: rozpustenie, disperzia a oplach",
        "expert_p1": "Viditeľné šmuhy vznikajú vtedy, keď sa prací prostriedok alebo minerálne zvyšky nedostanú z textilu von. Pri prášku je dôležité rozpustenie, pri géli rovnomerné rozptýlenie a pri oboch následný oplach.",
        "expert_p2": "Čierne oblečenie je iba prísny test procesu. To, čo na svetlom textile nevidno, sa na tmavom ukáže ako biely pruh, povlak alebo matná mapa.",
        "checklist": "Pred ďalším praním skontrolujte dávku, typ prostriedku, tvrdosť vody, teplotu, dĺžku programu, veľkosť náplne a to, či sa textil v bubne môže voľne pohybovať.",
        "rule": "Pri bielych šmuhách nepridávajte viac prostriedku. Znížte dávku, uvoľnite bubon a pridajte oplach alebo vhodnejší program.",
        "recommendation_intro": "Pri čiernom oblečení je praktické používať dobre dávkovateľný prací gél a nepreplniť bubon. Cieľom je čistý textil bez zvyškov.",
        "product_text": "Vhodný na bežné tmavé a farebné pranie pri primeranom dávkovaní, keď chcete znížiť riziko viditeľných zvyškov na látke.",
        "links": [
            ("/n/praci-gel-alebo-praci-prasok-kedy-co-funguje-lepsie-a-preco", "Prací gél alebo prací prášok"),
            ("/n/ako-vybrat-praci-gel-podla-typu-bielizne", "Ako vybrať prací gél"),
            ("/n/ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu", "Ako prať tmavý denim"),
        ],
        "faq": [
            ("Ako odstrániť biele šmuhy z čierneho trička?", "Skúste extra oplach alebo prepranie bez ďalšej dávky. Potom upravte dávku a veľkosť náplne."),
            ("Je lepší gél alebo prášok na čierne oblečenie?", "Často je praktickejší gél, ale rozhoduje dávkovanie a oplach. Aj gél môže zanechať film, ak ho je veľa."),
            ("Sú biele šmuhy vyblednutá farba?", "Nie vždy. Ak zmiznú po navlhčení alebo oplachu, ide skôr o zvyšok produktu než o vyblednutie."),
        ],
    },
    "shirt_holes": {
        "marker": "Detailnejší postup na dierky v tričkách po praní a sušení",
        "problem": "dierky v tričkách po praní a sušení nemusia vzniknúť samotným praním, ale kombináciou opotrebovania vlákna, trenia o zipsy, preplneného bubna, ostrých hrán, opasku, tašky alebo sušičky",
        "scope": "bavlnených tričkách, tenkých úpletoch, detskom oblečení, pyžame, tričkách s elastanom, spodných vrstvách a jemných letných materiáloch",
        "avoid": "prať tenké tričká so zipsami a suchým zipsom, preplniť bubon, používať príliš agresívne odstreďovanie a hľadať príčinu iba v práčke",
        "diagnosis": [
            "<strong>Miesto dierky napovie:</strong> pri páse môže ísť o opasok, zips alebo trenie o pracovnú dosku.",
            "<strong>Jemné tričká triediť:</strong> nepatria k uterákom, rifliam a mikinám so zipsom.",
            "<strong>Skontrolujte bubon:</strong> ostrá hrana alebo poškodený zips môžu textil zachytiť.",
            "<strong>Sušička pridáva mechaniku:</strong> pri tenkých úpletoch môže urýchliť opotrebovanie.",
        ],
        "state_rows": [
            ("dierky pri páse", "skontrolovať opasok a zipsy", "trenie mimo práčky"),
            ("dierky po celej ploche", "skontrolovať dávku a bubon", "mechanické poškodenie"),
            ("jemné tričko", "prať naruby a oddelene", "nižšie trenie"),
            ("opakovaný problém", "sledovať rovnaké miesto", "príčina sa opakuje"),
        ],
        "textile_rows": [
            ("tenká bavlna", "jemnejší program a menšia dávka", "slabšie vlákna"),
            ("tričko s elastanom", "nižšie teplo", "pružnosť"),
            ("detské tričko", "kontrola zipsov a suchých zipsov", "kontakt v dávke"),
            ("pyžamo", "prať s jemnými kusmi", "časté pranie"),
        ],
        "sections": [
            ("Prečo vznikajú malé dierky na tričkách", "Malé dierky často vzniknú tam, kde je látka dlhodobo oslabená. Môže ísť o trenie o opasok, zips nohavíc, kuchynskú linku, tašku, batoh alebo hrubé textílie v bubne. Pranie potom poškodenie iba zviditeľní.", "Preto sa oplatí sledovať miesto dierky, nie iba prací program."),
            ("Ako prať tričká, aby sa menej ničili", "Tričká otočte naruby, perte ich s podobne jemnými kusmi a nepreplňte bubon. Nedávajte ich do jednej dávky s rifľami, uterákmi, mikinami so zipsom alebo oblečením so suchým zipsom. Pri tenkých úpletoch zvoľte nižšiu mechaniku.", "K zapínaniu nadväzuje článok <a href=\"/n/ako-prat-oblecenie-so-zipsami-a-suchym-zipsom-bez-zatrhnutia\">ako prať oblečenie so zipsami a suchým zipsom</a>."),
            ("Kontrola práčky a oblečenia v dávke", "Ak sa dierky objavujú náhle a na viacerých kusoch, skontrolujte bubon, tesnenie, zipsy, háčiky a tvrdé aplikácie v dávke. Jemná látka sa môže zachytiť o malú ostrú hranu, ktorú si pri bežnom praní nevšimnete.", "Skontrolujte aj podprsenky s háčikmi, kovové prvky a poškodené jazdce zipsov."),
            ("Sušička a tenké tričká", "Sušička pridáva teplo a mechaniku. Pri kvalitnom a odolnom tričku to nemusí byť problém, ale pri tenkých úpletoch alebo staršom materiáli môže urýchliť opotrebovanie. Ak sa dierky opakujú, skúste tričká sušiť voľne.", "Pred sušením tiež skontrolujte, či sa tričko nezachytilo alebo nemá už oslabené miesto."),
            ("Dierky verzus žmolkovanie a opotrebovanie", "Žmolkovanie a dierky majú spoločného menovateľa: trenie. Žmolok je uzlík uvoľnených vlákien, dierka je už strata súdržnosti látky. Ak textil žmolkuje a zároveň sa stenčuje, je vyššie riziko dierok.", "Súvisiaci návod je <a href=\"/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie\">prečo sa oblečenie žmolkuje</a>."),
        ],
        "depth": [
            ("Dierky pri bruchu a páse", "Veľmi časté sú dierky vpredu dole na tričku. Nemusí to byť práčka. Látka sa tam môže trieť o gombík riflí, zips, opasok, pracovnú dosku alebo bezpečnostný pás v aute. Pranie už len ukáže oslabené vlákno.", "Ak sa dierky objavujú stále na rovnakom mieste, sledujte denný kontakt trička s okolím."),
            ("Kvalita látky a časté pranie", "Tenké módne tričká môžu byť príjemné, ale nemusia zniesť rovnaké trenie ako hrubšia bavlna. Časté pranie, silné odstreďovanie a sušička skracujú životnosť. Šetrnejší režim pomôže, ale nezmení konštrukciu látky.", "Pri obľúbených tričkách má zmysel prať ich naruby a oddelene od tvrdých prvkov."),
        ],
        "expert_title": "Odbornejší pohľad: mechanická únava vlákna",
        "expert_p1": "Dierka je výsledok mechanického oslabenia vlákien. Pracie prostriedky v bežnej dávke zvyčajne nie sú hlavná príčina malých dierok; častejšie ide o trenie, zachytenie, oslabenú priadzu alebo kombináciu nosenia a prania.",
        "expert_p2": "Práčka je prostredie, kde sa oslabenie prejaví. Textílie sa trú, naťahujú a narážajú o iné kusy. Ak sú v dávke tvrdé časti, riziko pre tenký úplet rastie.",
        "checklist": "Pred praním skontrolujte zipsy, suchý zips, háčiky, kovové prvky, preplnenie bubna, stav trička pri páse, staršie oslabené miesta a to, či tričko nepatrí radšej do jemnejšej dávky.",
        "rule": "Pri dierkach hľadajte mechanickú príčinu: triediť jemné tričká, zapnúť zipsy, znížiť trenie a sledovať miesto poškodenia.",
        "recommendation_intro": "Prací gél používajte primerane, ale pri dierkach rozhoduje hlavne mechanická ochrana textilu. Jemné tričká perte s podobne jemnými kusmi.",
        "product_text": "Vhodný na šetrné pranie tričiek a jemnejších úpletov podľa štítku, keď je dávka správne roztriedená a nie je preplnená.",
        "links": [
            ("/n/ako-prat-oblecenie-so-zipsami-a-suchym-zipsom-bez-zatrhnutia", "Ako prať oblečenie so zipsami"),
            ("/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie", "Prečo sa oblečenie žmolkuje"),
            ("/n/ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu", "Ako prať tmavý denim"),
        ],
        "faq": [
            ("Robí dierky v tričkách práčka?", "Niekedy môže prispieť, ale často ide o trenie pri nosení, zipsy, opasok alebo oslabenú látku."),
            ("Ako predísť dierkam pri praní?", "Perte tričká naruby, s podobne jemnými kusmi, bez zipsov a suchého zipsu v rovnakej dávke."),
            ("Pomôže jemnejší prací gél?", "Pomôže šetrnému praniu, ale dierky sú najmä mechanický problém. Dôležité je triedenie a nižšie trenie."),
        ],
    },
}


def build_expansion(topic):
    config = TOPICS[topic]
    sections = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["sections"])
    depth = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["depth"])
    faq = "\n".join(f"<h3>{question}</h3>\n<p>{answer}</p>" for question, answer in config["faq"])
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["problem"].capitalize()}. Preto sa oplatí najprv rozlíšiť, či riešite mechanické nečistoty, pigment, zvyšky pracieho prostriedku, opotrebovanie alebo pach.</p>
        <p>Pri textile ako {config["scope"]} rozhoduje štítok, mechanická záťaž, množstvo vody, teplota a spôsob sušenia. Najväčšie riziko je {config["avoid"]}.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu textilu alebo škvrny</h2>
        {table(["Stav", "Čo urobiť", "Poznámka"], config["state_rows"])}
        <h2>Postup podľa typu textilu</h2>
        {table(["Textil", "Postup", "Prečo"], config["textile_rows"])}
        {sections}
        <h2>{config["expert_title"]}</h2>
        <p>{config["expert_p1"]}</p>
        <p>{config["expert_p2"]}</p>
        <p>Pri domácich problémoch s textilom je užitočné postupovať vecne: najprv odstrániť to, čo sa dá odstrániť mechanicky, potom prať podľa štítku, následne skontrolovať výsledok a až potom sušiť alebo žehliť. Tak sa znižuje riziko, že sa problém iba presunie alebo zafixuje.</p>
        {depth}
        <h2>Kontrolný checklist pred praním</h2>
        <p>{config["checklist"]}</p>
        <p>Do jednej dávky nedávajte textílie s protichodnými potrebami. Psia deka, pigmentové tričko, čierne legíny so šmuhami a tenké tričko s rizikom dierok potrebujú iný režim. Triedenie je súčasť výsledku, nie detail navyše.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po prvom praní zostal tieň, chlp, povlak, pach, mastná mapa alebo poškodenie povrchu, nesušte textil horúco. Najprv rozlíšte, či ide ešte o nečistotu alebo už o zmenu materiálu. Opakovaný mierny postup je bezpečnejší než jeden agresívny zásah.</p>
        <p>Pri opakovaných problémoch sledujte vzor: rovnaké miesto dierky, rovnaká dávka s chlpmi, rovnaké šmuhy na čiernom oblečení alebo rovnaká kuchynská škvrna. Vzor často prezradí príčinu rýchlejšie než výmena produktu.</p>
        <h2>Ako predísť poškodeniu pri sušení</h2>
        <p>Sušenie často rozhodne o výsledku. Chlpy sa po vyschnutí odstraňujú inak než za mokra, paprika sa môže teplom zafixovať, biele šmuhy sa môžu zvýrazniť a dierky sa pri sušičke môžu ďalej zväčšiť. Teplo používajte až po kontrole.</p>
        <p>Pri škvrnách najprv overte, že miesto je čisté. Pri chlpovom znečistení skontrolujte aj práčku. Pri jemných tričkách radšej znížte mechaniku a nechajte ich vyschnúť voľne.</p>
        <h2>Domáca rutina pre opakované problémy</h2>
        <p>Ak sa podobné problémy opakujú, nastavte si jednoduchú rutinu: kontrola pred košom na bielizeň, mechanické odstránenie chlpov alebo zvyškov, lokálne predčistenie škvŕn, primeraná dávka pracieho gélu, nepreplnený bubon a kontrola pred sušením.</p>
        <p>Pri psoch sledujte pelechy a deky, pri kuchynských škvrnách pigment a mastnotu, pri čiernom oblečení zvyšky produktu a pri dierkach mechanické trenie. Konkrétna príčina rozhoduje o ďalšom praní.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Rýchla zásada</h2>
        <p>{config["rule"]}</p>
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


PUBLIC_REPLACEMENTS = [
    (
        re.compile(r"<p>\s*V článku pokrývame aj hľadané výrazy ako\s*<strong>(.*?)</strong>\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: <strong>\1</strong>. \2</p>",
    ),
    (
        re.compile(r"<p>\s*Pokryté výrazy:\s*(.*?)\.\s*</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: \1.</p>",
    ),
    (
        re.compile(r"<p>\s*Článok cieli výrazy ako\s+(.*?)\.\s*</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré ľudia pri tejto téme často riešia: \1.</p>",
    ),
]


def public_cleanup(long):
    cleaned = long
    for pattern, replacement in PUBLIC_REPLACEMENTS:
        cleaned = pattern.sub(replacement, cleaned)
    return cleaned


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
    long = public_cleanup(long)
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 23 pet/paprika/hair/residue/holes articles.")
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
                "wave": "retrofit-wave-23-pets-paprika-hair-residue-holes-five",
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
                "wave": "retrofit-wave-23-pets-paprika-hair-residue-holes-five",
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
