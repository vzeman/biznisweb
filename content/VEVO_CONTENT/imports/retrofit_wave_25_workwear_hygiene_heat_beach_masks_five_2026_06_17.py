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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-25-workwear-hygiene-heat-beach-masks-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-25-workwear-hygiene-heat-beach-masks-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-vyprat-pracovne-tricko-po-zahradkarceni-od-hliny-a-potu",
        "post_id": "2184",
        "url": "https://www.vevo.sk/n/ako-vyprat-pracovne-tricko-po-zahradkarceni-od-hliny-a-potu",
        "topic": "garden_shirt",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-prat-menstruacne-nohavicky-bezpecne-a-hygienicky",
        "post_id": "2144",
        "url": "https://www.vevo.sk/n/ako-prat-menstruacne-nohavicky-bezpecne-a-hygienicky",
        "topic": "period_underwear",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-prat-potah-na-termofor-a-hrejivy-vankusik-bez-poskodenia-vyplne",
        "post_id": "2219",
        "url": "https://www.vevo.sk/n/ako-prat-potah-na-termofor-a-hrejivy-vankusik-bez-poskodenia-vyplne",
        "topic": "heat_pillow_cover",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-prat-plazove-pareo-satku-a-lahku-tuniku-po-dovolenke",
        "post_id": "2214",
        "url": "https://www.vevo.sk/n/ako-prat-plazove-pareo-satku-a-lahku-tuniku-po-dovolenke",
        "topic": "beach_pareo",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-prat-latkove-ruska-a-textilne-obaly-hygienicky",
        "post_id": "2220",
        "url": "https://www.vevo.sk/n/ako-prat-latkove-ruska-a-textilne-obaly-hygienicky",
        "topic": "cloth_masks",
    },
]


def clean(markup):
    return textwrap.dedent(markup).strip()


def table(headers, rows):
    head = "".join(f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{item}</th>' for item in headers)
    body = "\n".join(
        "<tr>" + "".join(f'<td style="border: 1px solid #e5e5e5; padding: 10px;">{cell}</td>' for cell in row) + "</tr>"
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


def product_category_card(config):
    return clean(
        f"""
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie z VEVO</h2>
        <p>{config["recommendation_intro"]}</p>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; margin: 14px 0;">
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>{config["product_text"]}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
        </div>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Kategória pracie gély</h3>
        <p>{config["category_text"]}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        </div>
        </div>
        """
    )


def related_links(links):
    items = "\n".join(f'<li><a href="{href}">{label}</a></li>' for href, label in links)
    items += '\n<li><a href="/n/ako-vybrat-praci-gel-podla-typu-bielizne">Ako vybrať prací gél podľa typu bielizne</a></li>'
    items += '\n<li><a href="/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia">Prečo oblečenie zapácha po praní</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


TOPICS = {
    "garden_shirt": {
        "marker": "Detailnejší postup na pracovné tričko po záhradkárčení",
        "problem": "Pracovné tričko po záhrade má na sebe dve odlišné vrstvy: minerálnu špinu z hliny a biologickú špinu z potu. Keď sa mokré blato začne hneď drhnúť alebo prať s jemnou bielizňou, môže sa rozšíriť po celej dávke.",
        "scope": "bavlnené tričko, funkčné tričko, golier, podpazušie, spodný lem, rukávy od rastlín, kolená pri práci na zemi, textil po komposte a veci po kosení",
        "avoid": "mokré rozmazanie blata, pranie pracovných vecí s obliečkami, preplnený bubon, sušičku pred kontrolou pachu a silnú vôňu namiesto odstránenia potu",
        "diagnosis": [
            "<strong>Hlina najprv mechanicky preč:</strong> suchú hlinu vytraste alebo vykefujte skôr, než tričko namočíte.",
            "<strong>Pot riešte lokálne:</strong> podpazušie, golier a spodný lem často potrebujú predčistenie.",
            "<strong>Pracovné veci perte oddelene:</strong> prach a zvyšky rastlín nepatria k jemnej bielizni.",
            "<strong>Práčku po ťažšej dávke skontrolujte:</strong> hlina sa môže držať v tesnení, filtri a bubne.",
        ],
        "state_rows": [
            ("suchá hlina", "vytriasť alebo vykefovať", "pred namočením"),
            ("mokré blato", "nechať čiastočne preschnúť", "nerozotierať"),
            ("pot v podpazuší", "lokálne predčistiť gélom", "rieši pach"),
            ("ťažká pracovná dávka", "nepreplniť bubon", "špina musí odísť s vodou"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "predprať potné miesta a prať s podobnými kusmi", "znesie viac než jemný úplet"),
            ("funkčné tričko", "bez aviváže, dobrý oplach", "film môže držať pach"),
            ("mikina zo záhrady", "najprv prach a rastliny preč", "hrubší textil zachytí špinu"),
            ("pracovné nohavice", "oddeliť od jemnej bielizne", "prenášajú hlinu do dávky"),
        ],
        "sections": [
            ("Ako odstrániť hlinu z trička pred praním", "Ak je hlina mokrá, nechajte ju najprv čiastočne preschnúť. Potom tričko vytraste vonku a mäkkou kefkou uvoľnite zvyšky z povrchu. Pranie má riešiť zvyšky vo vlákne, nie hrubú vrstvu zeminy.", "Pri úplete netlačte kefku silno do látky. Cieľom je znížiť množstvo špiny v práčke a nepoškodiť povrch trička."),
            ("Pot a zápach po záhradkárčení", "Po práci vonku sa pot mieša s prachom, peľom, kompostom a rastlinnými zvyškami. Najviac pachu býva v podpazuší, golieri a spodnom leme, preto tieto miesta ošetrite lokálne pred praním.", "Ak tričko po praní stále zapácha, riešte dávku, preplnenie bubna a sušenie. Samotná vôňa pot neodstráni."),
            ("Ako prať záhradkárske oblečenie oddelene", "Pracovné tričká, nohavice a mikiny nedávajte do jednej dávky s uterákmi, obliečkami alebo jemnou bielizňou. Hlina a prach sa môžu preniesť na textílie, ktoré neboli znečistené.", "Menšia samostatná dávka sa lepšie vyplaví a menej zaťaží práčku."),
            ("Kedy vyčistiť práčku po pracovných veciach", "Po veľmi špinavej dávke skontrolujte tesnenie, bubon a filter. Ak v práčke ostane piesok alebo hlina, ďalšia čistá dávka môže mať šmuhy alebo nepríjemný pach.", "Súvisiaci detail je <a href=\"/n/ako-vycistit-bubon-pracky-po-prani-pelechu-topanok-alebo-pracovnych-veci\">ako vyčistiť bubon práčky po pracovných veciach</a>."),
            ("Sušenie pracovného trička", "Tričko neskladajte vlhké a neukladajte ho hneď do koša alebo skrine. Zvyškový pot a vlhkosť vytvoria zatuchnutie, ktoré ďalšie pranie rieši ťažšie.", "Sušte voľne a až po kontrole, že pach aj špina zmizli. Pri funkčných tričkách sa vyhnite prehriatiu."),
            ("Prevencia pri ďalšej práci v záhrade", "Ak viete, že budete pracovať s hlinou, kompostom alebo trávou, vyhraďte si pracovné tričká. Nebudete musieť agresívne zachraňovať jemné kusy a zároveň znížite prenos špiny do bežnej bielizne.", "Po práci tričko nenechajte zatvorené v taške. Najprv ho nechajte preschnúť a až potom ho dajte do prania."),
        ],
        "expert_title": "Odbornejší pohľad: minerálna špina, pot a mechanika práčky",
        "expert_p1": "Hlina je z veľkej časti mechanická nečistota. Ak jej je veľa, prací gél ju sám nerozpustí. Pot, maz a pachové zložky sú iný typ problému a vyžadujú vodu, čas, primeranú dávku gélu a dobrý oplach.",
        "expert_p2": "Preto záhradkárske oblečenie potrebuje kombinovaný postup: suchú špinu najprv odstrániť, potné miesta predčistiť, bubon nepreplniť a po praní skontrolovať práčku aj textil.",
        "checklist": "Pred praním skontrolujte suchú hlinu, mokré blato, rastlinné zvyšky, podpazušie, golier, spodný lem, typ trička, farbu, veľkosť dávky, stav práčky a to, či je oblečenie vhodné prať s ostatnými vecami.",
        "rule": "Pri pracovnom tričku po záhrade najprv odstráňte hlinu, potom riešte pot a až nakoniec perte celú dávku.",
        "recommendation_intro": "Pri pracovnom oblečení má prací gél pomôcť odstrániť pot a bežnú špinu po tom, čo mechanické zvyšky dostanete z textilu preč.",
        "product_text": "Vhodný na následné pranie pracovných tričiek, mikín a bežných textílií po vytrasení hliny a lokálnom predčistení potných miest.",
        "category_text": "Pri pracovnej bielizni vyberajte prací gél podľa znečistenia, tvrdosti vody a veľkosti dávky. Príliš veľa gélu nenahradí vyklepanie hliny.",
        "links": [
            ("/n/ako-vycistit-bubon-pracky-po-prani-pelechu-topanok-alebo-pracovnych-veci", "Ako vyčistiť bubon práčky po pracovných veciach"),
            ("/n/preplnena-pracka-preco-sa-bielizen-nevyperie-neoplachne-a-zapacha", "Preplnená práčka a zlý výsledok prania"),
            ("/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu", "Ako odstrániť zápach zo športových legín"),
        ],
        "faq": [
            ("Môžem prať tričko od hliny hneď mokré?", "Radšej nie. Hrubú hlinu nechajte čiastočne preschnúť a odstráňte ju pred praním."),
            ("Prečo tričko po záhrade stále zapácha?", "V podpazuší alebo golieri zostal pot, prípadne bola dávka preplnená a zle sa opláchla."),
            ("Mám pracovné tričko prať s uterákmi?", "Nie je to ideálne. Hlina a prach sa môžu preniesť na textílie, ktoré boli pôvodne čisté."),
        ],
    },
    "period_underwear": {
        "marker": "Detailnejší postup na menštruačné nohavičky a absorpčnú vrstvu",
        "problem": "Menštruačné nohavičky sú vrstvený hygienický textil. Treba z nich dostať krv a pach, ale zároveň nepoškodiť absorpčnú vrstvu, gumičky a priedušnosť. Preto je poradie krokov dôležitejšie než silný program.",
        "scope": "absorpčnú vrstvu, klin, gumičky, švy, citlivú pokožku, predpranie studenou vodou, skladovanie pred praním, sušenie a oddelenie od hrubých textílií",
        "avoid": "horúcu vodu ako prvý krok, aviváž, agresívne žmýkanie, sušičku bez povolenia výrobcu, skladovanie vlhkých nohavičiek v uzavretom sáčku a pranie s drsnými zipsami alebo uterákmi",
        "diagnosis": [
            "<strong>Studená voda ako prvý krok:</strong> krv sa rieši lepšie pred teplom.",
            "<strong>Absorpčná vrstva potrebuje oplach:</strong> zvyšky pracieho prostriedku a aviváže môžu znižovať funkciu.",
            "<strong>Nohavičky nekrúťte agresívne:</strong> vrstvy, švy a gumičky sa môžu poškodiť.",
            "<strong>Úplné sušenie je povinné:</strong> vlhký vrstvený textil rýchlo získa zápach.",
        ],
        "state_rows": [
            ("čerstvé použitie", "prepláchnuť studenou vodou", "kým voda výrazne nefarbí"),
            ("zápach", "riešiť oplach a sušenie", "nie silnou vôňou"),
            ("citlivá pokožka", "primeraná dávka a dobrý oplach", "menej zvyškov v textile"),
            ("gumičky a švy", "jemná dávka bez drsných kusov", "nižšie opotrebovanie"),
        ],
        "textile_rows": [
            ("absorpčný klin", "nepreplniť bubon a nepreháňať gél", "musí sa dobre vypláchnuť"),
            ("jemná čipka alebo lem", "prať vo vrecku", "chráni tvar"),
            ("bavlnené telo nohavičiek", "podľa štítku a farby", "stálosť farby"),
            ("elastické časti", "bez horúceho sušenia", "gumičky sa menej ničia"),
        ],
        "sections": [
            ("Ako prepláchnuť menštruačné nohavičky po použití", "Po použití ich prepláchnite studenou vodou, kým voda nie je výrazne sfarbená. Netrite absorpčnú vrstvu tvrdou kefkou a nekrúťte ju agresívne.", "Cieľom je dostať krv von skôr, než sa začne riešiť hlavné pranie. Teplú vodu a sušičku nechajte až po kontrole štítku a odporúčaní výrobcu."),
            ("Čím prať menštruačné nohavičky", "Použite primeranú dávku pracieho gélu a vyhnite sa aviváži. Aviváž môže vytvoriť film, ktorý nie je vhodný pre absorpčnú vrstvu ani pre textil blízko pokožky.", "Ak má výrobca vlastné odporúčanie, má prednosť. Pri vrstvenom textile nie je univerzálny trik bezpečnejší než štítok."),
            ("Menštruačné nohavičky a zápach", "Zápach zvyčajne súvisí so zvyškom krvi, vlhkosťou alebo nedostatočným preschnutím. Silná vôňa ho môže krátko prekryť, ale nevyrieši príčinu.", "Pomáha studený predoplach, primeraná dávka gélu, nepreplnený bubon a úplné vysušenie na vzduchu."),
            ("Ako prať menštruačné nohavičky v práčke", "Nohavičky perte podľa štítku, ideálne s podobne jemnou bielizňou a bez zipsov, háčikov alebo hrubých uterákov. Vrecko na jemnú bielizeň chráni lemy a švy.", "Ak periete viac kusov naraz, bubon nepreplňte. Absorpčná vrstva potrebuje vodu aj priestor na oplach."),
            ("Sušenie absorpčného textilu", "Sušte úplne do sucha. Vrstvené textílie schnú pomalšie než bežné nohavičky a vlhkosť vnútri vrstiev môže spôsobiť zatuchnutie.", "Ak výrobca nepovoľuje sušičku, sušte voľne. Horúce sušenie môže zhoršiť elastické časti alebo vrstvy."),
            ("Kedy kus vyradiť", "Ak sa nohavičky deformujú, zapáchajú aj po správnom praní, majú poškodené gumičky alebo absorpčná vrstva nefunguje ako predtým, ďalšie agresívne pranie nemusí pomôcť.", "Vtedy je lepšie skontrolovať odporúčania výrobcu a zvážiť výmenu. Hygienický textil musí byť funkčný aj pohodlný."),
        ],
        "expert_title": "Odbornejší pohľad: krv, bielkoviny a vrstvený textil",
        "expert_p1": "Krv patrí medzi škvrny, pri ktorých je dôležitý prvý kontakt s vodou. Studený predoplach pomáha odstrániť čo najviac zvyškov pred hlavným praním. Teplo na začiatku môže zvyšky vo vlákne zafixovať.",
        "expert_p2": "Menštruačné nohavičky zároveň nie sú obyčajné bavlnené prádlo. Absorpčné vrstvy, elastan, švy a gumičky potrebujú šetrné pranie, dobrý oplach a dôkladné sušenie.",
        "checklist": "Pred praním skontrolujte predoplach, farbu vody, absorpčnú vrstvu, švy, gumičky, štítok, aviváž, veľkosť dávky, prítomnosť zipsov alebo háčikov v bubne a to, či textil zvládne úplne preschnúť.",
        "rule": "Pri menštruačných nohavičkách najprv studený predoplach, potom šetrné pranie bez aviváže a nakoniec úplné vysušenie.",
        "recommendation_intro": "Pri hygienickom textile dávkujte prací gél primerane a oplach nechajte pracovať. Cieľom je čistota bez zvyškov v absorpčnej vrstve.",
        "product_text": "Vhodný na šetrné následné pranie menštruačných nohavičiek podľa štítku po studenom predoplachu, bez aviváže a bez preplnenia bubna.",
        "category_text": "Pri spodnej bielizni a hygienických textíliách vyberajte prací gél podľa citlivosti pokožky, materiálu a potreby dôkladného oplachu.",
        "links": [
            ("/n/ako-prat-podprsenku-a-jemnu-spodnu-bielizen-bez-deformacie", "Ako prať podprsenku a jemnú spodnú bielizeň"),
            ("/n/extra-oplach-v-pracke-kedy-pomoze-pri-zapachu-tvrdej-bielizni-a-citlivej-pokozke", "Kedy pomôže extra oplach"),
            ("/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach", "Zvyšky gélu, dávkovanie a oplach"),
        ],
        "faq": [
            ("Môžem použiť aviváž?", "Pri menštruačných nohavičkách radšej nie. Môže vytvoriť film na absorpčnej vrstve."),
            ("Prečo sa používa studená voda?", "Krv sa na začiatku rieši bezpečnejšie studeným predoplachom. Teplo nechajte až podľa štítku a odporúčaní výrobcu."),
            ("Ako predísť zápachu?", "Prepláchnuť po použití, prať primerane dávkovaným gélom, dobre opláchnuť a vysušiť úplne do sucha."),
        ],
    },
    "heat_pillow_cover": {
        "marker": "Detailnejší postup na poťah termoforu a hrejivý vankúšik",
        "problem": "Poťah na termofor a hrejivý vankúšik sú pri tele, často pri kréme, pote a teple. Zásadný rozdiel je medzi snímateľným poťahom a samotnou výplňou. Nie každá výplň znesie vodu.",
        "scope": "snímateľný poťah, gélovú vložku, čerešňové kôstky, obilné alebo semenné výplne, švy, zipsy, fľaky od krému, pot, skladovanie a sušenie",
        "avoid": "pranie celej výplne bez súhlasu výrobcu, namáčanie kôstok alebo semien, agresívne žmýkanie, nasadenie vlhkého poťahu späť, sušenie pri silnom zdroji tepla a skladovanie s pachom",
        "diagnosis": [
            "<strong>Poťah a výplň oddeľte:</strong> prateľný býva často len poťah.",
            "<strong>Výplne nenamáčajte naslepo:</strong> kôstky, semená a obilniny môžu plesnivieť alebo tvrdnúť.",
            "<strong>Krémy riešte lokálne:</strong> mastná mapa sa pri teple zvýrazní.",
            "<strong>Poťah musí úplne preschnúť:</strong> vlhkosť pri teple a tele rýchlo zapácha.",
        ],
        "state_rows": [
            ("snímateľný poťah", "prať podľa štítku", "samostatne od výplne"),
            ("kôstková výplň", "nenamáčať bez povolenia", "riziko plesne"),
            ("fľak od krému", "lokálne predčistiť", "mastná mapa"),
            ("pach po používaní", "vetrať a prať poťah", "nie prevoňať vlhkosť"),
        ],
        "textile_rows": [
            ("bavlnený poťah", "prať samostatne a dobre vysušiť", "kontakt s pokožkou"),
            ("fleece poťah", "jemne, bez prehriatia", "môže držať pach"),
            ("vankúšik s kôstkami", "vetrať výplň podľa návodu", "neznáša vodu"),
            ("gélová vložka", "čistiť povrchovo", "nepatrí do práčky"),
        ],
        "sections": [
            ("Ako prať poťah na termofor", "Ak je poťah snímateľný a štítok pranie povoľuje, perte ho samostatne alebo s podobne jemným textilom. Pred praním zapnite zipsy a skontrolujte švy.", "Mastné miesta od krému predčistite lokálne. Poťah nasaďte späť až vtedy, keď je úplne suchý."),
            ("Hrejivý vankúšik s čerešňovými kôstkami alebo semienkami", "Výplne z kôstok, semien alebo obilnín sa nemajú automaticky namáčať. Voda môže spôsobiť tvrdnutie, zápach alebo plesnivenie. Riaďte sa návodom výrobcu.", "Ak je špinavý iba poťah, riešte poťah. Samotnú výplň skôr vetrajte a chráňte pred vlhkosťou."),
            ("Fľaky od krému a oleja", "Termofor sa často používa po natretí pokožky krémom alebo olejom. Tie môžu na poťahu zanechať mastnú mapu, ktorá sa po zahriatí zvýrazní.", "Pred praním naneste malé množstvo gélu lokálne a jemne ho prepracujte. Potom perte podľa štítku."),
            ("Pach poťahu po opakovanom používaní", "Pach vzniká z potu, krému a nedostatočného sušenia. Ak poťah iba prevoňate, po ďalšom zahriatí sa pach vráti.", "Pomáha pravidelné pranie poťahu, vetranie a suché skladovanie mimo uzavretých plastových obalov."),
            ("Sušenie bez poškodenia tvaru", "Poťah sušte voľne a úplne. Neurýchľujte sušenie tak, že ho nasadíte na horúci termofor alebo položíte priamo na radiátor bez kontroly štítku.", "Teplo môže zmeniť tvar, zraziť poťah alebo zvýrazniť zvyšky mastnoty."),
            ("Ako skladovať termofor a hrejivý vankúšik", "Pred uložením musí byť poťah suchý a výplň bez vlhkosti. Ak vankúšik skladujete v skrini, nesmie byť cítiť zatuchnutím.", "Pri sezónnom používaní ho pred prvým použitím vyvetrajte a skontrolujte švy, zips aj povrch."),
        ],
        "expert_title": "Odbornejší pohľad: teplo, vlhkosť a organické výplne",
        "expert_p1": "Teplo urýchľuje uvoľňovanie pachov a zvýrazňuje mastné zvyšky. Preto poťah, ktorý pri bežnej teplote pôsobí čistý, môže po nahriatí cítiť krémom, potom alebo zatuchnutím.",
        "expert_p2": "Organické výplne sú samostatné riziko. Ak sa namočia a nevyschnú dokonale, môžu meniť vôňu, štruktúru a hygienu. Preto je bezpečnejšie oddeliť čistenie poťahu od starostlivosti o výplň.",
        "checklist": "Pred praním skontrolujte, či je poťah snímateľný, či štítok povoľuje pranie, aký typ výplne má vankúšik, či sú na poťahu mastné miesta, pach, zips, švy a či sa dá poťah úplne vysušiť pred ďalším použitím.",
        "rule": "Pri termofore perte poťah, nie výplň naslepo. Voda a organická výplň sú riziková kombinácia.",
        "recommendation_intro": "Pri prateľnom poťahu pomáha jemný prací gél a lokálne predčistenie mastných miest. Výplň riešte iba podľa návodu výrobcu.",
        "product_text": "Vhodný na pranie snímateľných prateľných poťahov podľa štítku, najmä po kontakte s pokožkou, potom alebo krémom.",
        "category_text": "Pri domácich textíliách pri tele používajte primeranú dávku pracieho gélu a nechajte textil úplne preschnúť.",
        "links": [
            ("/n/ako-odstranit-zapach-z-cestovneho-vankusa-po-lietadle", "Ako odstrániť zápach z cestovného vankúša"),
            ("/n/ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka", "Ako vyprať opaľovací olej z textilu"),
            ("/n/ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly", "Ako vyčistiť filter práčky pri zápachu"),
        ],
        "faq": [
            ("Môžem vyprať celý hrejivý vankúšik?", "Iba ak to výrobca výslovne povoľuje. Pri kôstkach, semenách alebo gélovej vložke je to často rizikové."),
            ("Prečo poťah po nahriatí zapácha?", "Teplo zvýrazní pot, krém, mastnotu alebo zatuchnutie, ktoré v textile zostali."),
            ("Kedy nasadiť poťah späť?", "Až keď je úplne suchý. Vlhkosť pri termofore alebo výplni zvyšuje riziko zápachu."),
        ],
    },
    "beach_pareo": {
        "marker": "Detailnejší postup na plážové pareo, šatku a ľahkú tuniku",
        "problem": "Plážové pareo, šatka a ľahká tunika často vyzerajú iba mierne pokrčené, ale nesú soľ, piesok, pot, opaľovací krém a olej. Tenké látky sa pritom ľahko vytiahnu, zmapujú alebo vyblednú.",
        "scope": "pareo, šatku, kaftan, ľahkú tuniku, viskózu, bavlnu, polyester, strapce, potlač, fľaky od opaľovacieho prípravku, soľ a piesok z pláže",
        "avoid": "pranie s pieskom vo vláknach, silné žmýkanie, ostré slnko pri sušení farieb, aviváž pri funkčnej alebo jemnej látke, horúcu sušičku a odloženie vlhkých plážových vecí do kufra",
        "diagnosis": [
            "<strong>Piesok najprv preč:</strong> zrnká môžu drieť jemnú látku aj práčku.",
            "<strong>Soľ a krém sú neviditeľné:</strong> textil môže pôsobiť čistý, ale po čase zapácha alebo žltne.",
            "<strong>Jemné látky potrebujú vrecko:</strong> pareo a šatka sa ľahko zachytia.",
            "<strong>Sušte v tieni:</strong> ostré slnko môže zhoršiť blednutie farieb.",
        ],
        "state_rows": [
            ("piesok v látke", "vytriasť pred praním", "nepatrí do bubna"),
            ("opaľovací krém", "lokálne predčistiť", "mastná mapa"),
            ("soľ z mora", "oplach a jemné pranie", "tvrdší pocit látky"),
            ("vlhké veci v kufri", "najprv vysušiť", "riziko zatuchnutia"),
        ],
        "textile_rows": [
            ("viskózové pareo", "jemne, nízke otáčky, sušiť rozložené", "môže sa vyťahať"),
            ("bavlnená šatka", "podľa farby a potlače", "pozor na púšťanie farby"),
            ("syntetická tunika", "dobrý oplach a bez prehriatia", "môže držať pach"),
            ("strapce a ozdoby", "pracie vrecko", "menej zachytávania"),
        ],
        "sections": [
            ("Ako pripraviť plážové veci pred praním", "Pareo, šatku a tuniku najprv vytraste vonku. Piesok sa drží v lemoch, strapcoch a záhyboch. Ak ho dáte rovno do práčky, môže drieť látku a zostať v bubne.", "Potom skontrolujte škvrny od krému, oleja alebo jedla. Tie riešte lokálne ešte pred jemným programom."),
            ("Soľ z mora a tvrdý pocit látky", "Soľ môže po vyschnutí zanechať tvrdší pocit, najmä pri tenkých látkach. Oplach alebo jemné pranie pomáha dostať zvyšky soli preč bez zbytočného trenia.", "Ak textil po dovolenke iba odložíte, soľ, pot a krém sa v látke usadia a ďalšie pranie bude náročnejšie."),
            ("Škvrny od opaľovacieho krému a oleja", "Opaľovací prípravok často vytvára mastné alebo žltkasté mapy. Pred praním naneste malé množstvo gélu lokálne a jemne ho prepracujte.", "Súvisiaci detail nájdete v článku <a href=\"/n/ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka\">ako vyprať opaľovací olej z plážovej tuniky</a>."),
            ("Ako prať jemné letné látky", "Pareo a šatku perte podľa štítku, ideálne vo vrecku a s nízkymi otáčkami. Tenká viskóza alebo ľahká zmes sa môže pri silnom žmýkaní vyťahať.", "Nekombinujte ich s džínsami, uterákmi alebo zipsami. Jemná dávka má byť naozaj jemná."),
            ("Sušenie po dovolenke", "Sušte v tieni a rozložené alebo voľne zavesené tak, aby sa látka nevyťahala. Ostré slnko môže urýchliť blednutie farieb, najmä pri potlači.", "Do kufra alebo skrine ukladajte len úplne suché kúsky. Vlhké pareo v plastovej taške veľmi rýchlo zatuchne."),
            ("Ako pripraviť plážové veci na ďalšiu sezónu", "Po poslednom praní sezóny skontrolujte lemy, strapce, škvrny od krému a pach. Ak zostane mastný tieň, neodkladajte ho na mesiace.", "Čisté a suché plážové textílie skladujte vzdušne. Pred dovolenkou ich potom stačí skontrolovať, nie zachraňovať staré škvrny."),
        ],
        "expert_title": "Odbornejší pohľad: soľ, UV svetlo, oleje a jemná väzba",
        "expert_p1": "Plážový textil je namáhaný inak než bežné tričko. Pôsobí naň soľ, slnko, opaľovací prípravok, pot a mechanické trenie piesku. Jemná väzba pritom nemá rezervu na tvrdé drhnutie.",
        "expert_p2": "Najlepšia stratégia je odstrániť piesok, riešiť mastné miesta lokálne, prať jemne a sušiť mimo ostrého prehriatia. Tak sa zachová splývavosť aj farba.",
        "checklist": "Pred praním skontrolujte piesok, soľ, mastné mapy, potlač, strapce, zloženie látky, farbu, štítok, pracie vrecko, otáčky a to, či textil po praní nebude visieť tak, že sa vytiahne.",
        "rule": "Pri plážových textíliách najprv piesok a krém, potom jemný program, nízke otáčky a sušenie v tieni.",
        "recommendation_intro": "Pri ľahkých letných textíliách používajte jemnú dávku gélu a riešte mastné miesta skôr, než sa celý kus vyperie.",
        "product_text": "Vhodný na následné šetrné pranie parea, šatiek a ľahkých tuník podľa štítku po odstránení piesku a lokálnom predčistení krému.",
        "category_text": "Pri plážových a letných textíliách vyberajte prací gél s ohľadom na jemnosť látky, farbu, pot a zvyšky opaľovacích prípravkov.",
        "links": [
            ("/n/ako-vyprat-opalovaci-olej-z-plazovej-tuniky-a-uteraka", "Ako vyprať opaľovací olej z plážovej tuniky"),
            ("/n/ako-dostat-piesok-z-detskych-sortiek-a-tricka-po-plazi-pred-pranim", "Ako dostať piesok z oblečenia po pláži"),
            ("/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost", "Čo je viskóza a ako sa o ňu starať"),
        ],
        "faq": [
            ("Môžem prať pareo s uterákmi?", "Radšej nie. Uteráky sú ťažšie, drsnejšie a môžu jemné pareo zbytočne namáhať."),
            ("Prečo plážová tunika po dovolenke zapácha?", "V látke zostal pot, soľ alebo opaľovací prípravok, prípadne bola odložená vlhká."),
            ("Ako sušiť šatku s potlačou?", "V tieni, bez prehriatia a tak, aby sa látka nevyťahala vlastnou váhou."),
        ],
    },
    "cloth_masks": {
        "marker": "Detailnejší postup na látkové rúška a malé textilné obaly",
        "problem": "Látkové rúška a malé textilné obaly sa používajú pri tvári, v kabelke, batohu alebo pri hygienických veciach. Preto je dôležitá pravidelnosť, oddelené skladovanie použitého kusu a úplné vysušenie.",
        "scope": "látkové rúško, textilný obal, malé puzdro, obal na hygienické potreby, gumičky, švy, vnútornú stranu, make-up, pot, vlhkosť v kabelke a skladovanie medzi použitiami",
        "avoid": "uloženie vlhkého rúška medzi čisté veci, silnú vôňu pri textile pri tvári, pranie s drsnými uterákmi, skladovanie v nepriedušnom obale po celý deň a používanie poškodených gumičiek alebo švov",
        "diagnosis": [
            "<strong>Použitý kus oddeľte:</strong> nedávajte ho voľne medzi čisté textílie.",
            "<strong>Vlhkosť riešte hneď:</strong> vlhké rúško alebo obal v taške rýchlo zapácha.",
            "<strong>Pri tvári menej vône:</strong> parfumácia nenahrádza čistotu a môže rušiť.",
            "<strong>Poškodené časti vyraďte:</strong> gumičky, švy a tvar majú praktický význam.",
        ],
        "state_rows": [
            ("vlhké rúško", "odložiť oddelene a vyprať", "nepatrí medzi čisté veci"),
            ("textilný obal v kabelke", "otočiť naruby a prať", "pach a prach"),
            ("make-up na vnútornej strane", "lokálne predčistiť", "mastný film"),
            ("poškodené gumičky", "opraviť alebo vyradiť", "zhoršené nosenie"),
        ],
        "textile_rows": [
            ("bavlnené rúško", "prať podľa štítku a dobre vysušiť", "kontakt s tvárou"),
            ("textilné puzdro", "otočiť naruby", "špina v rohoch"),
            ("obal na hygienické potreby", "prať oddelene od jemnej bielizne", "praktická hygiena"),
            ("rúško s gumičkami", "chrániť pred prehriatím", "elastické časti"),
        ],
        "sections": [
            ("Ako prať látkové rúško po použití", "Použité rúško odložte oddelene a perte podľa materiálu. Ak je vlhké alebo špinavé, nenechávajte ho voľne v kabelke medzi čistými vecami.", "CDC/NIOSH pri textilných maskách uvádza, že látkové masky sa majú prať aspoň raz denne alebo vtedy, keď sú mokré či špinavé. Prakticky to znamená pravidelnosť a úplné vysušenie, nie silnú parfumáciu."),
            ("Textilný obal v kabelke alebo batohu", "Malý obal pôsobí nenápadne, ale v rohoch drží prach, omrvinky, make-up aj pach. Pred praním ho otočte naruby a skontrolujte švy.", "Ak je obal na hygienické potreby, perte ho oddelene od jemnej bielizne a nechajte ho úplne preschnúť."),
            ("Make-up, rúž a pot na vnútornej strane", "Pri kontakte s tvárou sa v látke môže držať make-up, rúž, pot alebo krém. Tieto zvyšky neriešte iba vôňou. Ak vidíte mastnú mapu, predčistite ju lokálne.", "Pri podobných škvrnách pomáha aj návod <a href=\"/n/ako-odstranit-ruz-z-kosele-salu-a-latkovej-servitky\">ako odstrániť rúž z textilu</a>."),
            ("Ako sušiť rúška a malé obaly", "Po praní ich nechajte úplne vyschnúť. Vlhký textil pri tvári alebo v kabelke rýchlo zapácha a môže preniesť vlhkosť na ďalšie veci.", "Pri elastických gumičkách nepreháňajte teplo, ak to štítok neodporúča. Poškodené švy a gumičky vyraďte alebo opravte."),
            ("Ako skladovať čisté a použité kusy", "Čisté kusy skladujte oddelene od použitých. Suchý čistý kus môže byť v priedušnom obale, použitý alebo vlhký kus dajte bokom na pranie.", "Dôležité je nemať v jednej priehradke čisté rúško, použité rúško, kozmetiku a omrvinky z tašky."),
            ("Kedy textilný obal vyčistiť aj bez viditeľnej škvrny", "Ak obal nosíte denne v kabelke, nemusí mať viditeľný fľak, aby potreboval pranie. Pach, prach a kontakt s rukami sa hromadia postupne.", "Nastavte si pravidelnosť podľa používania. Malé textílie sa ľahko zabudnú, ale v praxi sú často bližšie k tvári a rukám než veľké kusy bielizne."),
        ],
        "expert_title": "Odbornejší pohľad: malé textílie, vlhkosť a kontakt s tvárou",
        "expert_p1": "Malé textílie majú vysoký kontakt s rukami, tvárou alebo obsahom tašky. Ich problémom nie je veľká škvrna, ale opakované drobné znečistenie, vlhkosť a skladovanie bez vzduchu.",
        "expert_p2": "Preto je dôležité oddeliť čisté a použité kusy, prať pravidelne a sušiť úplne. Odborný kontext k používaniu a starostlivosti o textilné masky nájdete v materiáli <a rel=\"noopener\" href=\"https://www.cdc.gov/niosh/ppe/php/mask-use/index.html\" target=\"_blank\">CDC/NIOSH Mask Use and Care</a>.",
        "checklist": "Pred praním skontrolujte, či je kus vlhký, či bol pri tvári, či má make-up alebo krém, či je obal otočený naruby, či sú gumičky a švy v poriadku, či nejde medzi čisté veci a či po praní úplne preschne.",
        "rule": "Pri rúškach a malých textilných obaloch oddeľte použité od čistých, perte pravidelne a skladujte až po úplnom vysušení.",
        "recommendation_intro": "Pri textile pri tvári a hygienických obaloch používajte primeranú dávku gélu a nepreháňajte vôňu. Dôležitý je dobrý oplach a úplné sušenie.",
        "product_text": "Vhodný na šetrné pranie látkových rúšok, malých obalov a textílií pri tvári podľa materiálu a štítku, najmä pri primeranej dávke a dobrom oplachu.",
        "category_text": "Pri malých hygienických textíliách sledujte zvyšky produktu, oplach a vôňu. Menej parfumácie býva pri textile pri tvári príjemnejšie.",
        "links": [
            ("/n/ako-odstranit-ruz-z-kosele-salu-a-latkovej-servitky", "Ako odstrániť rúž z textilu"),
            ("/n/extra-oplach-v-pracke-kedy-pomoze-pri-zapachu-tvrdej-bielizni-a-citlivej-pokozke", "Extra oplach pri citlivej pokožke"),
            ("/n/ako-prat-podprsenku-a-jemnu-spodnu-bielizen-bez-deformacie", "Ako prať jemnú spodnú bielizeň"),
        ],
        "faq": [
            ("Ako často prať látkové rúško?", "Pri bežnom používaní pravidelne a vždy, keď je mokré alebo špinavé. Riaďte sa aj materiálom a účelom použitia."),
            ("Môžem použiť výrazne voňavý produkt?", "Pri textile pri tvári radšej opatrne. Čistota, oplach a sušenie sú dôležitejšie než silná vôňa."),
            ("Ako skladovať použité rúško?", "Oddelene od čistých vecí a tak, aby ste ho čo najskôr vyprali. Vlhký kus nenechávajte dlho uzavretý v taške."),
        ],
    },
}


def build_expansion(topic):
    config = TOPICS[topic]
    sections = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["sections"])
    faq = "\n".join(f"<h3>{question}</h3>\n<p>{answer}</p>" for question, answer in config["faq"])
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["problem"]}</p>
        <p>V praxi sa oplatí pozerať na celý kontext: {config["scope"]}. Najväčšie riziko je {config["avoid"]}. Keď sa najprv vyrieši konkrétny problém a až potom príde hlavné pranie, výsledok je čistejší a textil sa menej ničí.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu problému</h2>
        {table(["Stav", "Čo urobiť", "Poznámka"], config["state_rows"])}
        <h2>Postup podľa typu textilu alebo časti</h2>
        {table(["Textil alebo časť", "Postup", "Prečo"], config["textile_rows"])}
        {sections}
        <h2>{config["expert_title"]}</h2>
        <p>{config["expert_p1"]}</p>
        <p>{config["expert_p2"]}</p>
        <p>Pri domácom praní je dôležité rozlíšiť, či riešite mechanickú špinu, biologické znečistenie, mastný film, vlhkosť alebo citlivú konštrukciu textilu. Jeden agresívny program nevyrieši všetko a môže poškodiť práve tú časť, ktorú chcete zachrániť.</p>
        <h2>Kontrolný checklist pred praním</h2>
        <p>{config["checklist"]}</p>
        <p>Do jednej dávky nedávajte textílie s protichodnými potrebami. Pracovné tričko od hliny, menštruačné nohavičky, poťah termoforu, plážové pareo a rúško pri tvári potrebujú rozdielnu prípravu, trenie, oplach aj sušenie.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po praní zostal tieň, pach, vlhkosť, lepkavý alebo mastný pocit, nesušte textil horúco a neodkladajte ho do skrine. Najprv určte, či ide ešte o nečistotu, alebo už o poškodenie materiálu.</p>
        <p>Opakovaný mierny postup býva bezpečnejší než jeden tvrdý zásah. Pri jemných, vrstvených alebo funkčných textíliách je cieľom zachovať použiteľnosť, nie vyhrať nad škvrnou za cenu deformácie.</p>
        <h2>Ako predísť zafixovaniu pri sušení</h2>
        <p>Sušenie je posledná kontrola. Teplo môže zvýrazniť mastnotu, zafixovať pigment, zhoršiť elastické časti alebo uzavrieť vlhkosť vo vrstve. Preto kontrolujte výsledok pred sušičkou, žehlením alebo uložením.</p>
        <p>Pri textíliách pri tele a pri viacerých vrstvách sledujte aj rub, švy a lemy. Suchý povrch nemusí znamenať, že je suchá celá absorpčná alebo výplňová časť.</p>
        <h2>Čo sledovať po druhom praní alebo čistení</h2>
        <p>Ak sa problém opakuje, zapamätajte si presnú príčinu: hlina, krv, krém, soľ, pot, make-up, vlhkosť alebo zvyšok pracieho prostriedku. Každá príčina potrebuje iný prvý krok.</p>
        <p>Pri ďalšom praní potom nezačínate od nuly. Viete, či pomohlo vyklepanie, studený predoplach, lokálne predčistenie, pracie vrecko, extra oplach alebo dlhšie sušenie.</p>
        <h2>Kedy nepokračovať agresívnejším praním</h2>
        <p>Ak textil púšťa farbu, mení tvar, tvrdne, stráca pružnosť alebo sa na povrchu objavil lesklý či vydratý fľak, ďalšie silnejšie pranie nemusí byť riešenie. Vtedy už možno nevidíte špinu, ale poškodenie materiálu.</p>
        <p>Pri drahšom, hygienickom alebo vrstvenom textile je lepšie zastaviť sa pri šetrnom postupe a skontrolovať odporúčanie výrobcu. Oprava rutiny do budúcna je často užitočnejšia než ďalší tvrdý zásah do jedného kusu.</p>
        <h2>Domáca rutina pri opakovanom probléme</h2>
        <p>Nastavte si jednoduchý postup: kontrola pred košom na bielizeň, odstránenie povrchových zvyškov, lokálne predčistenie, primeraná dávka gélu, nepreplnený bubon a kontrola pred sušením.</p>
        <p>Takto sa z prania nestane náhodný pokus. Pri pravidelne používaných textíliách je práve rutina rozdiel medzi čistým výsledkom a tým, že sa pach alebo škvrna vracia.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Rýchla zásada</h2>
        <p>{config["rule"]}</p>
        </div>
        {product_category_card(config)}
        {related_links(config["links"])}
        <h2>FAQ: praktické otázky</h2>
        {faq}
        """
    )


MARKERS = {key: value["marker"] for key, value in TOPICS.items()}
EXPANSIONS = {key: build_expansion(key) for key in TOPICS}


PUBLIC_REPLACEMENTS = [
    (
        re.compile(r"<p>\s*V článku pokrývame aj hľadané výrazy ako\s*(.*?)\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: \1. \2</p>",
    ),
    (
        re.compile(r"<p>\s*Článok pokrýva\s*(.*?)\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto téme často riešia: \1. \2</p>",
    ),
]


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
    if isinstance(data, dict) and isinstance(data.get("articles"), list):
        return data, data["articles"]
    raise SystemExit(f"Unsupported source format: {path}")


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
    marker = MARKERS[key]
    if marker in long:
        start = long.find(f"<h2>{marker}</h2>")
        search_from = long.find("<h2>FAQ: praktické otázky</h2>", start)
        if search_from == -1:
            search_from = start + len(marker)
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 25 workwear/hygiene/heat/beach/masks articles.")
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
                "wave": "retrofit-wave-25-workwear-hygiene-heat-beach-masks-five",
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
                "wave": "retrofit-wave-25-workwear-hygiene-heat-beach-masks-five",
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
