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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-24-childcare-costume-cosmetic-sticky-food-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-24-childcare-costume-cosmetic-sticky-food-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-prat-textilne-navleky-na-kocik-po-prechadzke-v-dazdi",
        "post_id": "2204",
        "url": "https://www.vevo.sk/n/ako-prat-textilne-navleky-na-kocik-po-prechadzke-v-dazdi",
        "topic": "stroller_covers",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-pach-z-kostymu-po-karnevale-bez-poskodenia-latky",
        "post_id": "2182",
        "url": "https://www.vevo.sk/n/ako-odstranit-pach-z-kostymu-po-karnevale-bez-poskodenia-latky",
        "topic": "carnival_costume",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-vyprat-suchy-sampon-z-cierneho-tricka-a-goliera",
        "post_id": "2179",
        "url": "https://www.vevo.sk/n/ako-vyprat-suchy-sampon-z-cierneho-tricka-a-goliera",
        "topic": "dry_shampoo",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-odstranit-zuvacku-z-nohavic-mikiny-a-potahu",
        "post_id": "2141",
        "url": "https://www.vevo.sk/n/ako-odstranit-zuvacku-z-nohavic-mikiny-a-potahu",
        "topic": "chewing_gum",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-odstranit-skvrny-od-horcice-z-tricka-obrusu-a-utierky",
        "post_id": "2136",
        "url": "https://www.vevo.sk/n/ako-odstranit-skvrny-od-horcice-z-tricka-obrusu-a-utierky",
        "topic": "mustard",
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
    "stroller_covers": {
        "marker": "Detailnejší postup na textilné návleky na kočík po daždi",
        "problem": "Textilné návleky na kočík po daždi riešia naraz vodu, blato, prach z chodníka, kontakt s detskou pokožkou a často aj funkčnú úpravu látky. Preto nestačí hodiť mokrý návlek do koša na bielizeň a vybrať silný program.",
        "scope": "striešku, nánožník, vložku, textilný poťah madla, podložku, drobné návleky, suché zipsy, patentky a časti, ktoré sa dotýkajú dieťaťa",
        "avoid": "skladanie vlhkých návlekov do tašky, pranie nevysušeného blata, agresívne kefovanie povrchovej úpravy, aviváž pri funkčnom textile a nasadenie späť na kočík pred úplným preschnutím",
        "diagnosis": [
            "<strong>Najprv sušenie a blato:</strong> mokrý textil nechajte rozložený, blato nechajte čiastočne preschnúť a odstráňte ho šetrne.",
            "<strong>Nie všetko patrí do práčky:</strong> výstuže, koženka, penové časti a lepené prvky môžu potrebovať iba lokálne čistenie.",
            "<strong>Suché zipsy zapnite:</strong> nezachytia jemný povrch a nepoškodia inú časť návleku.",
            "<strong>Detský kontakt je dôležitý:</strong> prací produkt dávkujte mierne a oplach nechajte dôkladný.",
        ],
        "state_rows": [
            ("vlhké návleky po prechádzke", "rozložiť a vetrať", "neskladať do tašky"),
            ("zaschnuté blato", "odstrániť mäkkou kefkou", "až potom prať"),
            ("funkčná úprava látky", "bez aviváže a bez horúceho sušenia", "chráni povrch"),
            ("kontakt s pokožkou dieťaťa", "jemná dávka a dobrý oplach", "menej zvyškov v textile"),
        ],
        "textile_rows": [
            ("strieška", "čistiť podľa štítku a výstuží", "nemusí byť prateľná"),
            ("nánožník", "najprv blato a piesok preč", "šetrí bubon aj látku"),
            ("vložka do kočíka", "prať samostatne alebo s podobným textilom", "kontakt s pokožkou"),
            ("madlo a popruhy", "skôr lokálne čistenie", "tvar a bezpečnostné prvky"),
        ],
        "sections": [
            ("Návleky na kočík po daždi: čo urobiť hneď doma", "Po príchode domov návleky nezatvárajte do tašky ani do košíka pod kočíkom. Mokrá látka bez prúdenia vzduchu rýchlo získa zatuchnutý pach a blato sa môže roztlačiť do väčšej plochy.", "Najbezpečnejší prvý krok je rozložiť textil, zotrieť voľnú vodu a nechať ho doschnúť tak, aby ste videli, čo je naozaj špina a čo je iba vlhká mapa."),
            ("Ako odstrániť blato z návlekov na kočík", "Blato je lepšie nechať čiastočne preschnúť a potom ho uvoľniť mäkkou kefkou alebo handričkou. Ak ho začnete hneď mokré drhnúť, zatlačíte ho hlbšie do väzby a vznikne väčšia sivohnedá mapa.", "Pri jemnom alebo funkčnom povrchu sa vyhnite tvrdej kefke. Cieľom je odstrániť častice, nie poškriabať úpravu látky."),
            ("Kedy prať a kedy stačí lokálne čistenie", "Ak je znečistená iba spodná hrana nánožníka alebo miesto pri kolieskach, často stačí lokálne čistenie. Celé pranie má zmysel pri pachu, väčšom znečistení, kontakte s jedlom alebo vtedy, keď sa špina preniesla na vnútornú stranu.", "Lokálne čistenie je šetrnejšie k výstužiam, suchým zipsom a tvaru. Práčka má byť riešenie, nie automatická prvá reakcia."),
            ("Ako prať odnímateľné prateľné časti", "Ak štítok pranie povoľuje, zapnite suché zipsy, odopnite odnímateľné tvrdé časti a perte s malou dávkou gélu. Nepreplňte bubon, aby sa návleky nekrčili do jednej hrče a oplach fungoval aj v záhyboch.", "Pri funkčných a vodoodpudivých úpravách vynechajte aviváž. Môže vytvoriť film, ktorý zmení správanie povrchu."),
            ("Sušenie návlekov pred nasadením späť na kočík", "Najväčšia chyba je nasadiť mierne vlhký textil späť na konštrukciu. V záhyboch, pri švoch a výstužiach vlhkosť schne pomalšie a môže vytvoriť zatuchnutie.", "Sušte rozložené, mimo priameho prehriatia. Pred nasadením prejdite rukou švy, rohy a miesta pri suchom zipse."),
            ("Kočík, detská pokožka a zvyšky pracieho prostriedku", "Návleky sú blízko dieťaťa, preto je dôležité nepreháňať dávku pracieho prostriedku. Viac gélu nemusí znamenať čistejší výsledok, najmä ak sa textil horšie oplachuje.", "Ak má dieťa citlivú pokožku, pomáha menšia dávka, nepreplnený bubon a istota, že textil pred použitím úplne preschol."),
        ],
        "expert_title": "Odbornejší pohľad: vlhkosť, blato a povrchová úprava textilu",
        "expert_p1": "Dažďová prechádzka prináša do textilu vodu, minerálne častice, organickú špinu a prach z chodníka. Pri praní potom nejde len o vizuálnu škvrnu, ale aj o to, či v švoch nezostane vlhkosť alebo častice blata.",
        "expert_p2": "Pri funkčných povrchoch rozhoduje jemnosť. Príliš silné trenie, aviváž alebo vysoké teplo môžu zhoršiť odpudzovanie vody a vzhľad látky. Preto je rozumné čistiť najprv mechanicky a až potom prať.",
        "checklist": "Pred praním skontrolujte štítok, výstuže, suché zipsy, patentky, penové časti, množstvo blata, pach, vnútornú stranu textilu, kontakt s pokožkou dieťaťa a to, či sa dá návlek bezpečne vysušiť do sucha.",
        "rule": "Pri návlekoch na kočík platí: najprv vysušiť a odstrániť blato, potom rozhodnúť medzi lokálnym čistením a praním, nakoniec sušiť naozaj do sucha.",
        "recommendation_intro": "Pri prateľných častiach kočíka vyberajte jemný prací základ a dávkujte ho striedmo. Produkt má pomôcť čistote, nie prekryť vlhkosť alebo blato.",
        "product_text": "Vhodný na následné pranie odnímateľných prateľných návlekov, vložiek a detských textílií podľa štítku, najmä pri miernej dávke a dôkladnom oplachu.",
        "category_text": "Pri detských a domácich textíliách sa oplatí mať prací gél, ktorý použijete až po odstránení blata, piesku alebo iných pevných nečistôt.",
        "links": [
            ("/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany", "Ako prať softshell bez poškodenia membrány"),
            ("/n/ako-dostat-piesok-z-detskych-sortiek-a-tricka-po-plazi-pred-pranim", "Ako dostať piesok z detského oblečenia pred praním"),
            ("/n/ako-vycistit-bubon-pracky-po-prani-pelechu-topanok-alebo-pracovnych-veci", "Ako vyčistiť bubon práčky po špinavších veciach"),
        ],
        "faq": [
            ("Môžem dať návleky na kočík rovno do práčky?", "Iba ak to povoľuje štítok a návleky nemajú výstuže alebo časti, ktoré pranie poškodia. Najprv vždy odstráňte blato."),
            ("Prečo návleky po praní zapáchajú?", "Najčastejšie preto, že boli uložené vlhké alebo nevyschli v švoch. Pach riešte sušením, nie silnejšou vôňou."),
            ("Môžem použiť aviváž?", "Pri funkčných alebo vodoodpudivých úpravách radšej nie. Aviváž môže vytvoriť film a zmeniť správanie látky."),
        ],
    },
    "carnival_costume": {
        "marker": "Detailnejší postup na karnevalový kostým po nosení",
        "problem": "Karnevalový kostým často nie je bežné oblečenie. Môže mať lacný satén, tyl, lepené aplikácie, flitre, farbené časti, penové výstuže a pot v podpazuší. Preto sa pach musí riešiť šetrne a podľa konštrukcie.",
        "scope": "detský kostým, tylovú sukňu, plášť, masku z textilu, čelenku, našité aplikácie, flitre, golier, podpazušie a podšívku",
        "avoid": "silné parfumovanie vlhkého kostýmu, horúcu vodu, žmýkanie tylu, pranie lepených dekorácií, sušičku a uloženie kostýmu do skrine pred úplným preschnutím",
        "diagnosis": [
            "<strong>Najprv vetrať:</strong> čerstvý pot a vlhkosť sa často výrazne zmiernia po dobrom vyvetraní.",
            "<strong>Dekorácie rozhodujú:</strong> flitre, lepidlá a potlače nemusia zvládnuť bežný program.",
            "<strong>Pach hľadajte lokálne:</strong> najčastejšie je v podpazuší, golieri, podšívke a pri páse.",
            "<strong>Skladovanie až po preschnutí:</strong> vlhký kostým v sáčku začne zapáchať ešte viac.",
        ],
        "state_rows": [
            ("pot v podpazuší", "lokálne ošetriť a vetrať", "neprať naslepo celý kus"),
            ("tyl a lacný satén", "jemne, bez žmýkania", "rýchlo mení tvar"),
            ("flitre a lepené časti", "skôr ručne alebo lokálne", "riziko odlepenia"),
            ("kostým do skrine", "uložiť až suchý", "inak vznikne zatuchnutie"),
        ],
        "textile_rows": [
            ("tylová sukňa", "ručné prepláchnutie alebo veľmi jemný režim", "chráni objem a tvar"),
            ("saténový plášť", "test farby a nízka mechanika", "môže sa lesknúť alebo mapovať"),
            ("kostým s flitrami", "obrátiť naruby alebo lokálne čistiť", "chráni ozdoby"),
            ("maska a doplnky", "neprať automaticky", "lepidlá a výstuže"),
        ],
        "sections": [
            ("Ako odstrániť pach z kostýmu po karnevale bez prania", "Kostým najprv zaveste na vzduch. Nechajte ho voľne visieť, nie preložený cez stoličku v jednej mokrej vrstve. Pach potu sa často zníži už tým, že textil vyschne a prestane byť uzavretý.", "Ak je pach iba mierny a kostým má veľa dekorácií, vetranie a lokálne čistenie býva bezpečnejšie než celé pranie."),
            ("Kedy prať karnevalový kostým", "Pranie má zmysel, ak je kostým prateľný, má škvrny od jedla, výrazný pot alebo bol celý deň na tele. Rozhodujúci je štítok a konštrukcia. Ak štítok chýba, posudzujte najcitlivejšiu časť kostýmu.", "Lepené ozdoby, potlač a penové prvky sú častý dôvod, prečo nepoužiť bežnú práčku."),
            ("Podpazušie, golier a podšívka", "Pach býva najviac v miestach kontaktu s pokožkou. Podpazušie alebo golier ošetrite lokálne miernym roztokom pracieho gélu, potom miesto opatrne vypláchnite alebo pretrite čistou vlhkou handričkou.", "Nepremočte celý kostým, ak to nie je potrebné. Lokálny postup chráni ozdoby a tvar."),
            ("Tyl, flitre a aplikácie", "Tyl neznáša silné žmýkanie a flitre sa môžu zachytávať. Pri podobných materiáloch je užitočný aj návod <a href=\"/n/ako-prat-tylovu-suknu-zavoj-a-jemny-tyl-bez-potrhania\">ako prať tyl</a> a <a href=\"/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami\">ako prať oblečenie s flitrami</a>.", "Ak sa ozdoby začínajú uvoľňovať, domáce pranie môže zhoršiť stav viac než samotný pach."),
            ("Sušenie kostýmu bez deformácie", "Kostým sušte rozložený alebo zavesený tak, aby sa nevyťahal. Tylu vráťte objem rukou, satén neprehrievajte a flitre nechajte voľne preschnúť.", "Sušička je pri karnevalových kostýmoch riziková. Teplo môže poškodiť lepidlá, tvar aj povrch."),
            ("Ako skladovať kostým do ďalšej sezóny", "Do skrine patrí iba suchý a čistý kostým. Ak ho uložíte s miernym pachom potu, po mesiacoch bude problém výraznejší. Vhodné je vzdušné uloženie, nie tesný plastový sáčok hneď po nosení.", "Pred odložením skontrolujte podpazušie, golier a lemy. Práve tam sa pach vracia najčastejšie."),
        ],
        "expert_title": "Odbornejší pohľad: pach potu, vlhkosť a citlivé dekorácie",
        "expert_p1": "Pach po karnevale vzniká najmä z potu, vlhkosti a uzavretého skladovania. Pri kostýmoch je problém, že materiály často nie sú navrhnuté na časté pranie. Látka môže byť tenká, farbená, lepená alebo kombinovaná s dekoráciami.",
        "expert_p2": "Preto je lepšie pracovať lokálne a postupne. Najprv vysušiť a vyvetrať, potom ošetriť miesta kontaktu s pokožkou, nakoniec rozhodnúť, či je celé pranie vôbec bezpečné.",
        "checklist": "Pred praním skontrolujte štítok, flitre, korálky, lepené časti, tyl, satén, farbu, potlač, podpazušie, golier, podšívku, výstuže a to, či kostým zvládne vodu aj mechaniku.",
        "rule": "Pri kostýme po karnevale riešte najprv pach a vlhkosť lokálne. Celé pranie použite iba vtedy, keď to dovolí materiál a dekorácie.",
        "recommendation_intro": "Pri prateľných kostýmoch má zmysel jemný prací základ, nízka mechanika a dôkladné sušenie. Silná vôňa nemá prekrývať pot ani vlhkosť.",
        "product_text": "Vhodný na šetrné lokálne predčistenie a následné pranie prateľných kostýmových častí podľa štítku, najmä pri potných miestach a bežnom textile bez citlivých dekorácií.",
        "category_text": "Pri sezónnych kostýmoch, detskom oblečení a jemnejších textíliách vyberajte prací gél podľa materiálu, nie podľa toho, ako silno má kostým voňať.",
        "links": [
            ("/n/ako-prat-tylovu-suknu-zavoj-a-jemny-tyl-bez-potrhania", "Ako prať tylovú sukňu, závoj a jemný tyl"),
            ("/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami", "Ako prať oblečenie s flitrami a korálkami"),
            ("/n/ako-prat-spolocenske-saty-doma-a-kedy-zvolit-cistiaren", "Ako prať spoločenské šaty doma"),
        ],
        "faq": [
            ("Môžem karnevalový kostým vyprať v práčke?", "Len ak to povoľuje štítok a kostým nemá rizikové dekorácie, výstuže alebo lepené časti."),
            ("Ako odstrániť pach bez poškodenia flitrov?", "Najprv vetrať, potom lokálne ošetriť podpazušie a golier. Flitre zbytočne nedrhnite."),
            ("Môžem kostým prevoňať namiesto prania?", "Iba ak je čistý a suchý. Vôňa nemá prekrývať pot, vlhkosť ani zatuchnutie."),
        ],
    },
    "dry_shampoo": {
        "marker": "Detailnejší postup na suchý šampón z čierneho trička a goliera",
        "problem": "Suchý šampón na čiernom textile je zradný, pretože ide o jemný prášok zmiešaný s mazom z vlasov, stylingom a trením pri golieri. Vlhká handrička môže z prášku urobiť sivú mapu.",
        "scope": "čierne tričko, golier košele, lem mikiny, rameno saka, šál, kapucňu, športové tričko a tmavý textil pri vlasoch",
        "avoid": "mokré rozotieranie prášku do strán, pranie s priveľa gélom, sušenie pred kontrolou, trenie kefou na jemnom čiernom úplete a miešanie s uterákmi, ktoré púšťajú vlákna",
        "diagnosis": [
            "<strong>Najprv nasucho:</strong> prášok vyklepte alebo vyčešte skôr, než ho namočíte.",
            "<strong>Čierny textil ukáže každý povlak:</strong> kontrolujte miesto pri dennom svetle.",
            "<strong>Golier má aj maz:</strong> nejde len o biely prášok, ale aj o vlasový produkt a pot.",
            "<strong>Prebytok gélu nepomôže:</strong> zvyšky pracieho prostriedku môžu vytvoriť ďalšiu šmuhu.",
        ],
        "state_rows": [
            ("suchý biely povlak", "vyklepať a jemne vyčesať", "bez vody"),
            ("sivá mapa po utretí", "lokálne predčistiť a prať naruby", "prášok sa zmiešal s vlhkosťou"),
            ("mastný golier", "riešiť maz aj prášok", "často dve vrstvy"),
            ("čierne tričko po praní", "kontrola pred sušením", "šmuhy sa môžu vrátiť"),
        ],
        "textile_rows": [
            ("čierne tričko", "prať naruby s tmavými kusmi", "chráni farbu a povrch"),
            ("golier košele", "lokálne predčistiť pri vlasoch", "maz drží prášok"),
            ("šál alebo kapucňa", "najprv vytriasť", "produkt sa drží v záhyboch"),
            ("sako pri ramenách", "skôr lokálne a opatrne", "nemusí byť prateľné"),
        ],
        "sections": [
            ("Suchý šampón na čiernom tričku: prvý krok", "Tričko najprv vytraste a miesto jemne prejdite mäkkou kefkou nasucho. Cieľom je dostať prebytočný prášok preč skôr, než sa spojí s vodou a vytvorí sivý film.", "Ak je prášku veľa, pracujte po malých úsekoch. Silné trenie na čiernom úplete môže vytvoriť lesklé miesto."),
            ("Ako vyprať biely prášok z goliera", "Golier má často kombináciu suchého šampónu, vlasového mazu, potu a zvyškov stylingu. Po nasucho odstránenom prášku použite malé množstvo gélu priamo na znečistené miesto a jemne ho prepracujte prstami.", "Potom perte podľa štítku, ideálne naruby a s tmavými kusmi. Nepreplňte bubon, aby sa golier dobre opláchol."),
            ("Sivá mapa na čiernej látke po mokrej handričke", "Ak ste prášok najprv zotreli mokrou handričkou, mohla vzniknúť väčšia sivá mapa. Vtedy už nejde iba o prach na povrchu, ale o zmes vody, púdrového zvyšku a mazu.", "Postupujte mierne: lokálne predčistenie, pranie naruby, dôkladný oplach a kontrola po vyschnutí bez horúceho sušenia."),
            ("Prečo čierne oblečenie po praní ukáže šmuhy", "Tmavý textil zvýrazní nielen suchý šampón, ale aj zvyšky pracieho prostriedku. Ak zostane šmuha po celom tričku, problém nemusí byť iba vo vlasovom produkte, ale aj v dávkovaní gélu, preplnenom bubne alebo krátkom programe.", "Súvisiaci návod je <a href=\"/n/ako-odstranit-biele-smuhy-od-pracieho-prasku-z-cierneho-oblecenia\">ako odstrániť biele šmuhy z čierneho oblečenia</a>."),
            ("Prevencia pri používaní suchého šampónu", "Suchý šampón nanášajte s predstihom a nechajte ho vo vlasoch usadiť. Pred oblečením čierneho trička alebo saka prečešte vlasy a skontrolujte ramená.", "Pri častom používaní pomáha mať oddelené tmavé tričká na dni, keď používate viac vlasového produktu."),
            ("Čierny textil, zvyšky produktu a vôňa", "Ak textil po praní vonia, ale na golieri ostal sivý povlak, problém nie je vyriešený. Vôňa nedokáže nahradiť odstránenie prášku, mazu a zvyškov stylingu.", "Najprv odstráňte povlak a až potom riešte sviežosť celej dávky."),
        ],
        "expert_title": "Odbornejší pohľad: práškový zvyšok, maz a tmavé farby",
        "expert_p1": "Suchý šampón funguje tak, že absorbuje mastnotu vo vlasoch. Na textile preto zanechá jemný absorpčný prášok, ktorý sa môže spojiť s mazom a potom sa správa inak než obyčajný prach.",
        "expert_p2": "Na čiernom textile je problém viditeľnejší, pretože svetlý povlak vytvára kontrast. Úspech závisí od poradia krokov: nasucho odstrániť čo najviac, potom lokálne riešiť maz a nakoniec prať s dobrým oplachom.",
        "checklist": "Pred praním skontrolujte množstvo prášku, mastný pocit na golieri, farbu textilu, potlač, rub látky, dávku gélu, veľkosť náplne, program a to, či sa podobné šmuhy neobjavujú aj na inom čiernom oblečení.",
        "rule": "Pri suchom šampóne na čiernom textile najprv pracujte nasucho. Vodu pridajte až po odstránení prebytku prášku.",
        "recommendation_intro": "Pri čiernom textile je dôležité šetrné pranie, správna dávka a dobrý oplach. Produkt má pomôcť odstrániť zvyšky, nie pridať ďalšiu šmuhu.",
        "product_text": "Vhodný na následné pranie tmavých tričiek, golierov a bežných textílií po nasucho odstránenom suchom šampóne a lokálnom predčistení.",
        "category_text": "Pri čiernom oblečení sledujte dávkovanie, oplach a triedenie. Prací gél používajte primerane, aby na tmavej látke nezostávali zvyšky.",
        "links": [
            ("/n/ako-odstranit-biele-smuhy-od-pracieho-prasku-z-cierneho-oblecenia", "Ako odstrániť biele šmuhy z čierneho oblečenia"),
            ("/n/ako-odstranit-lak-na-vlasy-z-goliera-kosele-a-satky", "Ako odstrániť lak na vlasy z goliera"),
            ("/n/ako-davkovat-praci-gel-podla-tvrdosti-vody-naplne-a-znecistenia", "Ako dávkovať prací gél"),
        ],
        "faq": [
            ("Mám suchý šampón z čierneho trička hneď utrieť mokrou handričkou?", "Radšej nie. Najprv odstráňte prášok nasucho, inak môže vzniknúť sivá mapa."),
            ("Prečo zostal biely povlak aj po praní?", "Mohlo ísť o zmes prášku, mazu a zvyškov pracieho prostriedku. Skontrolujte dávku gélu a oplach."),
            ("Ako predísť šmuhám od suchého šampónu?", "Nanášajte ho skôr, vlasy prečešte a čierne oblečenie si oblečte až po usadení produktu."),
        ],
    },
    "chewing_gum": {
        "marker": "Detailnejší postup na žuvačku z nohavíc, mikiny a poťahu",
        "problem": "Žuvačka je lepivá hmota, ktorá sa pri teple a tlaku rozťahuje hlbšie do vlákien. Preto je najhorší prvý krok horúca voda, pranie bez prípravy alebo snaha žuvačku rozotrieť.",
        "scope": "rifle, tepláky, mikinu, školské nohavice, gaučový poťah, autosedačku, koberec, vrecko, lem a miesto, ktoré sa pri sedení pritlačilo",
        "avoid": "horúcu vodu, žehličku, sušičku, silné trenie do strán, pranie kúskov žuvačky v bubne a rozpúšťadlá bez testu na skrytom mieste",
        "diagnosis": [
            "<strong>Najprv stuhnúť:</strong> chlad pomôže, aby sa žuvačka odlupovala namiesto rozmazávania.",
            "<strong>Objem musí ísť preč pred praním:</strong> práčka nemá odstraňovať hrudku žuvačky.",
            "<strong>Zvyšok môže lepiť:</strong> po odobratí hmoty zostáva film alebo mastný tieň.",
            "<strong>Poťah nepremačajte:</strong> výplň schne dlho a môže zapáchať.",
        ],
        "state_rows": [
            ("mäkká žuvačka", "ochladiť a netlačiť", "najprv spevniť"),
            ("stuhnutý okraj", "odlupovať tupou hranou", "po malých častiach"),
            ("lepkavý film", "lokálne predčistiť", "až po odobratí hmoty"),
            ("poťah alebo sedačka", "čistiť povrchovo", "nepremačať výplň"),
        ],
        "textile_rows": [
            ("rifle a nohavice", "ochladiť, odobrať, potom prať", "odolnejší materiál"),
            ("mikina", "pracovať jemne na úplete", "riziko vyťahania"),
            ("gaučový poťah", "podľa štítku a snímateľnosti", "pozor na výplň"),
            ("koberec", "malé úseky a odsávanie vlhkosti", "nešíriť škvrnu"),
        ],
        "sections": [
            ("Ako odstrániť žuvačku z nohavíc", "Ak je to možné, nohavice vložte do vrecka a ochlaďte, alebo miesto schlaďte lokálne. Keď žuvačka stuhne, odoberajte ju tupou hranou po malých častiach.", "Neťahajte ju prudko cez väzbu látky. Pri rifliach je materiál odolnejší, ale aj tam môžete vytvoriť svetlejšie vydraté miesto."),
            ("Žuvačka na mikine alebo teplákoch", "Úplet sa pri ťahaní ľahko deformuje. Preto pracujte pomaly a látku zbytočne nenaťahujte. Po odstránení objemu skontrolujte, či miesto nelepí alebo nemá mastný tieň.", "Ak lepí, použite lokálne malé množstvo pracieho gélu a až potom perte podľa štítku."),
            ("Žuvačka na gaučovom poťahu", "Pri poťahu najprv zistite, či je snímateľný a prateľný. Ak nie je, pracujte iba povrchovo a s minimom vlhkosti. Výplň pod látkou nesmie zbytočne premoknúť.", "Po čistení miesto dosušte vzduchom. Vlhká výplň môže zapáchať ešte horšie než pôvodný problém."),
            ("Ako vyprať textil po odstránení žuvačky", "Pranie má zmysel až po tom, čo je hrubá hmota preč. Inak sa zvyšky môžu rozotrieť na ďalšie kusy alebo zachytiť v záhyboch.", "Textil perte podľa štítku a pred sušením skontrolujte lepkavosť. Ak miesto stále lepí, teplo by problém zafixovalo."),
            ("Kedy testovať čistiaci postup", "Ak uvažujete o silnejšom lokálnom prípravku, najprv ho otestujte na skrytom mieste. Farebný poťah, jemná mikina alebo syntetická látka môžu reagovať inak než očakávate.", "Test je dôležitejší než rýchlosť. Malá žuvačka sa dá zhoršiť na veľkú mapu nesprávnym rozpúšťadlom."),
            ("Prevencia v škole, aute a na sedačke", "Pri deťoch sa žuvačka často objaví na kolenách, vreckách, rukávoch alebo poťahu auta. Pred praním kontrolujte vrecká a miesta, kde sa sedelo.", "Ak sa problém opakuje, nastavte jednoduché pravidlo: žuvačka nepatrí do vrecka ani na nočný stolík pri oblečení."),
        ],
        "expert_title": "Odbornejší pohľad: prečo chlad funguje lepšie ako teplo",
        "expert_p1": "Žuvačka je elastická a lepivá. Chlad ju spevní, takže sa dá mechanicky oddeľovať od textilu. Teplo robí opačný efekt: hmota je mäkšia, rozťahuje sa a ľahšie sa vtlačí medzi vlákna.",
        "expert_p2": "Pri odstraňovaní teda najprv riešite fyzický stav hmoty, až potom zvyšný film. Pranie je posledná fáza, ktorá dokončí hygienu a odstráni zvyšky po predčistení.",
        "checklist": "Pred praním skontrolujte, či na látke nezostal objem žuvačky, lepkavý film, mastný tieň, zvyšky v šve, rub látky, farba, typ úpletu, poťahová výplň a riziko prenosu na ďalšiu bielizeň.",
        "rule": "Pri žuvačke najprv chlad, potom mechanické odobratie, potom lokálne predčistenie a až nakoniec pranie.",
        "recommendation_intro": "Prací gél má zmysel až po odstránení hmoty. Ak v textile stále sedí kus žuvačky, pranie problém skôr roznesie.",
        "product_text": "Vhodný na následné pranie nohavíc, mikín a prateľných poťahov po mechanickom odstránení žuvačky a kontrole lepkavého zvyšku.",
        "category_text": "Pri lepkavých škvrnách je dobré mať doma šetrný prací gél, ale používať ho až po tom, čo je pevný alebo lepkavý zvyšok pod kontrolou.",
        "links": [
            ("/n/ako-odstranit-sliz-z-detskeho-tricka-a-deky-bez-lepkavych-zvyskov", "Ako odstrániť sliz bez lepkavých zvyškov"),
            ("/n/ako-odstranit-vosk-zo-sviecky-z-obrusu-a-textilu", "Ako odstrániť vosk zo sviečky z textilu"),
            ("/n/ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu", "Ako vyprať voskovky z peračníka"),
        ],
        "faq": [
            ("Môžem žuvačku vyprať rovno v práčke?", "Nie je to dobrý nápad. Najprv odstráňte hmotu chladom a tupou hranou."),
            ("Čo ak miesto po odstránení stále lepí?", "Lokálne ho predčistite a perte až potom. Nesušte horúco, kým lepkavosť nezmizne."),
            ("Ako riešiť žuvačku na sedačke?", "Čistite povrchovo, nepremačajte výplň a najprv overte, či je poťah snímateľný a prateľný."),
        ],
    },
    "mustard": {
        "marker": "Detailnejší postup na horčicu z trička, obrusu a utierky",
        "problem": "Horčica je výrazná škvrna, pretože spája žltý pigment, kyslú zložku, korenie a často aj mastnotu z jedla. Ak ju začnete horúcou vodou alebo sušičkou, žltý tieň sa môže zafixovať.",
        "scope": "bavlnené tričko, biely obrus, kuchynskú utierku, zásteru, detské body, prestieranie, rukáv mikiny a textil po grilovaní alebo sendviči",
        "avoid": "vtieranie horčice do strán, horúcu vodu ako prvý krok, sušičku pred kontrolou, pranie špinavej utierky s jemnou bielizňou a bielenie farebného textilu bez testu",
        "diagnosis": [
            "<strong>Najprv odobrať prebytok:</strong> horčicu zoškrabte tupou hranou, netlačte ju hlbšie.",
            "<strong>Preplach z rubu:</strong> pigment tlačte von z vlákna, nie cez škvrnu do látky.",
            "<strong>Žltý tieň kontrolujte pred sušením:</strong> mokrá látka môže vyzerať čistejšie.",
            "<strong>Utierky perte oddelene:</strong> kombinujú jedlo, mastnotu a pach.",
        ],
        "state_rows": [
            ("čerstvá horčica", "zoškrabať a prepláchnuť z rubu", "bez trenia"),
            ("žltý tieň", "predčistiť a nesušiť horúco", "kontrola pri svetle"),
            ("obrus po jedle", "riešiť pigment aj mastnotu", "často kombinovaná škvrna"),
            ("kuchynská utierka", "prať s kuchynským textilom", "neprenášať pach"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "preplach z rubu a lokálne predčistenie", "úplet drží pigment"),
            ("biely obrus", "kontrola pred žehlením", "teplo fixuje zvyšok"),
            ("farebná utierka", "test stálosti farby", "pozor na bielenie"),
            ("detské oblečenie", "jemný postup a dobrý oplach", "kontakt s pokožkou"),
        ],
        "sections": [
            ("Ako odstrániť čerstvú horčicu z trička", "Najprv odoberte hrubú vrstvu horčice tupou hranou. Neutierajte ju do strán, lebo zväčšíte plochu pigmentu. Potom škvrnu preplachujte z rubovej strany studenšou vodou.", "Po preplachu naneste malé množstvo pracieho gélu a jemne prepracujte prstami. Tričko perte podľa štítku a kontrolujte pred sušením."),
            ("Horčica na obruse po jedle", "Obrus často zachytí horčicu spolu s mastnotou z párku, dressingom alebo omáčkou. Preto sledujte nielen žltý tieň, ale aj tmavší okraj mastnoty.", "Obrus nežehliť, kým si nie ste istí, že škvrna zmizla. Žehlenie môže zvyšok zafixovať podobne ako sušička."),
            ("Kuchynská utierka a horčica", "Utierka býva savá a už pred škvrnou môže obsahovať olej, omrvinky alebo pach. Preto ju neperte s jemnou bielizňou. Predčistite najvýraznejšie miesto a perte ju s kuchynským textilom.", "Ak utierka po praní stále zapácha, riešte aj sušenie a veľkosť dávky v práčke."),
            ("Žltý fľak od horčice po praní", "Ak po praní zostal žltý tieň, textil nesušte horúco. Mokrá látka môže klamať, preto miesto skontrolujte pri dennom svetle po voľnom preschnutí.", "Opakovaný mierny postup je bezpečnejší než agresívne bielenie, najmä pri farebnom textile."),
            ("Horčica, kari a paprika: podobné, ale nie rovnaké škvrny", "Horčica, kari a paprika patria medzi výrazné potravinové škvrny, ale každá sa správa trochu inak. Kari často súvisí s kurkumou, paprika s červeným pigmentom a horčica so žltým pigmentom a kyslou zložkou.", "Súvisiace návody sú <a href=\"/n/ako-vyprat-kari-a-kurkumu-z-bavlneneho-tricka-bez-zlteho-tiena\">kari a kurkuma</a> a <a href=\"/n/ako-odstranit-cervenu-papriku-z-tricka-a-kuchynskej-utierky\">červená paprika</a>."),
            ("Prevencia pri grilovaní a deťoch", "Pri grilovaní, hotdogoch a sendvičoch majte poruke servítku, ale škvrnu netrite nasucho do väčšej plochy. Deti si často utrú rukáv alebo tričko, preto kontrolujte aj rukávy a spodný lem.", "Najlepšia prevencia je rýchlo odobrať prebytok a nenechať textil prejsť sušičkou s viditeľným tieňom."),
        ],
        "expert_title": "Odbornejší pohľad: pigment, kyslá zložka a teplo",
        "expert_p1": "Pri horčici rozhoduje žltý pigment a zloženie jedla. Pigment môže zostať vo vlákne aj vtedy, keď sa hrubá vrstva už odstránila. Kyslá a korenistá zložka zároveň môže vytvoriť farebný okraj.",
        "expert_p2": "Princíp je rovnaký ako pri mnohých potravinových škvrnách: prebytok preč, preplach z rubu, lokálne predčistenie, pranie podľa štítku a kontrola pred teplom.",
        "checklist": "Pred praním skontrolujte hrubý zvyšok horčice, žltý tieň, mastný okraj, rub látky, farbu textilu, potlač, kuchynský pach, štítok a to, či sa textil nebude žehliť alebo sušiť horúco.",
        "rule": "Pri horčici najprv odstráňte prebytok a preplachujte z rubu. Teplo patrí až po kontrole, že žltý tieň zmizol.",
        "recommendation_intro": "Pri horčici používajte prací produkt až po odstránení prebytku škvrny. Dôležitý je pigment, mastnota a kontrola pred sušením.",
        "product_text": "Vhodný na následné pranie tričiek, obrusov a kuchynských textílií po preplachu a lokálnom predčistení horčicovej škvrny.",
        "category_text": "Pri potravinových škvrnách majte po ruke prací gél, ale prvý krok je vždy mechanické odstránenie prebytku a šetrné predčistenie.",
        "links": [
            ("/n/ako-vyprat-kari-a-kurkumu-z-bavlneneho-tricka-bez-zlteho-tiena", "Ako vyprať kari a kurkumu"),
            ("/n/ako-odstranit-cervenu-papriku-z-tricka-a-kuchynskej-utierky", "Ako odstrániť červenú papriku"),
            ("/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku", "Ako odstrániť majonézu a dressing"),
        ],
        "faq": [
            ("Ide horčica vyprať z bieleho trička?", "Často áno, ak ju najprv zoškrabete, prepláchnete z rubu a nesušíte horúco pred kontrolou."),
            ("Prečo zostal žltý tieň?", "V látke zostal pigment. Postup zopakujte mierne a nefixujte ho sušičkou ani žehličkou."),
            ("Môžem použiť horúcu vodu?", "Nie ako prvý krok. Začnite studenšou vodou a postupujte podľa štítku textilu."),
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
        <p>V praxi sa oplatí pozerať na celý kontext: {config["scope"]}. Najväčšie riziko je {config["avoid"]}. Keď sa poradie krokov trafí správne, pranie je šetrnejšie a výsledok stabilnejší.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu problému</h2>
        {table(["Stav", "Čo urobiť", "Poznámka"], config["state_rows"])}
        <h2>Postup podľa typu textilu alebo časti</h2>
        {table(["Textil alebo časť", "Postup", "Prečo"], config["textile_rows"])}
        {sections}
        <h2>{config["expert_title"]}</h2>
        <p>{config["expert_p1"]}</p>
        <p>{config["expert_p2"]}</p>
        <p>Pri škvrnách a pachu pomáha nepreskakovať diagnostiku. Prací cyklus má dokončiť čistenie, nie nahradiť odstránenie hrubej špiny, lepkavej hmoty, práškového zvyšku alebo vlhkosti. Praktický odborný zdroj k domácemu triedeniu škvŕn je <a rel="noopener" href="https://extension.illinois.edu/global/stain-solutions" target="_blank">Illinois Extension Stain Solutions</a>.</p>
        <h2>Kontrolný checklist pred praním</h2>
        <p>{config["checklist"]}</p>
        <p>Do jednej dávky nedávajte textílie s protichodnými potrebami. Jemný kostým, čierne tričko s práškovým povlakom, kuchynský obrus s pigmentom a špinavý návlek z kočíka potrebujú iné predčistenie, iné trenie a často aj iný spôsob sušenia.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po praní zostal tieň, lepkavosť, sivý povlak, pach alebo vlhká mapa, nesušte textil horúco. Najprv určte, či ide o pigment, mastnotu, práškový zvyšok, mechanickú hmotu alebo nedostatočné vysušenie.</p>
        <p>Opakovaný mierny postup býva bezpečnejší než jeden agresívny zásah. Pri drahšom, jemnom alebo nejasne označenom textile je lepšie zastaviť sa skôr, než poškodiť farbu, povrch alebo tvar.</p>
        <h2>Ako predísť zafixovaniu pri sušení</h2>
        <p>Sušenie je kontrolný bod. Teplo môže zafixovať pigment, spevniť lepkavý zvyšok, zvýrazniť sivú šmuhu alebo uzavrieť vlhkosť v hrubšej časti. Preto kontrolujte výsledok pri dennom svetle ešte pred sušičkou, žehličkou alebo uložením do skrine.</p>
        <p>Pri textíliách s výplňou, švami, výstužami alebo viacerými vrstvami sledujte aj miesta, ktoré schnú pomalšie. Suchý povrch nemusí znamenať, že je suchý celý kus.</p>
        <h2>Domáca rutina pri opakovanom probléme</h2>
        <p>Ak sa podobná situácia opakuje, zmeňte rutinu pred praním: rýchla kontrola pri koši na bielizeň, odstránenie povrchových zvyškov, lokálne predčistenie, primeraná dávka gélu, nepreplnený bubon a kontrola pred sušením.</p>
        <p>Takto sa z prania nestane náhodný pokus. Budete vedieť, či problém vzniká pri nosení, jedle, počas dažďa, pri kozmetike, v práčke alebo až pri sušení. Šetrí to čas aj textil.</p>
        <h2>Čo sledovať po druhom praní alebo čistení</h2>
        <p>Ak sa problém po druhom šetrnom pokuse nemení, treba rozlíšiť, či ešte čistíte škvrnu, alebo už pozeráte na poškodený povrch. Pigmentový tieň, lepkavý film, sivý práškový povlak a zmenený lesk látky nie sú rovnaký problém.</p>
        <p>Pri opakovanom zásahu si zapamätajte, ktorý krok pomohol najviac: chlad, suché vyklepanie, preplach z rubu, lokálne predčistenie, extra oplach alebo dlhšie sušenie. Nabudúce tak viete začať presnejšie a menej riskovať farbu, tvar aj povrch textilu.</p>
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 24 childcare/costume/cosmetic/sticky/food articles.")
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
                "wave": "retrofit-wave-24-childcare-costume-cosmetic-sticky-food-five",
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
                "wave": "retrofit-wave-24-childcare-costume-cosmetic-sticky-food-five",
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
