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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-21-embellished-water-suit-denim-balsamic-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-21-embellished-water-suit-denim-balsamic-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami",
        "post_id": "2189",
        "url": "https://www.vevo.sk/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami",
        "topic": "embellished",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-18-2026-06-10-articles.json",
        "slug": "ako-odstranit-mapy-od-vody-zo-sedacky-zavesov-a-calunenia",
        "post_id": "2193",
        "url": "https://www.vevo.sk/n/ako-odstranit-mapy-od-vody-zo-sedacky-zavesov-a-calunenia",
        "topic": "water_marks",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-prat-sako-doma-a-kedy-ho-radsej-dat-do-cistiarne",
        "post_id": "2156",
        "url": "https://www.vevo.sk/n/ako-prat-sako-doma-a-kedy-ho-radsej-dat-do-cistiarne",
        "topic": "suit_jacket",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu",
        "post_id": "2155",
        "url": "https://www.vevo.sk/n/ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu",
        "topic": "dark_denim",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-balzamikovy-ocot-z-bieleho-obrusu",
        "post_id": "2175",
        "url": "https://www.vevo.sk/n/ako-odstranit-balzamikovy-ocot-z-bieleho-obrusu",
        "topic": "balsamic",
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
        <p>Pri citlivých textíliách, tmavých farbách a škvrnách, ktoré sa nesmú zafixovať teplom, pomáha mierne dávkovanie, dobrý oplach a pranie podľa štítku.</p>
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
    "embellished": {
        "marker": "Detailnejší postup na flitre, korálky a aplikácie na oblečení",
        "problem": "oblečenie s flitrami, korálkami a aplikáciami sa pri praní nepoškodí len vodou, ale najmä nárazmi v bubne, trením o iné kusy a povolením šitia alebo lepidla",
        "scope": "flitrovom tričku, spoločenských šatách, topoch s korálkami, detských kostýmoch, tylových dieloch s aplikáciami a ozdobených kardiganoch",
        "avoid": "bežný program s uterákmi, pranie so zipsami a suchým zipsom, vysoké otáčky, sušička a žehlenie priamo cez ozdobu",
        "diagnosis": [
            "<strong>Najprv zistite uchytenie:</strong> šité flitre znesú viac než lepené aplikácie, ale stále sa môžu zatrhnúť.",
            "<strong>Ozdoba má vlastný limit:</strong> štítok látky nemusí stačiť, ak je na nej lepidlo, niť, kov alebo plast.",
            "<strong>Riziko je mechanika:</strong> nárazy v bubne a trenie o uteráky poškodia ozdoby rýchlejšie než samotný prací gél.",
            "<strong>Drahé šaty neposúvajte naslepo:</strong> pri hodnote, zložitej výšivke alebo nejasnom štítku je bezpečnejšia čistiareň.",
        ],
        "state_rows": [
            ("šité flitre", "otočiť naruby a použiť vrecko", "stále nízka mechanika"),
            ("lepené aplikácie", "bodové čistenie alebo čistiareň", "lepidlo môže povoliť"),
            ("korálky", "kontrola nite pred praním", "riziko uvoľnenia"),
            ("ozdoba pri škvrne", "čistiť okolie bez trenia ozdoby", "neťahať za nitky"),
        ],
        "textile_rows": [
            ("flitrové tričko", "jemný program alebo ručne", "menej nárazov"),
            ("spoločenské šaty", "často čistiareň", "konštrukcia a hodnota"),
            ("detský kostým", "skontrolovať lepidlá", "lacné aplikácie môžu pustiť"),
            ("top s korálkami", "vrecko a nízke otáčky", "ochrana šitia"),
        ],
        "sections": [
            ("Ako prať flitrové tričko", "Flitrové tričko otočte naruby, zapnite prípadné zipsy na iných kusoch mimo dávky a použite ochranné vrecko. Ak je tričko len zapotené, často stačí krátke jemné pranie, nie dlhý program s veľkým bubnovaním.", "Po praní ho nevytáčajte krútením. Vodu nechajte odtiecť, tričko jemne vyrovnajte a sušte mimo priameho tepla."),
            ("Ako prať šaty s korálkami", "Korálky bývajú prišité niťou, ktorá sa môže vo vode uvoľniť alebo zachytiť. Pred praním skontrolujte, či sa niektoré korálky nehýbu. Ak áno, domáce pranie radšej odložte alebo riešte iba lokálne miesto bez namáčania celej ozdoby.", "Pri spoločenských šatách je často rozhodujúca aj podšívka, výstuž, tyl alebo zips. Nie je to rovnaké ako bežné bavlnené tričko."),
            ("Lepené aplikácie a potlače", "Lepené ozdoby sú najrizikovejšie. Voda, teplo a mechanika môžu oslabiť lepidlo a aplikácia sa začne odlepovať po okrajoch. Ak je štítok nejasný, testujte len malú časť alebo zvoľte profesionálne čistenie.", "Najhoršia kombinácia je horúca voda, sušička a trenie o hrubé textílie."),
            ("Čo robiť, keď vypadne fliter alebo korálka", "Ak sa ozdoba uvoľní, neťahajte za voľnú niť. Zastavte pranie, nechajte kus uschnúť a zvážte opravu. Pokračovať v praní môže zväčšiť poškodenie a uvoľniť ďalšie ozdoby.", "Pri praní v práčke môžu voľné korálky poškriabať iné textílie alebo zostať v bubne."),
            ("Sušenie a žehlenie ozdobeného oblečenia", "Sušička a priame žehlenie cez flitre sú rizikové. Plastové ozdoby sa môžu zdeformovať, kovové časti zohriať a lepidlo povoliť. Sušte voľne, tvar upravte rukou a žehlite len podľa štítku z rubovej strany cez ochrannú látku.", "Ak je hlavným problémom pokrčenie, pri hodnotných šatách je často lepšie odborné naparovanie."),
        ],
        "depth": [
            ("Šité verzus lepené ozdoby", "Šité ozdoby držia mechanicky, lepené chemicky. Preto môžu mať rozdielnu odolnosť aj na rovnakom kuse oblečenia. Pri šitých flitroch sledujte nitky, pri lepených okraje a citlivosť na teplo.", "Toto rozlíšenie je praktickejšie než otázka, či je látka bavlna alebo polyester. Ozdoba môže byť najslabším miestom celého kusu."),
            ("Prečo čistiareň nie je zlyhanie", "Pri spoločenských šatách, saku s aplikáciami alebo drahom tope môže byť profesionálne čistenie najlacnejšia prevencia škody. Domáce pranie má zmysel pri jednoduchších a jasne prateľných kusoch.", "Ak neviete, či je ozdoba šitá, lepená alebo tepelne fixovaná, neriskujte dlhý prací cyklus."),
        ],
        "expert_title": "Odbornejší pohľad: konštrukcia odevu je dôležitejšia než samotná látka",
        "expert_p1": "Pri ozdobenom oblečení sa stretáva viac materiálov naraz: základná tkanina, niť, plast, kov, lepidlo, potlač a niekedy aj podšívka. Každá časť môže mať inú reakciu na vodu, teplo a trenie. Preto je opatrný postup logickejší než univerzálny prací program.",
        "expert_p2": "Najčastejšia chyba je hodnotiť len látku. Ak je látka prateľná, ale aplikácia nie, výsledkom môže byť čistý textil s poškodenou ozdobou.",
        "checklist": "Skontrolujte štítok, uchytenie ozdôb, voľné nitky, lepené okraje, zipsy, suchý zips, podšívku a to, či je škvrna lokálna alebo je potrebné prať celý kus. Pri pochybnosti nezačínajte práčkou.",
        "rule": "Pri flitroch a korálkach chráňte ozdobu pred trením: naruby, vrecko, minimum otáčok, žiadne hrubé kusy a žiadna sušička.",
        "recommendation_intro": "Pri prateľných ozdobených kusoch používajte prací gél striedmo. Viac gélu neochráni flitre ani korálky, dôležitejší je jemný režim a dobrý oplach.",
        "product_text": "Vhodný na šetrné pranie bežných prateľných textílií s ozdobami, ak štítok povoľuje domáce pranie a aplikácie sú pevné.",
        "links": [
            ("/n/ako-odstranit-trblietky-z-siat-saka-a-kabata-po-oslave", "Ako odstrániť trblietky zo šiat a saka"),
            ("/n/ako-prat-tylovu-suknu-zavoj-a-jemny-tyl-bez-potrhania", "Ako prať tylovú sukňu a závoj"),
            ("/n/ako-prat-spolocenske-saty-doma-a-kedy-zvolit-cistiaren", "Ako prať spoločenské šaty doma"),
        ],
        "faq": [
            ("Môžu ísť flitre do práčky?", "Len ak to povoľuje štítok a ozdoby sú pevné. Použite ochranné vrecko, jemný program a nízke otáčky."),
            ("Čo ak sú aplikácie lepené?", "Lepené aplikácie sú rizikové. Čistite skôr lokálne alebo zvoľte čistiareň."),
            ("Môžem flitrové oblečenie sušiť v sušičke?", "Vo väčšine prípadov nie. Teplo a nárazy môžu deformovať ozdoby alebo oslabiť lepidlo."),
        ],
    },
    "water_marks": {
        "marker": "Detailnejší postup na mapy od vody na sedačke, závesoch a čalúnení",
        "problem": "mapa od vody nie je vždy čistá voda, ale často zóna, kde sa rozpustili prach, minerály, zvyšky čistiaceho prípravku alebo nečistoty v čalúnení a po nerovnom schnutí zostal okraj",
        "scope": "sedačke, kresle, návleku, závese, dekoračnom vankúši, čalúnení a prateľnom poťahu",
        "avoid": "lokálne premočenie malého kruhu, silné drhnutie, sušičku pri neznámom poťahu a čistenie bez testu stálofarebnosti",
        "diagnosis": [
            "<strong>Mapa má okraj:</strong> problém často nie je samotná voda, ale nečistoty presunuté k okraju schnutia.",
            "<strong>Čalúnenie nie je tričko:</strong> výplň pod látkou môže držať vlhkosť dlhšie než povrch.",
            "<strong>Závesy sa správajú inak:</strong> ak sú prateľné, riešite celý kus, nie len bod.",
            "<strong>Najprv test:</strong> farba, vlas a povrchová úprava musia zvládnuť vodu aj prípravok.",
        ],
        "state_rows": [
            ("čerstvá mokrá mapa", "odsávať suchou handrou", "netrieť do strán"),
            ("suchý kruh", "čistiť širšiu prechodovú zónu", "nie iba bod"),
            ("prateľný poťah", "prať podľa štítku", "kontrola zrazenia"),
            ("neznáme čalúnenie", "test a skôr odborné čistenie", "riziko väčšej mapy"),
        ],
        "textile_rows": [
            ("sedačka", "minimum vody a rýchle sušenie", "výplň drží vlhkosť"),
            ("záves", "prať celý panel podľa štítku", "lokálne čistenie môže nechať rozdiel"),
            ("poťah vankúša", "samostatne a naruby", "farba a zips"),
            ("čalúnené kreslo", "test na skrytom mieste", "povrchová úprava"),
        ],
        "sections": [
            ("Ako odstrániť mapy od vody zo sedačky", "Najprv odsajte zvyšnú vlhkosť suchou bielou handrou alebo uterákom. Netrite kruh do strán. Ak je mapa už suchá, pracujte so širšou prechodovou zónou, nie len s ostrým okrajom fľaku.", "Pri sedačke je dôležité použiť čo najmenej vody. Premočená výplň schne dlho a môže vytvoriť ďalší okraj alebo zatuchnutý pach."),
            ("Mapy od vody na závesoch", "Pri závesoch býva lepšie riešiť celý panel podľa štítku než len jedno miesto. Lokálne čistenie môže vytvoriť rozdiel v odtieni, najmä ak je zvyšok závesu zaprášený. Pred praním odstráňte prach a skontrolujte háčiky.", "Ak záves po praní zapácha alebo je zatuchnutý, súvisí to skôr so sušením a vetraním než s väčším množstvom pracieho gélu."),
            ("Čalúnenie, návleky a poťahy", "Ak je poťah snímateľný a štítok povoľuje pranie, postupujte ako pri citlivejšej bielizni: mierna dávka pracieho gélu, nižšia teplota a kontrola pred sušením. Pri pevnom čalúnení je riziko vyššie, pretože neviete, čo sa deje vo výplni.", "Pri neznámom materiáli testujte na skrytom mieste. Ak farba púšťa alebo vlas mení smer, nepokračujte naslepo."),
            ("Prečo vznikne okraj po čistení", "Keď namočíte len malý bod, voda rozpustí prach a zvyšky v látke. Pri schnutí sa presunú na hranicu mokrej zóny a zostane kruh. Preto sa pri čalúnení často čistí väčšia prechodová plocha a potom sa povrch rovnomerne vysuší.", "Rovnomerné schnutie je rovnako dôležité ako samotný čistiaci krok."),
            ("Kedy zavolať profesionálne čistenie", "Ak ide o drahú sedačku, svetlé čalúnenie, zamatový povrch, hodvábny záves, neznámy poťah alebo veľkú mapu, domáci pokus môže problém zväčšiť. Profesionálne čistenie má význam najmä vtedy, keď je voda už vo výplni.", "Pri plesnivom zápachu alebo opakovanej vlhkosti najprv riešte príčinu, nie iba fľak."),
        ],
        "depth": [
            ("Voda ako nosič nečistôt", "Voda sama o sebe nemusí zanechať škvrnu. Často však prenesie minerály, prach, zvyšky jedla, saponátu alebo nečistoty vo vlákne. Po odparení ostane viditeľný okraj. Preto sa mapy od vody riešia inak než jednoduchá kvapka na hladkom povrchu.", "Čím viac bol textil predtým zaprášený, tým vyššie riziko mapy po lokálnom namočení."),
            ("Rýchle sušenie bez tepelného šoku", "Cieľom je dostať vlhkosť preč, ale neprehriať materiál. Pri čalúnení pomáha prúdenie vzduchu, suché odsávanie a vetranie. Horúci radiátor alebo fén zblízka môže zmeniť povrch, stiahnuť vlákno alebo zvýrazniť okraj.", "Pri prateľných poťahoch kontrolujte tvar ešte za vlhka, aby sa poťah po vyschnutí zbytočne nedeformoval."),
        ],
        "expert_title": "Odbornejší pohľad: nerovnomerné schnutie a migrácia nečistôt",
        "expert_p1": "Mapy od vody sú často výsledkom migrácie nečistôt. Vlhkosť rozpustí zvyšky v textile, tie sa presunú k okraju mokrej oblasti a po vyschnutí sa zviditeľnia. Preto lokálne dočisťovanie malého kruhu môže vytvoriť ešte ostrejšiu mapu.",
        "expert_p2": "Pri čalúnení je podstatné aj to, že povrch môže vyzerať suchý, ale výplň ešte drží vlhkosť. Dlhé schnutie zvyšuje riziko zápachu a ďalších máp.",
        "checklist": "Pred čistením zistite, či je poťah snímateľný, či štítok povoľuje vodu, či farba nepúšťa, či je mapa čerstvá alebo stará a či pod textilom nie je výplň, ktorá by zostala mokrá.",
        "rule": "Pri mapách od vody nečistite iba ostrý okraj. Pracujte jemne, s minimom vody, cez širšiu prechodovú zónu a s dôrazom na rýchle rovnomerné schnutie.",
        "recommendation_intro": "Prací gél má zmysel najmä pri snímateľných a prateľných poťahoch alebo závesoch. Pri pevnom čalúnení sa riaďte štítkom a najprv testujte na skrytom mieste.",
        "product_text": "Vhodný na prateľné poťahy, návleky a závesy, ak štítok povoľuje vodné pranie. Pri pevnom čalúnení postupujte opatrne.",
        "links": [
            ("/n/ako-odstranit-pivo-z-tricka-obrusu-a-sedacky-bez-zapachu", "Ako odstrániť pivo zo sedačky"),
            ("/n/ako-vyprat-stare-skvrny-kompletny-sprievodca-pre-ciste-oblecenie", "Ako vyprať staré škvrny"),
            ("/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia", "Prečo textílie zapáchajú po praní"),
        ],
        "faq": [
            ("Prečo po vode zostal kruh?", "Voda presunula prach, minerály alebo zvyšky prípravku k okraju mokrej zóny a po vyschnutí zostal viditeľný obrys."),
            ("Môžem vyčistiť sedačku pracím gélom?", "Len veľmi opatrne a iba ak materiál znesie vodu. Pri snímateľnom poťahu sa riaďte štítkom, pri pevnom čalúnení testujte."),
            ("Ako sušiť čalúnenie po čistení?", "Rýchlo, rovnomerne a bez horúceho tepla zblízka. Pomáha vetranie a suché odsávanie vlhkosti."),
        ],
    },
    "suit_jacket": {
        "marker": "Detailnejší postup na sako, podšívku, ramená a čistiareň",
        "problem": "sako nie je bežná bunda, pretože drží tvar vďaka podšívke, výstuži, ramenám, švom a často aj vlnenému alebo zmesovému materiálu",
        "scope": "pánskom saku, dámskom blejzri, ľahkom letnom saku, podšívke, golieri, manžetách a ramennej výstuži",
        "avoid": "pranie naslepo v práčke, vysoké otáčky, vešanie mokrého saka za ramená, sušičku a žehlenie bez rešpektovania materiálu",
        "diagnosis": [
            "<strong>Sako drží tvar:</strong> poškodenie výstuže je často horšie než samotná škvrna.",
            "<strong>Podšívka sa môže zraziť inak:</strong> vrchná látka a vnútro nemusia reagovať rovnako.",
            "<strong>Golier a manžety riešte lokálne:</strong> celý kus netreba prať pri malej špine.",
            "<strong>Čistiareň je správna voľba:</strong> pri vlne, výstuži, škvrne neznámeho pôvodu alebo drahom saku.",
        ],
        "state_rows": [
            ("pach po nosení", "vetrať a kefovať", "nie vždy prať"),
            ("špinavý golier", "lokálne čistenie", "bez premočenia výstuže"),
            ("pokrčené sako", "napariť podľa štítku", "nie horúco naslepo"),
            ("škvrna na vlne", "čistiareň", "nižšie riziko poškodenia"),
        ],
        "textile_rows": [
            ("vlnené sako", "skôr čistiareň", "citlivosť na vodu a tvar"),
            ("polyesterový blejzer", "možno jemne podľa štítku", "podšívka rozhoduje"),
            ("ľanové sako", "pozor na krčenie", "tvarovať a žehliť opatrne"),
            ("sako s aplikáciou", "čistiareň alebo bodovo", "ozdoby a výstuže"),
        ],
        "sections": [
            ("Ako zistiť, či sa sako môže prať doma", "Začnite štítkom, ale nekončite pri ňom. Pozrite sa na materiál, podšívku, ramená, výstuž, gombíky a prípadné lepené časti. Ak štítok povoľuje len chemické čistenie, domáce pranie nie je dobrý test odvahy.", "Ak štítok povoľuje vodné pranie, stále používajte najjemnejší režim a rátajte s tým, že tvar je hlavné riziko."),
            ("Lokálne čistenie goliera a manžiet", "Golier a manžety zachytávajú pot, kožný maz, parfum a make-up. Čistite ich mierne navlhčenou bielou handrou a malým množstvom vhodného prípravku, bez premočenia. Potom miesto jemne dočistite čistou vlhkou handrou.", "Pri tmavom saku najprv testujte, či farba nepúšťa. Pri svetlom saku si dajte pozor na okraje po lokálnom čistení."),
            ("Ako osviežiť sako bez prania", "Mnohé saká nepotrebujú po jednom nosení prať. Pomôže vyvetranie na širokom vešiaku, jemné vykefovanie, kontrola vreciek a oddelenie od prevoňaných alebo vlhkých textílií. Pach sa často drží v podšívke a podpazuší.", "Ak pach ostáva aj po vetraní, neriešte ho iba vôňou. Najprv zistite, či nejde o pot, vlhkosť alebo zvyšok parfumového produktu."),
            ("Sako v práčke: kedy je to riziko", "Práčka je riziková najmä pre ramená, výstuž, podšívku a švy. Aj keď sa látka nezrazí, sako môže stratiť líniu. Vysoké odstreďovanie vytvorí záhyby, ktoré sa ťažko vracajú späť.", "Ak domáce pranie vôbec zvolíte, sako perte samostatne alebo s veľmi jemnými kusmi, bez preplnenia bubna a bez sušičky."),
            ("Sušenie saka bez deformácie", "Mokré sako nevešajte na tenký vešiak, ktorý vytlačí ramená. Nechajte vodu odtiecť, vytvarujte ramená a použite široký vešiak alebo rovné dosušenie podľa materiálu. Teplo z radiátora môže zdeformovať podšívku aj vrchnú látku.", "Po vyschnutí pomôže jemné naparenie podľa štítku, nie agresívne žehlenie cez lesklé miesta."),
        ],
        "depth": [
            ("Prečo sa sako po praní zvlní", "Vlnená alebo zmesová vrchná látka, podšívka a výstuž môžu schnúť rozdielnou rýchlosťou a meniť rozmery inak. Výsledkom je zvlnenie okrajov, napätie pri švoch alebo zmena ramien.", "To je dôvod, prečo je pri saku dôležitá konštrukcia, nie len zloženie textilu na štítku."),
            ("Kedy čistiareň chráni peniaze", "Ak je sako drahé, vlnené, vystužené, súčasťou obleku alebo má neznámu škvrnu, čistiareň je rozumnejšia než domáci pokus. Cena poškodeného saka býva vyššia než profesionálne čistenie.", "Domáce čistenie má zmysel najmä pri jednoduchšom blejzri, prateľnom materiáli a malej lokálnej špine."),
        ],
        "expert_title": "Odbornejší pohľad: sako je konštrukčný odev, nie len textília",
        "expert_p1": "Sako má držať siluetu. Tú vytvára kombinácia vrchnej látky, výstuží, podšívky, švov a žehlenia pri výrobe. Voda a mechanika môžu tieto vrstvy rozhýbať odlišne, preto je domáce pranie saka rizikovejšie než pranie košele.",
        "expert_p2": "Pri saku je cieľom predĺžiť interval medzi čisteniami: vetrať, kefovať, riešiť malé miesta včas a profesionálne čistiť vtedy, keď je riziko domáceho zásahu vysoké.",
        "checklist": "Pred domácim zásahom skontrolujte štítok, materiál, podšívku, ramená, výstuž, gombíky, farbu, typ škvrny a hodnotu saka. Ak sa niektorý bod nedá posúdiť, nerobte celé pranie v práčke.",
        "rule": "Pri saku najprv vetrať a čistiť lokálne. Celé domáce pranie zvoľte iba vtedy, keď to povoľuje štítok a konštrukcia saka je jednoduchá.",
        "recommendation_intro": "Prací gél používajte len pri saku, ktoré štítok povoľuje prať vo vode. Pri vlne, výstuži alebo nejasnej škvrne je bezpečnejšia čistiareň.",
        "product_text": "Vhodný na šetrné pranie jednoduchších prateľných blejzrov a podšívok podľa štítku. Pri klasickom saku zvážte čistiareň.",
        "links": [
            ("/n/ako-prat-spolocenske-saty-doma-a-kedy-zvolit-cistiaren", "Ako prať spoločenské šaty doma"),
            ("/n/ako-prat-oblecenie-s-flitrami-koralkami-a-aplikaciami", "Ako prať oblečenie s aplikáciami"),
            ("/n/ako-odstranit-podkladovy-krem-z-goliera-bluzky-a-kosele", "Ako odstrániť podkladový krém z goliera"),
        ],
        "faq": [
            ("Môžem sako prať v práčke?", "Len ak to povoľuje štítok a ide o jednoduchší prateľný kus. Pri klasickom vystuženom saku je bezpečnejšia čistiareň."),
            ("Ako odstrániť pach zo saka?", "Najprv vetrať na širokom vešiaku, potom riešiť lokálne podpazušie alebo podšívku. Vôňa nemá zakrývať pot a vlhkosť."),
            ("Prečo sa sako po praní zdeformovalo?", "Vrchná látka, podšívka a výstuž sa mohli zraziť alebo vyschnúť rozdielne. Vysoké otáčky a sušička riziko zvyšujú."),
        ],
    },
    "dark_denim": {
        "marker": "Detailnejší postup na rifľovú bundu a tmavé džínsy bez púšťania farby",
        "problem": "tmavý denim môže pri prvých praniach púšťať farbu a zároveň blednúť trením, vysokou teplotou, silným programom alebo praním s nevhodnou bielizňou",
        "scope": "tmavých džínsoch, rifľovej bunde, čiernom denime, elastických rifliach, nových nohaviciach a farebnej bavlnenej zmesi",
        "avoid": "horúcu vodu, preplnený bubon, pranie s bielou bielizňou, sušičku na vysokom teple a časté pranie po každom krátkom nosení",
        "diagnosis": [
            "<strong>Nový denim perte oddelene:</strong> prvé prania sú najrizikovejšie pre púšťanie farby.",
            "<strong>Otočenie naruby pomáha:</strong> znižuje trenie viditeľnej strany o bubon.",
            "<strong>Studenšia voda chráni farbu:</strong> vysoká teplota zrýchľuje blednutie.",
            "<strong>Elastické rifle neprehrievať:</strong> elastan v denime nemá rád sušičku a horúce pranie.",
        ],
        "state_rows": [
            ("nové tmavé džínsy", "prať samostatne naruby", "riziko púšťania"),
            ("rifľová bunda", "menej časté pranie", "vetrať medzi noseniami"),
            ("čierny denim", "jemný gél a nízka teplota", "blednutie na hranách"),
            ("elastické rifle", "bez sušičky", "ochrana pružnosti"),
        ],
        "textile_rows": [
            ("100 % bavlnený denim", "samostatne a naruby", "farba a trenie"),
            ("strečové džínsy", "nižšia teplota", "elastan"),
            ("rifľová bunda", "prať len keď treba", "tvar a švy"),
            ("čierne rifle", "bez práškuových zvyškov", "viditeľné šmuhy"),
        ],
        "sections": [
            ("Ako prať tmavé džínsy prvýkrát", "Nové tmavé džínsy perte samostatne alebo s veľmi podobnými tmavými farbami. Otočte ich naruby, zapnite zips a gombík a použite kratší jemnejší program. Prvé pranie je najdôležitejšie, pretože prebytočné farbivo sa môže uvoľniť.", "Nedávajte k nim biele ponožky, uteráky ani svetlé tričká. Ak farba pustí, škoda bude väčšia než úspora jednej dávky."),
            ("Ako prať rifľovú bundu", "Rifľová bunda sa nemusí prať po každom nosení. Často stačí vyvetranie, vykefovanie prachu a lokálne čistenie manžiet alebo goliera. Keď ju periete celú, otočte ju naruby a zvoľte nižšiu teplotu.", "Pri bunde sledujte aj kovové gombíky, švy a prípadnú podšívku. Tie môžu ovplyvniť sušenie aj tvar."),
            ("Prečo tmavé džínsy blednú", "Farba sa nestráca iba rozpustením vo vode. Veľkú úlohu má trenie o bubon, o iné kusy a o vlastné záhyby. Preto pomáha prať naruby, nepreplniť práčku a nepoužívať zbytočne dlhý program.", "Viditeľné blednutie býva najmä na hranách, kolenách, v rozkroku a pri švoch."),
            ("Čierny denim a biele šmuhy", "Na tmavom denime sú viditeľné zvyšky pracieho prášku, priveľa gélu aj nedostatočný oplach. Ak po praní vidíte šmuhy, problém nemusí byť farba, ale dávkovanie alebo preplnený bubon.", "K tejto téme nadväzuje návod <a href=\"/n/ako-odstranit-biele-smuhy-od-pracieho-prasku-z-cierneho-oblecenia\">ako odstrániť biele šmuhy z čierneho oblečenia</a>."),
            ("Sušenie denimu bez zbytočného blednutia", "Denim sušte voľne a radšej mimo priameho ostrého slnka. Sušička môže zvyšovať trenie, teplo a zrážanie, najmä pri strečových rifliach. Pred sušením nohavice vyrovnajte a skontrolujte, či nie sú na nich zvyšky gélu.", "Pri elastických rifliach je šetrné sušenie dôležité aj pre pružnosť, nielen pre farbu."),
        ],
        "depth": [
            ("Púšťanie farby verzus prirodzené blednutie", "Púšťanie farby znamená, že farbivo sa prenáša na vodu alebo iné textílie. Prirodzené blednutie vzniká postupne trením a nosením. Obe veci vyzerajú podobne, ale prevencia sa líši: nové kusy perte oddelene, dlhodobo znižujte trenie a teplo.", "Ak už farba pustila na iný textil, riaďte sa postupom pre zafarbenú bielizeň a nesušte ju horúco."),
            ("Denim s elastanom", "Strečové džínsy obsahujú elastické vlákna, ktoré pomáhajú pohodliu, ale nemajú rady horúcu vodu, sušičku a agresívne žmýkanie. Ak rifle stratia pružnosť alebo sa vytiahnu, problém nemusí byť len veľkosť, ale aj starostlivosť.", "Pri strečovom denime je dobré myslieť podobne ako pri športovej bielizni: menej tepla, menej aviváže a viac kontroly sušenia."),
        ],
        "expert_title": "Odbornejší pohľad: farbivo, trenie a prebytočná farba v novom denime",
        "expert_p1": "Tmavý denim je farbený tak, aby mal výrazný odtieň, ale časť farby sa môže pri prvých praniach uvoľňovať. Zároveň sa farba mechanicky obrusuje na miestach trenia. Preto nestačí len znížiť teplotu; treba znížiť aj mechanické namáhanie.",
        "expert_p2": "Pri tmavom oblečení je dôležitý aj čistý oplach. Zvyšky pracieho prostriedku vytvoria na povrchu šmuhy, ktoré vyzerajú ako vyblednutie, hoci ide o usadeninu.",
        "checklist": "Pred praním zapnite zips, otočte denim naruby, oddeľte svetlé kusy, skontrolujte vrecká, nastavte nižšiu teplotu, nepreplňte bubon a po praní skontrolujte šmuhy pred sušením.",
        "rule": "Pri tmavom denime chráňte farbu: naruby, samostatne pri prvých praniach, nižšia teplota, nepreplnený bubon a bez horúcej sušičky.",
        "recommendation_intro": "Pri tmavom denime používajte primerané množstvo pracieho gélu a dobrý oplach. Priveľa prostriedku môže na čiernych a tmavých rifliach vytvoriť viditeľný povlak.",
        "product_text": "Vhodný na šetrné pranie tmavého a farebného oblečenia pri rozumnom dávkovaní a praní naruby podľa štítku.",
        "links": [
            ("/n/ako-zabranit-pustaniu-farby-pri-prani-noveho-oblecenia", "Ako zabrániť púšťaniu farby"),
            ("/n/pustila-farba-v-pracke-co-urobit-s-bielym-trickom-a-ruzovou-bieliznou", "Pustila farba v práčke"),
            ("/n/ako-odstranit-biele-smuhy-od-pracieho-prasku-z-cierneho-oblecenia", "Biele šmuhy na čiernom oblečení"),
        ],
        "faq": [
            ("Ako prať nové tmavé džínsy?", "Samostatne alebo s podobnými tmavými farbami, naruby, pri nižšej teplote a bez preplnenia práčky."),
            ("Prečo rifle púšťajú farbu?", "Nový denim môže mať prebytočné farbivo a pri praní sa uvoľní. Riziko zvyšuje teplo, trenie a pranie so svetlými vecami."),
            ("Môžem dať džínsy do sušičky?", "Pri tmavom a strečovom denime radšej nie. Teplo a trenie zrýchľujú blednutie a môžu oslabiť elastan."),
        ],
    },
    "balsamic": {
        "marker": "Detailnejší postup na balzamikový ocot z bieleho obrusu",
        "problem": "balzamikový ocot spája tmavé zafarbenie, kyslosť, cukry a často aj jedlo alebo olej, preto môže na bielom obruse zanechať hnedý tieň aj po bežnom praní",
        "scope": "bielom obruse, bavlnenej servítke, kuchynskej utierke, ľanovom prestieraní, košeli pri stole a svetlom textile po oslave",
        "avoid": "horúcu vodu na začiatku, žehlenie pred kontrolou, sušičku so zvyškovým tieňom a drhnutie škvrny do šírky",
        "diagnosis": [
            "<strong>Riešte škvrnu hneď:</strong> tmavé zafarbenie sa na bielom textile rýchlo zvýrazní.",
            "<strong>Najprv odsajte prebytok:</strong> nerozotierajte ocot do väčšej mapy.",
            "<strong>Pozor na kombináciu s olejom:</strong> šalátový dresing je aj mastná škvrna.",
            "<strong>Teplo až po kontrole:</strong> žehlenie obrusu môže zafixovať zvyškový hnedý tieň.",
        ],
        "state_rows": [
            ("čerstvý ocot", "odsávať a opláchnuť studenšou vodou", "nerozotierať"),
            ("ocot s olejom", "riešiť farbu aj mastnotu", "dresing"),
            ("zaschnutý tieň", "predpieranie a kontrola pred teplom", "trpezlivo"),
            ("biely obrus", "nežehliť pred výsledkom", "riziko fixácie"),
        ],
        "textile_rows": [
            ("bavlnený obrus", "lokálne predčistiť a vyprať", "dobrá odolnosť"),
            ("ľanové prestieranie", "jemnejšie trenie", "vlákno sa krčí"),
            ("servítka", "rýchly oplach", "malá plocha"),
            ("košeľa", "test farby a švov", "kontakt s pokožkou"),
        ],
        "sections": [
            ("Ako odstrániť čerstvý balzamikový ocot", "Najprv odoberte prebytok lyžičkou alebo čistou bielou handrou. Netrite škvrnu do šírky. Potom miesto prepláchnite z rubovej strany studenšou vodou, aby sa časť zafarbenia dostala von z vlákna.", "Až potom použite malé množstvo pracieho gélu alebo vhodné lokálne predčistenie podľa materiálu."),
            ("Balzamikový ocot s olejom alebo dresingom", "Ak bol balzamikový ocot súčasťou šalátového dresingu, riešite dve veci naraz: tmavý farebný tieň a mastnú zložku. Samotný oplach môže zlepšiť farbu, ale mastnota ostane ako mapa.", "Pri mastnej časti pomáha lokálne predpranie a následné pranie podľa štítku. Teplo pridajte až po kontrole."),
            ("Zaschnutý balzamikový ocot na obruse", "Zaschnutú škvrnu najprv navlhčite a nechajte krátko pracovať, nešúchajte ju agresívne nasucho. Pri bielom obruse je lákavé použiť silný zásah, ale ten môže vytvoriť svetlý kruh okolo pôvodnej škvrny.", "Postup radšej zopakujte mierne. Obrus kontrolujte pri dennom svetle, pretože mokrá látka vie hnedý tieň dočasne skryť."),
            ("Prečo nežiť obrus pred kontrolou", "Biely obrus sa často po praní automaticky žehlí. Pri balzamikovom octe je to rizikové, ak zostal čo i len slabý tieň. Teplo môže zvyšok zafixovať a ďalšie čistenie bude ťažšie.", "Pred žehlením nechajte miesto preschnúť alebo ho skontrolujte veľmi pozorne."),
            ("Balzamikový ocot na ľane", "Ľan je savý a krásny, ale pri drhnutí sa môže mechanicky opotrebovať a po praní sa výrazne krčí. Pri ľanovom prestieraní pracujte jemnejšie než pri hrubej bavlnenej utierke. Dôležité je predčistenie bez poškodenia štruktúry.", "Po praní ľan tvarujte za vlhka a žehlite až po kontrole škvrny."),
        ],
        "depth": [
            ("Balzamikový ocot verzus čierny čaj", "Obe škvrny môžu na bielom textile vyzerať ako hnedý tieň, ale balzamikový ocot býva kyslý a často obsahuje cukry alebo prímes jedla. Preto sa môže správať inak než čistý nápoj. Ak je v škvrne aj olej, postup musí riešiť mastnotu.", "Súvisiaci postup nájdete aj v návode na <a href=\"/n/ako-vyprat-cierny-caj-z-bieleho-obrusu-bez-hnedych-map\">čierny čaj z bieleho obrusu</a>."),
            ("Prečo nestačí bežné pranie", "Bežné pranie môže odstrániť časť škvrny, ale ak neprebehne lokálne predčistenie, hnedý tieň sa môže len zosvetliť a zostať. Pri bielom obruse je rozdiel viditeľný najmä po vyschnutí a vyžehlení.", "Preto je dôležitá kontrola pred sušením a pred žehlením, nie až pri prestieraní stola."),
        ],
        "expert_title": "Odbornejší pohľad: farebný tieň, kyslosť a kombinované škvrny",
        "expert_p1": "Balzamikový ocot je problematický preto, že nejde len o vodnú škvrnu. Má tmavé zafarbenie, kyslú zložku a často sa na obrus dostane spolu s olejom, bylinkami alebo jedlom. Pri čistení preto rozlišujte, či riešite farbu, mastnotu alebo oboje.",
        "expert_p2": "Odborné návody na škvrny zvyčajne zdôrazňujú rýchle odstránenie prebytku, prácu od rubovej strany a kontrolu pred teplom. Pri bielom obruse sú tieto kroky dôležité najmä preto, že žehlenie býva automatický ďalší krok.",
        "checklist": "Pred praním zistite, či je škvrna čerstvá alebo zaschnutá, či obsahuje olej, z akého materiálu je obrus, či textil znesie lokálne predčistenie a či sa pred žehlením dá miesto skontrolovať za sucha.",
        "rule": "Pri balzamikovom octe najprv odobrať prebytok, oplachovať z rubovej strany, predčistiť lokálne, vyprať a až po kontrole použiť teplo.",
        "recommendation_intro": "Pri bielom obruse používajte prací gél ako súčasť predprania a následného prania. Nečakajte, že samotný bežný cyklus bez lokálneho zásahu odstráni hnedý tieň.",
        "product_text": "Vhodný na šetrné predpranie a pranie obrusov, servítok a svetlých textílií podľa štítku, bez zbytočného drhnutia.",
        "links": [
            ("/n/ako-vyprat-cierny-caj-z-bieleho-obrusu-bez-hnedych-map", "Ako vyprať čierny čaj z bieleho obrusu"),
            ("/n/ako-odstranit-sojovu-omacku-z-kosele-obrusu-a-prestierania", "Ako odstrániť sójovú omáčku"),
            ("/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku", "Ako odstrániť majonézu a dressing"),
        ],
        "faq": [
            ("Ako rýchlo riešiť balzamikový ocot na obruse?", "Hneď odobrať prebytok, nerozotierať a preplachovať z rubovej strany studenšou vodou."),
            ("Čo ak bol balzamikový ocot v šalátovom dresingu?", "Riešte aj mastnotu. Po oplachu použite lokálne predpranie a až potom klasické pranie podľa štítku."),
            ("Môžem obrus po praní hneď žehliť?", "Nie, najprv skontrolujte, či nezostal hnedý tieň. Teplo môže zvyšok škvrny zafixovať."),
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
        <p>{config["problem"].capitalize()}. Preto sa oplatí najprv rozlíšiť materiál, konštrukciu, typ znečistenia a to, či je bezpečné prať celý kus alebo iba lokálne miesto.</p>
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
        <p>Pri škvrnách a citlivých odevoch je užitočné postupovať pomaly: najprv odstrániť prebytok alebo prach, potom zvoliť mierny postup, následne skontrolovať výsledok a až potom sušiť alebo žehliť. Tento princíp je konzervatívny, ale chráni textil pred zbytočným poškodením.</p>
        {depth}
        <h2>Kontrolný checklist pred praním</h2>
        <p>{config["checklist"]}</p>
        <p>Do jednej dávky nedávajte textílie s protichodnými potrebami. Ozdobené šaty, čalúnený poťah, sako, tmavý denim a biely obrus po škvrne potrebujú odlišný režim. Triedenie je súčasť výsledku, nie detail navyše.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po prvom praní zostal tieň, mapa, pach, povlak alebo zmena tvaru, nesušte textil horúco. Najprv rozlíšte, či ide ešte o nečistotu alebo už o zmenu materiálu. Opakovaný mierny postup je bezpečnejší než jeden agresívny zásah.</p>
        <p>Pri hodnotných kusoch zastavte domáce experimentovanie skôr. Sako s výstužou, spoločenské šaty s ozdobami, svetlé čalúnenie alebo drahý obrus môžu mať vyššiu hodnotu než úspora na nesprávnom čistení.</p>
        <h2>Ako predísť poškodeniu pri sušení</h2>
        <p>Sušenie často rozhodne o výsledku. Flitre sa môžu zdeformovať, čalúnenie vytvoriť novú mapu, sako stratiť ramená, denim vyblednúť a balzamikový tieň sa môže teplom zafixovať. Sušičku, radiátor a žehličku používajte iba vtedy, keď to štítok povoľuje a keď je textil po praní skontrolovaný.</p>
        <p>Pri škvrnách najprv overte, že miesto je čisté. Pri tvarovaných kusoch najprv obnovte tvar za vlhka. Pri tmavých farbách skontrolujte povlak alebo šmuhy ešte pred dosušením.</p>
        <h2>Domáca rutina pre náročnejšie kusy</h2>
        <p>Ak sa podobné problémy opakujú, nastavte si jednoduchú rutinu: kontrola pred košom na bielizeň, oddelenie citlivých kusov, lokálne predčistenie, primeraná dávka pracieho gélu, nepreplnený bubon a kontrola pred sušením.</p>
        <p>Pri ozdobách chráňte pred trením, pri mapách od vody kontrolujte vlhkosť, pri saku chráňte konštrukciu, pri denime farbu a pri balzamikovom octe biely textil pred teplom.</p>
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 21 embellished/water/suit/denim/balsamic articles.")
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
                "wave": "retrofit-wave-21-embellished-water-suit-denim-balsamic-five",
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
                "wave": "retrofit-wave-21-embellished-water-suit-denim-balsamic-five",
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
