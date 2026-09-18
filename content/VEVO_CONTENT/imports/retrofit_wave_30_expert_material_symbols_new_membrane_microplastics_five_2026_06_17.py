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
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-30-expert-material-symbols-new-membrane-microplastics-five-2026-06-17.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-30-expert-material-symbols-new-membrane-microplastics-five-2026-06-17-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-22-2026-06-11-articles.json",
        "slug": "co-je-lan-preco-sa-krci-ako-ho-prat-a-ako-ho-zjemnit",
        "post_id": "2237",
        "url": "https://www.vevo.sk/n/co-je-lan-preco-sa-krci-ako-ho-prat-a-ako-ho-zjemnit",
        "topic": "linen",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-25-2026-06-16-articles.json",
        "slug": "symboly-prania-na-stitku-co-znamena-vanicka-trojuholnik-kruh-stvorec-a-zehlicka",
        "post_id": "2251",
        "url": "https://www.vevo.sk/n/symboly-prania-na-stitku-co-znamena-vanicka-trojuholnik-kruh-stvorec-a-zehlicka",
        "topic": "care_symbols",
        "live_update": False,
        "skip_reason": "Post 2251 was intentionally hidden after batch 25 because it duplicated the canonical laundry-symbol guide.",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-25-2026-06-16-articles.json",
        "slug": "ako-prat-nove-oblecenie-prvykrat-farby-chemicky-pach-zrazanie-a-stitok",
        "post_id": "2253",
        "url": "https://www.vevo.sk/n/ako-prat-nove-oblecenie-prvykrat-farby-chemicky-pach-zrazanie-a-stitok",
        "topic": "new_clothes",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-24-2026-06-16-articles.json",
        "slug": "co-je-membranove-oblecenie-vodny-stlpec-priedusnost-a-pranie-bez-poskodenia",
        "post_id": "2247",
        "url": "https://www.vevo.sk/n/co-je-membranove-oblecenie-vodny-stlpec-priedusnost-a-pranie-bez-poskodenia",
        "topic": "membrane",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-24-2026-06-16-articles.json",
        "slug": "mikroplasty-z-oblecenia-ako-prat-syntetiku-zodpovednejsie-bez-paniky",
        "post_id": "2249",
        "url": "https://www.vevo.sk/n/mikroplasty-z-oblecenia-ako-prat-syntetiku-zodpovednejsie-bez-paniky",
        "topic": "microplastics",
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
    items += '\n<li><a href="/n/casto-kladene-otazky-skvrny-na-obleceni-po-prani">Škvrny na oblečení po praní</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


TOPICS = {
    "linen": {
        "marker": "Detailnejší pohľad na ľan, krčivosť, zmäkčenie a prvé prania",
        "intro": "Ľan je pevné prírodné vlákno z ľanu siateho. V domácnosti pôsobí vzdušne a elegantne, ale pri nesprávnej rutine vie byť tvrdší, výrazne pokrčený alebo zrazený. Najviac rozhoduje prvých niekoľko praní, priestor v bubne, rýchle vybratie po praní a spôsob sušenia.",
        "scope": "ľanové šaty, košeľu, nohavice, obrus, utierky, ľanové obliečky, ľan vs bavlna, ľanové oblečenie prvé pranie, ako zjemniť ľan, ako žehliť ľan a prečo sa ľan krčí",
        "avoid": "preplnený bubon, prudké žmýkanie, dlhé státie mokrého ľanu v práčke, horúcu sušičku bez povolenia na štítku, agresívne drhnutie a očakávanie úplne hladkého vzhľadu ako pri syntetike",
        "diagnosis": [
            "<strong>Krčivosť je vlastnosť, nie automaticky chyba.</strong> Cieľom je mať čistý tvar a príjemný dotyk, nie bojovať proti materiálu za každú cenu.",
            "<strong>Prvé prania robte opatrnejšie.</strong> Nový ľan sa môže správať tuhšie a citlivejšie na teplo.",
            "<strong>Sušenie je polovica výsledku.</strong> Ľan vyberte včas, vyhlaďte rukami a sušte rozložený alebo zavesený do tvaru.",
            "<strong>Zmäkčenie prichádza rutinou.</strong> Ľan často príjemnie používaním, ale potrebuje dobrý oplach a priestor.",
        ],
        "state_rows": [
            ("ľanová košeľa", "prať s priestorom, vybrať hneď, sušiť na ramienku", "krčivosť a tvar goliera"),
            ("ľanové obliečky", "nepreplniť bubon, dosušiť bez vlhkých zlomov", "veľký objem a zatuchnutie"),
            ("ľanový obrus", "škvrny riešiť pred praním, sušiť vystretý", "mapy a záhyby"),
            ("ľanové utierky", "dobre opláchnuť a úplne vysušiť", "savosť a pach kuchyne"),
        ],
        "textile_rows": [
            ("ľan je tvrdý", "zvyšky pracieho produktu, presušenie alebo nová tkanina", "znížiť dávku, pridať oplach, sušiť menej agresívne"),
            ("ľan sa zrazil", "teplo alebo nevhodné sušenie", "ďalšie prania viesť šetrnejšie podľa štítku"),
            ("ľan je veľmi pokrčený", "preplnený bubon alebo dlhé státie mokrý", "vybrať hneď a tvarovať vlhký"),
            ("ľan zapácha", "pomalé sušenie v záhyboch", "sušiť vzdušne a neukladať vlhký"),
        ],
        "sections": [
            ("Ako prať ľanové oblečenie", "Ľanové oblečenie perte podľa štítku, ideálne v menšej dávke a s podobne ľahkými kusmi. Pri košeli, šatách alebo nohaviciach je dôležité, aby sa textil v bubne nelámal pod váhou uterákov alebo riflí. Zapnite gombíky podľa potreby, uvoľnite zrolované rukávy a po praní kus čo najskôr vyberte.", "Ak ľan vyberiete hneď, ešte vlhký sa dá vyhladiť rukami a vytvarovať. Tým znížite potrebu silného žehlenia a zároveň chránite farbu aj povrch tkaniny."),
            ("Ako prať ľanové obliečky", "Ľanové obliečky potrebujú v bubne viac miesta než tričko. Keď sa veľké kusy zrolujú, prací gél a voda sa nedostanú rovnomerne všade. Výsledkom môže byť tvrdší dotyk, slabší oplach alebo zatuchnuté rohy po pomalom sušení.", "Zapnite zipsy, perte s podobnými farbami a po praní obliečky rozložte tak, aby nevznikli vlhké hrubé záhyby. K posteľnej bielizni nadväzuje návod <a href=\"/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou\">ako správne prať obliečky</a>."),
            ("Ako zjemniť ľan bez prehnanej aviváže", "Ak je ľan tvrdý, najprv riešte zvyšky pracieho produktu, tvrdú vodu, presušenie a preplnený bubon. Pridanie väčšieho množstva aviváže nemusí byť najlepšia odpoveď, najmä pri utierkach a obliečkach, kde chcete zachovať savosť a vzdušnosť.", "Pomáha primeraná dávka gélu, dobrý oplach, voľné sušenie a pravidelné používanie. Nový ľan často zmäkne prirodzene po niekoľkých cykloch."),
            ("Ako žehliť ľan a kedy to nerobiť", "Ľan sa najlepšie upravuje, keď je mierne vlhký a keď to štítok povoľuje. Pri farebných kusoch žehlite z rubu alebo cez tenkú látku. Pri obrusoch a košeliach tvarujte švy, lemy a golier skôr, než textil úplne preschne.", "Nie každý ľan musí byť dokonale hladký. Pri ležérnych šatách alebo posteľnej bielizni môže byť prirodzená krčivosť súčasťou vzhľadu."),
            ("Ľan vs bavlna pri bežnom praní", "Bavlna býva univerzálnejšia a v domácnosti známejšia, ľan je vzdušnejší, pevný a výraznejšie sa krčí. Pri výbere medzi nimi sa nepýtajte iba na materiál, ale na použitie: tričko, obliečky, utierky, košeľa a obrus potrebujú odlišnú rutinu.", "Súvisiace porovnanie nájdete v článku <a href=\"/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost\">čo je bavlna</a>."),
        ],
        "expert_title": "Odbornejší pohľad: ľanové vlákno, pevnosť a krčivosť",
        "expert_p1": "Ľan patrí medzi rastlinné celulózové vlákna. Je cenený pre pevnosť, priedušnosť a prirodzenú textúru, ale má menšiu pružnosť než mnohé mäkšie úplety. Preto sa záhyby na ľane držia viditeľnejšie a starostlivosť sa musí sústrediť na tvarovanie po praní.",
        "expert_p2": "Alliance for European Flax-Linen and Hemp zdôrazňuje pôvod a sledovateľnosť ľanového vlákna v európskom kontexte. Prakticky to neznamená, že každý ľan sa perie rovnako. Rozhoduje väzba, farbenie, predzrážanie, zmes s bavlnou alebo viskózou a odporúčanie výrobcu na štítku.",
        "source_html": '<p>O pôvode a certifikácii ľanového vlákna pozri <a rel="noopener" href="https://allianceflaxlinenhemp.eu/en" target="_blank">Alliance for European Flax-Linen and Hemp</a>; materiálový základ dopĺňa <a rel="noopener" href="https://www.britannica.com/topic/linen" target="_blank">Britannica: Linen</a>.</p>',
        "test_title": "Malý test pred ďalším praním ľanu",
        "test_text": "Pred ďalším praním si poznačte, či bol ľan tvrdý, pokrčený, zrazený alebo zatuchnutý. Potom meňte iba jednu vec: menšiu náplň, nižšie otáčky, lepší oplach alebo iné sušenie. Tak zistíte, čo pomohlo, a nezačnete zbytočne meniť celú rutinu.",
        "checklist": "Pred praním skontrolujte štítok, farbu, hrúbku tkaniny, zipsy, škvrny, veľkosť dávky, otáčky, dávku gélu, možnosť voľného sušenia a to, či textil musí byť po vysušení žehlený.",
        "rule": "Ľan perte s priestorom, vyberajte ho z práčky rýchlo a sušte ho vytvarovaný. Najviac mu škodí kombinácia preplneného bubna, mokrého státia a horúceho sušenia.",
        "recommendation_intro": "Pri ľane je dôležitý jemný, dobre vypláchnutý prací cyklus a sušenie v tvare. Produkt má podporiť čistotu, nie prekryť zvyšky v tkanine.",
        "product_text": "Vhodný ako šetrný základ na bežné pranie ľanových a bavlnených textílií, pokiaľ to povoľuje štítok výrobcu.",
        "category_text": "Pri prírodných materiáloch vyberajte prací gél podľa farby, citlivosti tkaniny, potreby dobrého oplachu a spôsobu sušenia.",
        "links": [
            ("/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost", "Čo je bavlna: vlastnosti a starostlivosť"),
            ("/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou", "Ako správne prať obliečky"),
            ("/n/preco-uteraky-zapachaju-aj-po-prani-zatuchnuty-pach-tvrdost-a-strata-savosti", "Prečo uteráky zapáchajú aj po praní"),
        ],
        "faq": [
            ("Prečo sa ľan tak krčí?", "Pretože ľanové vlákno a tkanina majú prirodzene menšiu pružnosť. Pomáha menšia dávka, rýchle vybratie a sušenie v tvare."),
            ("Ako zjemniť tvrdý ľan?", "Najprv upravte dávkovanie, oplach a sušenie. Nový ľan často mäkne používaním, ale zvyšky pracieho produktu ho môžu robiť tvrdším."),
            ("Môžem ľan sušiť v sušičke?", "Iba ak to povoľuje štítok. Pri neistote je bezpečnejšie voľné sušenie a tvarovanie ešte vlhkého textilu."),
        ],
    },
    "care_symbols": {
        "marker": "Detailnejší pohľad na symboly prania ako rozhodovací systém",
        "intro": "Symboly na štítku nie sú dekorácia ani všeobecné odporúčanie. Sú to hranice bezpečnej starostlivosti pre hotový výrobok: látku, farbu, potlač, zipsy, výstuž, membránu aj povrchovú úpravu. Preto sa oplatí čítať ich spolu, nie izolovane.",
        "scope": "vaničku na pranie, trojuholník na bielenie, štvorec na sušenie, kruh na profesionálne čistenie, žehličku, bodky, podčiarknutia, preškrtnuté symboly a situáciu, keď štítok na oblečení chýba",
        "avoid": "prať iba podľa farby, ignorovať symbol sušenia, zamieňať maximálnu teplotu za povinnú teplotu, používať bielenie bez symbolu, sušiť v sušičke bez povolenia a žehliť potlač priamo z líca",
        "diagnosis": [
            "<strong>Vanička rieši pranie.</strong> Číslo alebo bodky hovoria o teplote, čiary pod symbolom o šetrnosti procesu.",
            "<strong>Trojuholník rieši bielenie.</strong> Preškrtnutý trojuholník znamená, že bielenie nie je vhodné.",
            "<strong>Štvorec rieši sušenie.</strong> Kruh v štvorci je sušička, čiary často naznačujú prirodzené sušenie.",
            "<strong>Žehlička a kruh sú samostatné rozhodnutia.</strong> Žehlenie a profesionálne čistenie neriešte až po poškodení.",
        ],
        "state_rows": [
            ("vanička", "pranie, teplota a mechanika", "začnite tu pri každej dávke"),
            ("trojuholník", "bielenie", "dôležité pri bielych aj farebných veciach"),
            ("štvorec", "sušenie", "často rozhoduje o zrážaní"),
            ("žehlička", "teplota žehlenia", "pozor na potlač a syntetiku"),
            ("kruh", "profesionálne čistenie", "pri saku, kabáte a citlivých kusoch"),
        ],
        "textile_rows": [
            ("jedna čiara pod vaničkou", "šetrnejší proces", "nižšia mechanika alebo vhodný program"),
            ("dve čiary pod vaničkou", "veľmi šetrný proces", "jemná bielizeň, vlna alebo citlivý výrobok"),
            ("preškrtnutý symbol", "daný postup nepoužiť", "nehľadať domáce obchádzky bez testu"),
            ("bodky", "teplotná úroveň", "pri praní, sušičke alebo žehlení podľa symbolu"),
        ],
        "sections": [
            ("Ako čítať symboly prania v poradí", "Najprv pozrite materiálové zloženie, potom symbol prania, sušenia, bielenia, žehlenia a profesionálneho čistenia. Až potom vyberte program. Ak je oblečenie zmesové, riaďte sa najcitlivejšou časťou výrobku, nie iba hlavným vláknom.", "Príklad: bavlnené tričko s elastanom a potlačou nie je obyčajná biela bavlnená utierka. Potlač a elastan môžu byť limitom, ktorý zmení celý postup."),
            ("Čo znamená vanička na štítku", "Vanička hovorí, či je domáce pranie povolené, pri akej teplote a s akou šetrnosťou. Číslo vo vaničke je maximálna hranica, nie povinnosť prať vždy na tejto teplote. Pri menej znečistenom oblečení môže byť nižšia teplota rozumnejšia.", "Ak je pod vaničkou čiara, myslite na šetrnejšiu mechaniku. Pri dvoch čiarach už treba byť výrazne opatrnejší."),
            ("Trojuholník, štvorec a sušička", "Trojuholník je bielenie. Štvorec rieši sušenie a ak je v ňom kruh, ide o sušičku. Veľa poškodení nevzniká pri praní, ale až pri horúcom sušení, keď sa textil zrazí, zdeformuje alebo stratí pružnosť.", "Pri neistote sušte voľne a až po kontrole výsledku. Sušička je pohodlná, ale nie je univerzálne bezpečná."),
            ("Žehlička a potlač", "Symbol žehličky nastavuje teplotu, no pri potlači, membráne, flitroch alebo pogumovaných detailoch treba rozmýšľať aj nad povrchom. Niektoré časti výrobku sa nesmú žehliť priamo, aj keď samotná látka by nižšiu teplotu zniesla.", "Žehlite z rubu, cez látku alebo vôbec, ak je to na štítku zakázané."),
            ("Keď štítok chýba alebo je nečitateľný", "Ak štítok chýba, zvoľte opatrný postup: nižšia teplota, menšia mechanika, podobné farby, žiadne bielenie, žiadna sušička bez istoty. Pri drahom saku, kabáte alebo citlivom materiáli je lepšia čistiareň než experiment.", "Kanonický prehľad symbolov máme v návode <a href=\"/n/symboly-prania-kompletny-sprievodca-praciim-stitkom\">symboly prania: kompletný sprievodca štítkom</a>. Tento článok rozširuje najmä praktické rozhodovanie pri bežnej domácnosti."),
        ],
        "expert_title": "Odbornejší pohľad: symbol platí pre celý hotový výrobok",
        "expert_p1": "Care label nie je hodnotenie jedného vlákna. Vzťahuje sa na celý výrobok vrátane farbiva, švov, výstuže, gombíkov, zipsov, podšívky, potlače a povrchových úprav. Preto môže mať oblečenie zo zdanlivo odolného materiálu šetrnejší symbol.",
        "expert_p2": "GINETEX definuje skupiny symbolov pre pranie, bielenie, sušenie, žehlenie a profesionálne čistenie. FTC v pravidlách pre označovanie zdôrazňuje, že výrobcovia majú uvádzať návod na pravidelnú starostlivosť. Pre zákazníka je pointa jednoduchá: symboly chránia konkrétny kus, nie iba kategóriu textilu.",
        "source_html": '<p>Pre oficiálny systém symbolov pozri <a rel="noopener" href="https://www.ginetex.net/gb/labelling/care-symbols.asp" target="_blank">GINETEX: Care symbols</a>; k pravidlám označovania pozri <a rel="noopener" href="https://www.ftc.gov/business-guidance/resources/clothes-captioning-complying-care-labeling-rule" target="_blank">FTC: Complying with the Care Labeling Rule</a>.</p>',
        "test_title": "Malý test pred praním neznámeho kusu",
        "test_text": "Ak neviete, čo symbol znamená alebo štítok chýba, neperte kus s plnou bežnou dávkou. Najprv skontrolujte farbu na skrytom mieste, otočte oblečenie naruby, použite jemnejší program a nesušte horúco, kým nevidíte výsledok po praní.",
        "checklist": "Pred praním skontrolujte materiál, symbol vaničky, bielenie, sušenie, žehlenie, profesionálne čistenie, potlač, zipsy, elastan, farbu, mieru znečistenia a to, či je štítok stále čitateľný.",
        "rule": "Symboly čítajte ako sériu rozhodnutí: materiál, pranie, bielenie, sušenie, žehlenie a profesionálne čistenie. Najcitlivejšia časť výrobku určuje bezpečný postup.",
        "recommendation_intro": "Pri praní podľa štítku pomáha šetrný prací produkt a dobrý oplach. Dôležité je neprekročiť symboly výrobcu.",
        "product_text": "Vhodný na bežné pranie mnohých textílií, keď štítok povoľuje domáce pranie a chcete postupovať šetrne.",
        "category_text": "V kategórii pracích gélov vyberajte podľa materiálu, farby, citlivosti pokožky a odporúčaní na štítku.",
        "links": [
            ("/n/symboly-prania-kompletny-sprievodca-praciim-stitkom", "Symboly prania: kompletný sprievodca štítkom"),
            ("/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program", "Ako čítať štítok na oblečení"),
            ("/n/otacky-pri-odstredovani-ako-ovplyvnuju-vlhkost-krcenie-a-opotrebovanie-oblecenia", "Otáčky pri odstreďovaní"),
        ],
        "faq": [
            ("Je číslo vo vaničke odporúčaná teplota?", "Je to bezpečný limit podľa štítku. Pri menej znečistenom textile môžete často prať šetrnejšie, ak to dáva zmysel."),
            ("Čo znamená preškrtnutý trojuholník?", "Nepoužívať bielenie. Platí to aj vtedy, keď je textil biely alebo svetlý."),
            ("Môžem ignorovať symbol sušičky?", "Nie. Práve sušenie býva častá príčina zrážania, deformácie alebo poškodenia povrchových úprav."),
        ],
    },
    "new_clothes": {
        "marker": "Detailnejší pohľad na prvé pranie nového oblečenia",
        "intro": "Prvé pranie nového oblečenia má tri ciele: odstrániť zvyšky z výroby, skladu a skúšania, znížiť riziko púšťania farby a nastaviť bezpečnú rutinu pre ďalšie prania. Nemá to byť agresívny zásah, ktorý hneď pri prvom cykle poškodí tvar alebo potlač.",
        "scope": "nové tričko, nové rifle, červené oblečenie, čierne oblečenie, detské oblečenie, spodnú bielizeň, chemický pach, zrážanie, púšťanie farby, prvé pranie obliečok a prvé pranie uterákov",
        "avoid": "hodiť nový tmavý kus k bielej bielizni, prekryť chemický pach vôňou, prať horúco bez štítku, sušiť v sušičke bez povolenia, ignorovať potlač a prvýkrát preplniť bubon",
        "diagnosis": [
            "<strong>Nový kus nemusí byť čistý.</strong> Prešiel výrobou, balením, skladom, dopravou a často aj skúšaním.",
            "<strong>Rizikové farby perte oddelene.</strong> Tmavý denim, červená, sýta modrá a čierna vedia pri prvých cykloch púšťať farbu.",
            "<strong>Chemický pach riešte praním a vetraním.</strong> Silná vôňa nie je náhrada odstránenia zvyškov.",
            "<strong>Pri detskom oblečení myslite na oplach.</strong> Menej produktu a lepšie vypláchnutie je často dôležitejšie než intenzívna parfumácia.",
        ],
        "state_rows": [
            ("nové rifle", "prať naruby a s tmavými farbami", "riziko púšťania farby"),
            ("detské body", "jemný prací produkt a dobrý oplach", "kontakt s pokožkou"),
            ("nové obliečky", "vyprať pred použitím a dosušiť", "prach, balenie a dotyk s pokožkou"),
            ("chemický pach", "vetrať, prať, sušiť; neprekrývať", "zvyšky úprav alebo balenia"),
        ],
        "textile_rows": [
            ("sýta farba", "prvé prania oddeliť", "ochrana svetlej bielizne"),
            ("potlač", "prať naruby", "menšie trenie"),
            ("elastan", "nižšia teplota a bez horúceho sušenia", "ochrana pružnosti"),
            ("spodná bielizeň", "vyprať pred nosením", "hygiena a pohodlie"),
        ],
        "sections": [
            ("Treba prať nové oblečenie pred nosením?", "Pri oblečení priamo na pokožku je prvé pranie rozumné. Týka sa to spodnej bielizne, tričiek, detských vecí, pyžama, obliečok a uterákov. Pri kabáte alebo saku, ktoré sa priamo nedotýka pokožky a má iba profesionálne čistenie, postupujte podľa štítku.", "Zmyslom nie je vyvolávať paniku, ale odstrániť bežné zvyšky a prach z cesty textilu k zákazníkovi."),
            ("Ako prať nové rifle prvýkrát", "Nové rifle perte naruby, samostatne alebo s podobnými tmavými kusmi. Nepoužívajte horúci program len preto, že ide o pevný materiál. Denim môže púšťať farbu a zároveň sa pri teple alebo sušičke správať inak, než čakáte.", "Súvisiaci detail je v článku <a href=\"/n/ako-prat-riflovu-bundu-a-tmave-dzinsy-aby-nepustali-farbu\">ako prať rifľovú bundu a tmavé džínsy</a>."),
            ("Ako prať nové detské oblečenie", "Detské oblečenie perte pred prvým nosením šetrne, s primeranou dávkou produktu a dobrým oplachom. Pri bábätkách a citlivej pokožke je menej často viac: cieľom je čistý textil bez zvyškov pracieho prostriedku.", "Nové detské veci nedávajte do rovnakej dávky s veľmi znečistenými uterákmi, kuchynskými utierkami alebo pracovným oblečením."),
            ("Chemický pach z nového oblečenia", "Ak nový kus výrazne páchne, najprv ho vyvetrajte a vyperte podľa štítku. Ak pach zostane, nesušte ho horúco a neukladajte do skrine. Zopakujte šetrný postup alebo zvážte reklamáciu, ak je pach extrémny a pretrváva.", "Parfum do prania použite až na čistý textil. Jeho úlohou je príjemný dojem, nie prekrytie problému."),
            ("Ako zabrániť zrážaniu pri prvom praní", "Pri prvom praní neprekračujte štítok, nepreplňte bubon a sušičku používajte iba vtedy, keď je povolená. Nový úplet, viskóza, elastan alebo bavlna s potlačou môžu reagovať citlivejšie než staršie, už oprané kusy.", "K téme nadväzuje článok <a href=\"/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia\">prečo sa oblečenie zrazí po praní</a>."),
        ],
        "expert_title": "Odbornejší pohľad: štítok, zvyšky úprav a farbivá",
        "expert_p1": "Nové oblečenie môže niesť zvyšky farbív, povrchových úprav, prachu zo skladu alebo bežnej manipulácie. Zároveň môže byť farba pri prvých praniach menej stabilná. Preto je prvý cyklus skôr kontrolovaný test než bežná zmiešaná dávka.",
        "expert_p2": "FTC pravidlá starostlivosti o textil zdôrazňujú význam pokynov na štítku. Odborné medicínske a toxikologické materiály k formaldehydu v textile zároveň upozorňujú, že pranie môže znižovať niektoré zvyšky, hoci nie je univerzálnym riešením pre každý citlivý prípad. Pri podráždení pokožky treba prestať textil nosiť a riešiť konkrétnu reakciu.",
        "source_html": '<p>K významu štítku pozri <a rel="noopener" href="https://www.ftc.gov/business-guidance/resources/clothes-captioning-complying-care-labeling-rule" target="_blank">FTC: Care Labeling Rule</a>; k formaldehydu v textile pozri správu <a rel="noopener" href="https://www.gao.gov/assets/gao-10-875.pdf" target="_blank">U.S. GAO: Formaldehyde in Textiles</a>.</p>',
        "test_title": "Malý test farby pred prvým praním",
        "test_text": "Pri sýtom alebo tmavom kuse navlhčite bielu handričku a jemne prejdite nenápadné miesto. Ak sa farba prenáša, prvé pranie urobte samostatne alebo iba s podobnými tmavými farbami. Test nenahrádza štítok, ale pomôže odhaliť riziko ešte pred plným bubnom.",
        "checklist": "Pred prvým praním skontrolujte štítok, farbu, potlač, zipsy, gombíky, elastan, podšívku, chemický pach, kontakt s pokožkou, riziko púšťania farby, veľkosť dávky a spôsob sušenia.",
        "rule": "Nové oblečenie prvýkrát perte šetrne, oddelene pri rizikových farbách a vždy podľa štítku. Prvé pranie je kontrola, nie test odolnosti.",
        "recommendation_intro": "Pri prvom praní nového oblečenia pomáha jemný prací základ, primerané dávkovanie a dobrý oplach.",
        "product_text": "Vhodný na šetrné prvé pranie mnohých bežných textílií, keď chcete odstrániť zvyšky z manipulácie bez prehnanej záťaže.",
        "category_text": "Pri novom oblečení vyberajte prací gél podľa farby, materiálu, kontaktu s pokožkou a potreby dôkladného oplachu.",
        "links": [
            ("/n/ako-zabranit-pustaniu-farby-pri-prani-noveho-oblecenia", "Ako zabrániť púšťaniu farby pri praní nového oblečenia"),
            ("/n/pustila-farba-v-pracke-co-urobit-s-bielym-trickom-a-ruzovou-bieliznou", "Pustila farba v práčke"),
            ("/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia", "Prečo sa oblečenie zrazí po praní"),
        ],
        "faq": [
            ("Treba prať každé nové oblečenie?", "Pri kusoch priamo na pokožku je to rozumné. Pri kabátoch a veciach s profesionálnym čistením rešpektujte štítok."),
            ("Ako vyprať nové čierne tričko?", "Naruby, s tmavými farbami alebo samostatne, pri teplote podľa štítku a bez horúceho sušenia."),
            ("Čo ak nový kus stále páchne?", "Vetrajte, vyperte podľa štítku a neskladujte vlhký. Ak pach pretrváva extrémne, zvážte reklamáciu alebo kus nenoste pri citlivej pokožke."),
        ],
    },
    "membrane": {
        "marker": "Detailnejší pohľad na membránové oblečenie, vodný stĺpec a pranie",
        "intro": "Membránové oblečenie funguje ako systém: vrchná látka, membrána, podlepené švy, zipsy, kapucňa a vodoodpudivá úprava. Keď sa zanedbá pranie, pot, mastnota a prach môžu zhoršiť priedušnosť aj odperľovanie vody. Keď sa perie nesprávne, môže utrpieť rovnaká funkcia.",
        "scope": "membránovú bundu, nohavice, softshell s membránou, turistickú bundu, lyžiarsku bundu, vodný stĺpec, priedušnosť, DWR, impregnáciu, pranie bez aviváže a obnovu odperľovania vody",
        "avoid": "aviváž, bielidlo, prášok so zvyškami, horúce žehlenie membrány, čistenie tvrdou kefou, skladovanie vlhkej bundy, ignorovanie potreby obnoviť vodoodpudivú úpravu a sušenie proti štítku",
        "diagnosis": [
            "<strong>Voda sa neodperľuje?</strong> Nemusí byť zničená membrána; často je zanesená alebo oslabená vonkajšia úprava.",
            "<strong>Bunda zvnútra vlhne?</strong> Môže ísť o pot, slabú priedušnosť pri záťaži alebo premočenú vrchnú látku.",
            "<strong>Aviváž vynechajte.</strong> Film z aviváže môže zhoršiť funkčný povrch.",
            "<strong>Pranie nie je nepriateľ.</strong> Pri správnom postupe pomáha odstrániť pot, soľ a nečistoty.",
        ],
        "state_rows": [
            ("turistická bunda", "prať podľa štítku, bez aviváže, dobrý oplach", "pot a prach znižujú komfort"),
            ("lyžiarske nohavice", "zapnúť zipsy, vyčistiť lemy, nepreplniť bubon", "soľ, sneh a oder"),
            ("softshell s membránou", "mierny program a kontrola impregnácie", "kombinácia pružnosti a ochrany"),
            ("dažďová bunda", "po praní obnoviť odperľovanie podľa potreby", "vrchná látka nesmie nasiaknuť"),
        ],
        "textile_rows": [
            ("voda tvorí kvapky", "úprava ešte funguje", "stačí bežná kontrola"),
            ("vrchná látka nasiakne", "DWR môže byť oslabená", "vyprať a obnoviť impregnáciu podľa výrobcu"),
            ("bunda zapácha", "pot a mastnota v límci", "prať včas, nevetrať donekonečna"),
            ("švy presakujú", "riziko poškodenia alebo opotrebovania", "zvážiť servis alebo reklamáciu"),
        ],
        "sections": [
            ("Ako prať membránovú bundu", "Zapnite zipsy, uvoľnite vrecká, skontrolujte štítok a perte menšiu dávku bez aviváže. Prací produkt musí byť dobre vypláchnutý, preto sa pri membránach oplatí pridať dôkladný oplach, ak to práčka umožňuje.", "Nepoužívajte náhodný agresívny program. Cieľom je odstrániť pot, prach a mastnotu bez filmu na povrchu."),
            ("Vodný stĺpec a priedušnosť v bežnom jazyku", "Vodný stĺpec hovorí o odolnosti proti tlaku vody, priedušnosť o schopnosti púšťať vlhkosť smerom von. V praxi však rozhoduje aj strih, vetranie, intenzita pohybu, vrstvenie a stav vrchnej tkaniny.", "Ani kvalitná membrána nezaručí sucho, ak sa pod bundou prehrejete alebo vrchná látka nasaje vodu a prestane dýchať."),
            ("Kedy obnoviť impregnáciu", "Ak voda prestane vytvárať kvapky a vonkajšia látka rýchlo tmavne nasiaknutím, membránový kus môže potrebovať obnovu vodoodpudivej úpravy. Najprv však býva vhodné pranie, pretože špina a mastnota vedia odperľovanie zhoršiť.", "Impregnáciu vyberajte podľa typu textilu a pokynov výrobcu. Pri nejasnom štítku nerobte univerzálny pokus na drahej bunde."),
            ("Membrána, soľ a pot", "Pot, telesná mastnota, opaľovací krém, soľ zo snehu a prach sa najčastejšie držia pri golieri, manžetách, lemoch a na ramenách pod batohom. Ak sa tieto miesta neriešia, bunda môže zapáchať a horšie odvádzať vlhkosť.", "K zimnej soli nadväzuje návod <a href=\"/n/ako-odstranit-sol-a-mokry-sneh-z-lyziarskych-rukavic-s-membranou\">ako odstrániť soľ a mokrý sneh z lyžiarskych rukavíc s membránou</a>."),
            ("Ako sušiť membránové oblečenie", "Sušenie robte podľa štítku. Niektoré úpravy potrebujú mierne teplo na aktiváciu, iné kusy vyžadujú voľné sušenie. Bez pokynu výrobcu nepoužívajte vysokú teplotu ani priame žehlenie membrány.", "Pred uložením musí byť bunda úplne suchá. Vlhké skladovanie zhoršuje pach aj životnosť detailov."),
        ],
        "expert_title": "Odbornejší pohľad: membrána nie je len jedna vrstva",
        "expert_p1": "Membránové oblečenie je laminát alebo konštrukcia, kde spolupracuje vonkajšia textília, membrána a úpravy povrchu. Keď vonkajšia látka nasiakne, človek môže mať pocit, že membrána prestala fungovať, hoci problém je v zanesení alebo oslabenej vodoodpudivej úprave.",
        "expert_p2": "GORE-TEX odporúča prať oblečenie, keď je špinavé, zapácha alebo keď voda prestáva tvoriť kvapky na povrchu. REI pri starostlivosti o nepremokavé oblečenie zdôrazňuje malé množstvo tekutého prostriedku, vyhnutie sa aviváži a bielidlu a dôkladný oplach.",
        "source_html": '<p>Praktické pokyny uvádza <a rel="noopener" href="https://www.gore-tex.com/en_uk/blog/wash-your-gore-tex-jacket-regularly" target="_blank">GORE-TEX: Wash your jacket regularly</a> a <a rel="noopener" href="https://www.rei.com/learn/expert-advice/washing-goretex-outerwear.html" target="_blank">REI: How to care for Gore-Tex rainwear</a>.</p>',
        "test_title": "Malý test odperľovania vody",
        "test_text": "Na čistú suchú časť bundy kvapnite trochu vody. Ak sa drží v kvapkách, vrchná úprava ešte funguje. Ak sa látka rýchlo zafarbí do tmava a voda sa vpíja, najprv zvážte pranie podľa štítku a potom obnovu impregnácie podľa výrobcu.",
        "checklist": "Pred praním skontrolujte štítok, zipsy, vrecká, suché zipsy, kapucňu, lemy, golier, škvrny od soli, zápach potu, zákaz aviváže, možnosť extra oplachu, sušenie a potrebu obnoviť odperľovanie.",
        "rule": "Membránové oblečenie perte včas a šetrne, bez aviváže, s dobrým oplachom. Nečistoty aj nesprávne prípravky môžu zhoršiť funkciu.",
        "recommendation_intro": "Pri membránach je najdôležitejšie rešpektovať štítok a nepoužívať aviváž. Bežný prací gél používajte iba vtedy, keď ho výrobca textilu povoľuje.",
        "product_text": "Vhodný na bežné pranie mnohých textílií. Pri membránovej bunde ho použite len vtedy, keď štítok nevyžaduje špeciálny technický prostriedok.",
        "category_text": "Pri funkčných materiáloch vyberajte prací produkt podľa štítku, typu membrány, miery potu a potreby dôkladného oplachu.",
        "links": [
            ("/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany", "Ako prať softshellovú bundu bez poškodenia membrány"),
            ("/n/ako-prat-prsiplast-a-reflexne-nepremokave-nohavice-po-dazdi", "Ako prať pršiplášť a nepremokavé nohavice"),
            ("/n/ako-odstranit-sol-a-mokry-sneh-z-lyziarskych-rukavic-s-membranou", "Ako odstrániť soľ z rukavíc s membránou"),
        ],
        "faq": [
            ("Môžem prať membránovú bundu v práčke?", "Áno, ak to povoľuje štítok. Použite šetrný postup, bez aviváže a s dobrým oplachom."),
            ("Prečo bunda po daždi nasiakne?", "Často je oslabená alebo zanesená vonkajšia vodoodpudivá úprava. Nemusí to hneď znamenať zničenú membránu."),
            ("Treba membránu impregnovať po každom praní?", "Nie vždy. Sledujte odperľovanie vody a odporúčania výrobcu konkrétneho kusu."),
        ],
    },
    "microplastics": {
        "marker": "Detailnejší pohľad na mikrovlákna zo syntetiky bez paniky",
        "intro": "Syntetické textílie môžu pri nosení a praní uvoľňovať drobné vlákna. Praktický cieľ nie je prestať prať ani robiť z každej mikiny problém. Rozumnejšie je znižovať zbytočné trenie, prať plnšie ale nie preplnené dávky, predlžovať životnosť oblečenia a kupovať syntetiku tam, kde dáva funkčný zmysel.",
        "scope": "polyester, fleece, športové oblečenie, legíny, softshell, mikrovlákna, pranie syntetiky, mikroplasty z textilu, filter do práčky, vrecká na pranie, žmolkovanie a zodpovednejšiu starostlivosť bez strašenia",
        "avoid": "paniku, zbytočne agresívne programy, pranie jedného kusu v prázdnom bubne, časté horúce sušenie syntetiky, vyhadzovanie funkčného oblečenia iba pre zloženie a lacné kúsky s krátkou životnosťou",
        "diagnosis": [
            "<strong>Najviac pomáha menej zbytočného trenia.</strong> Prať naruby, oddeliť drsné kusy a nepreplniť bubon.",
            "<strong>Fleece a mäkká syntetika sú citlivejšie na oder.</strong> Povrch sa môže opotrebovať rýchlejšie než hladká tkanina.",
            "<strong>Životnosť oblečenia je súčasť riešenia.</strong> Kus, ktorý vydrží roky, je praktickejší než rýchlo opotrebovaná náhrada.",
            "<strong>Pranie má byť účinné, nie extrémne.</strong> Silnejší program automaticky neznamená lepší ekologický výsledok.",
        ],
        "state_rows": [
            ("fleece mikina", "prať naruby, mimo suchých zipsov a uterákov", "nižšie trenie povrchu"),
            ("športové legíny", "prať s hladkými syntetickými kusmi", "ochrana pružnosti a povrchu"),
            ("softshell", "bez aviváže a podľa štítku", "funkčná úprava"),
            ("mikrovláknové handričky", "prať oddelene od oblečenia", "zvyšky špiny a čistiacich látok"),
        ],
        "textile_rows": [
            ("malá dávka", "veľa pohybu a trenia", "spojiť podobné kusy, ale nepreplniť"),
            ("preplnený bubon", "slabý oplach a trenie pod tlakom", "zmenšiť dávku"),
            ("drsné kusy v dávke", "oder a žmolkovanie", "oddeliť zipsy, suchý zips a uteráky"),
            ("horúce sušenie", "rýchlejšie opotrebovanie", "sušiť podľa štítku a nepresušovať"),
        ],
        "sections": [
            ("Ako prať syntetiku zodpovednejšie", "Syntetické oblečenie perte naruby, s podobne hladkými kusmi a bez zbytočne agresívnej mechaniky. Bubon nemá byť prázdny, ale ani natlačený. Pri športe riešte pot včas, aby ste nemuseli voliť tvrdší program a viac chémie.", "Najlepšia rutina je tá, ktorá odstráni pot a nečistoty, ale zbytočne nedrhne povrch textilu."),
            ("Fleece, polyester a mikrovlákna", "Fleece je typický príklad mäkkej syntetiky s česaným povrchom. Je príjemný a hrejivý, ale povrch je citlivejší na trenie. Preto ho neperte s uterákmi, rifľami, suchým zipsom alebo ťažkými bundami.", "Detail k materiálu nájdete v článku <a href=\"/n/co-je-fleece-hrejivost-zmolkovanie-a-starostlivost-pri-prani\">čo je fleece</a>."),
            ("Pomáha pracie vrecko alebo filter?", "Pracie vrecká a filtre môžu pomôcť zachytávať časť uvoľnených vlákien, ale nenahrádzajú dobré triedenie, nižšie trenie a rozumnú životnosť oblečenia. Ak ich používate, čistite ich podľa návodu a zachytené vlákna nevyplachujte späť do odtoku.", "Prakticky má zmysel kombinovať viac malých opatrení, nie spoliehať sa na jednu pomôcku."),
            ("Ako znížiť potrebu častého prania", "Nie každý syntetický kus treba prať po krátkom nosení. Ak nie je spotený ani špinavý, pomôže vetranie. Naopak, športové oblečenie po intenzívnom tréningu nenechávajte vlhké v taške, lebo pach sa potom rieši ťažšie.", "Cieľom je prať vtedy, keď to textil potrebuje, a prať dobre."),
            ("Nákup a životnosť syntetiky", "Syntetika má zmysel pri športe, membránach, rýchloschnúcich vrstvách alebo fleeci. Pri kúpe sledujte kvalitu švov, hustotu materiálu, účel a to, či kus reálne využijete. Najhoršia voľba je lacné oblečenie, ktoré rýchlo žmolkuje, zapácha alebo sa musí skoro nahradiť.", "K praniu polyesteru nadväzuje článok <a href=\"/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal\">čo je polyester a ako ho prať</a>."),
        ],
        "expert_title": "Odbornejší pohľad: textílie ako zdroj mikroplastov",
        "expert_p1": "Európska environmentálna agentúra uvádza nosenie a pranie syntetických textílií ako jeden zo zdrojov mikroplastov v prostredí. Neznamená to, že každé pranie je rovnaké. Uvoľňovanie vlákien ovplyvňuje materiál, vek textilu, mechanické namáhanie, konštrukcia látky aj životný cyklus oblečenia.",
        "expert_p2": "Praktické opatrenia preto dávajú zmysel najmä tam, kde znižujú opotrebovanie: menej trenia, triedenie, šetrnejšie sušenie, oprava a dlhšie používanie. Zodpovedná rutina nemá byť nepraktická. Má znížiť zbytočné uvoľňovanie vlákien a zároveň udržať oblečenie čisté.",
        "source_html": '<p>K téme pozri <a rel="noopener" href="https://www.eea.europa.eu/en/analysis/publications/microplastics-from-textiles-towards-a-circular-economy-for-textiles-in-europe" target="_blank">EEA: Microplastics from textiles</a> a novší indikátor <a rel="noopener" href="https://www.eea.europa.eu/en/circularity/sectoral-modules/textiles/microplastic-from-synthetic-textiles-unintentionally-released-into-the-environment-in-the-eu" target="_blank">EEA: Microplastic from synthetic textiles in the EU</a>.</p>',
        "test_title": "Malý test opotrebovania syntetiky",
        "test_text": "Po praní skontrolujte, či pribudli žmolky, chlpy, zmatnený povrch alebo tvrdší dotyk. Ak áno, najprv upravte triedenie: perte naruby, odstráňte suchý zips a uteráky z dávky, znížte teplo pri sušení a nepreplňte bubon.",
        "checklist": "Pred praním syntetiky skontrolujte štítok, typ materiálu, pot, zápach, žmolky, zipsy, suchý zips, drsné textílie v dávke, veľkosť bubna, teplotu, odstreďovanie a sušenie.",
        "rule": "Syntetiku perte tak, aby sa očistila, ale zbytočne nedrhla: naruby, s podobnými kusmi, bez preplnenia a bez horúceho sušenia mimo štítku.",
        "recommendation_intro": "Pri syntetike je dôležité odstrániť pot a zvyšky bez nadmerného trenia. Produkt používajte primerane a vždy s dobrým oplachom.",
        "product_text": "Vhodný na bežné pranie mnohých syntetických a zmesových textílií, ak to povoľuje štítok a nejde o špeciálnu membránu.",
        "category_text": "Pri syntetike vyberajte prací gél podľa zápachu, funkčnej úpravy, farby a potreby dôkladného oplachu.",
        "links": [
            ("/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal", "Čo je polyester a ako ho prať"),
            ("/n/co-je-fleece-hrejivost-zmolkovanie-a-starostlivost-pri-prani", "Čo je fleece"),
            ("/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie", "Prečo sa oblečenie žmolkuje"),
        ],
        "faq": [
            ("Mám prestať prať syntetiku?", "Nie. Špinavé alebo spotené oblečenie treba prať. Zmysel má znížiť zbytočné trenie a predĺžiť životnosť kusov."),
            ("Je fleece väčší problém než hladký polyester?", "Mäkký česaný povrch fleecu je náchylnejší na oder, preto potrebuje šetrnejšie triedenie a menej trenia."),
            ("Pomôže pranie naruby?", "Áno, znižuje priame trenie viditeľného povrchu a často pomáha aj proti žmolkovaniu."),
        ],
    },
}


def build_expansion(topic):
    config = TOPICS[topic]
    section_html = "\n".join(f"<h2>{title}</h2>\n<p>{p1}</p>\n<p>{p2}</p>" for title, p1, p2 in config["sections"])
    faq_html = "\n".join(f"<h3>{question}</h3>\n<p>{answer}</p>" for question, answer in config["faq"])
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["intro"]}</p>
        <p>V praxi sa oplatí myslieť na celý kontext: {config["scope"]}. Práve tieto situácie rozhodujú o tom, či bude výsledok po praní čistý, príjemný na dotyk a bez zbytočného poškodenia.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu textilu</h2>
        {table(["Stav alebo kus", "Čo urobiť", "Prečo je to dôležité"], config["state_rows"])}
        <h2>Najčastejšie riziká pri praní</h2>
        {table(["Riziko", "Typická príčina", "Lepší postup"], config["textile_rows"])}
        <h2>Čomu sa pri tejto téme vyhnúť</h2>
        <p>Najčastejšia chyba je použiť jeden univerzálny postup na všetko. Pri tejto téme sa vyhnite najmä: {config["avoid"]}. Ak si nie ste istí, začnite miernejším krokom a výsledok skontrolujte pred sušením.</p>
        {section_html}
        <h2>{config["expert_title"]}</h2>
        <p>{config["expert_p1"]}</p>
        <p>{config["expert_p2"]}</p>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
        <h2 style="margin-top: 0;">Zdroje a odborný kontext</h2>
        {config["source_html"]}
        </div>
        <h2>{config["test_title"]}</h2>
        <p>{config["test_text"]}</p>
        <h2>Kontrolný checklist pred praním</h2>
        <p>{config["checklist"]}</p>
        <h2>Ako rozhodnúť o dávke a programe</h2>
        <p>Rizikový kus nedávajte automaticky do najbližšej plnej práčky. Najprv ho porovnajte s ostatnou bielizňou podľa farby, hmotnosti, materiálu, špinavosti a citlivých detailov. Ak sa od zvyšku dávky výrazne líši, perte ho samostatne alebo s podobnými kusmi.</p>
        <p>Dobrá dávka nie je ani prázdny bubon, ani preplnený bubon. Textil potrebuje priestor na pohyb, oplach a odplavenie nečistôt. Pri citlivých kusoch býva menšia dávka bezpečnejšia než silnejší program.</p>
        <h2>Kontrola pred sušením a uložením</h2>
        <p>Po praní skontrolujte pach, tvar, zvyšky pracieho produktu, mapy, tvrdosť, farbu a citlivé miesta ako golier, manžety, lemy alebo švy. Ak problém vidíte ešte na mokrom textile, neriešte ho horúcou sušičkou. Teplo môže zafixovať škvrnu, zhoršiť zrážanie alebo zvýrazniť poškodenie povrchu.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Rýchla zásada</h2>
        <p>{config["rule"]}</p>
        </div>
        {product_category_card(config)}
        {related_links(config["links"])}
        <h2>FAQ: praktické otázky</h2>
        {faq_html}
        """
    )


MARKERS = {key: value["marker"] for key, value in TOPICS.items()}
EXPANSIONS = {key: build_expansion(key) for key in TOPICS}


PUBLIC_REPLACEMENTS = [
    (re.compile(r"\bCTA\b", re.IGNORECASE), "odporúčanie"),
    (re.compile(r"\blongtail\b", re.IGNORECASE), "konkrétne otázky"),
    (re.compile(r"\blong-tail\b", re.IGNORECASE), "konkrétne otázky"),
    (re.compile(r"\bSEO\b", re.IGNORECASE), "vyhľadávanie"),
    (re.compile(r"\bfan-out\b", re.IGNORECASE), "rozšírenie témy"),
    (re.compile(r"\bsub-query\b", re.IGNORECASE), "podotázka"),
    (re.compile(r"\bsub query\b", re.IGNORECASE), "podotázka"),
    (re.compile(r"hľadané výrazy", re.IGNORECASE), "praktické otázky"),
    (re.compile(r"interná komunikácia", re.IGNORECASE), "poznámka"),
]


def article_slug(article):
    return article.get("link") or article.get("slug") or article.get("url", "").rstrip("/").split("/")[-1]


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


def remove_legacy_tail(long):
    markers = [
        long.find('<div style="border: 1px solid #dbe5de'),
        long.find("\n<h2>Súvisiace"),
        long.find("\n<h2>FAQ"),
    ]
    markers = [index for index in markers if index != -1]
    if not markers:
        return long
    return long[: min(markers)].rstrip()


def remove_repeated_recommendation_tail(long):
    marker = '<div style="border: 1px solid #dbe5de'
    first = long.find(marker)
    if first == -1:
        return long
    second = long.find(marker, first + len(marker))
    if second == -1:
        return long
    return long[:second].rstrip()


def insert_expansion(long, topic):
    cleaned = public_cleanup(long)
    marker = MARKERS[topic]
    if marker in cleaned:
        before, after = cleaned.split(marker, 1)
        rebuilt = remove_legacy_tail(before).rstrip() + "\n" + marker + after.rstrip()
        return remove_repeated_recommendation_tail(rebuilt)
    idx = insertion_index(cleaned)
    return cleaned[:idx].rstrip() + "\n" + EXPANSIONS[topic]


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
    response = requests.post(endpoint, json=body, headers={"Accept": "application/json, text/event-stream"}, timeout=120)
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
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 30 expert material/symbol/new/membrane/microplastic articles.")
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
                "wave": "retrofit-wave-30-expert-material-symbols-new-membrane-microplastics-five",
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
            source_config = next(config for config in ARTICLES if config["slug"] == item["slug"])
            if source_config.get("live_update") is False:
                mcp_updates.append(
                    {
                        "post_id": item["post_id"],
                        "slug": item["slug"],
                        "url": item["url"],
                        "skipped": True,
                        "reason": source_config["skip_reason"],
                    }
                )
                continue
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
                "wave": "retrofit-wave-30-expert-material-symbols-new-membrane-microplastics-five",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "live_updated": args.update_live,
                "updated_count": sum(1 for item in mcp_updates if not item.get("skipped")),
                "skipped_count": sum(1 for item in mcp_updates if item.get("skipped")),
                "updates": mcp_updates,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"source_updates": len(updates), "live_updated": args.update_live, "mcp_updates": len(mcp_updates), "out": str(OUT_JSON), "mcp_results": str(MCP_RESULTS)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
