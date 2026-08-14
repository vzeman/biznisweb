#!/usr/bin/env python3
"""Expand canonical VEVO articles and differentiate their exact public duplicates."""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


PROJECT = Path(__file__).resolve().parents[1]
TOOLS = PROJECT / "tools"
sys.path.insert(0, str(TOOLS))

import biznisweb_vevo_content_mcp as content_mcp
import vevo_article_depth_guard as depth_guard
import vevo_html_safety_guard as html_guard
import vevo_public_content_guard as public_guard
from vevo_duplicate_guard import norm


DATE = "2026-07-14"
MARKER_PREFIX = "VEVO-EXACT-DUPLICATE-REPAIR-20260714"
PREPARED = PROJECT / "imports" / "exact-duplicate-remediation-2026-07-14-articles.json"
BACKUP = PROJECT / "exports" / "exact-duplicate-remediation-2026-07-14-backup.json"
REPORT = PROJECT / "exports" / "exact-duplicate-remediation-2026-07-14-results.json"

IFRA_STANDARDS = "https://ifrafragrance.org/initiatives-positions/safe-use-fragrance-science/ifra-standards/ifra-code-of-practice"
IFRA_MAKING = "https://ifrafragrance.org/about-fragrance/how-is-fragrance-made"
LIMONENE_STUDY = "https://pubmed.ncbi.nlm.nih.gov/19125719/"
LAUNDRY_HYGIENE = "https://pubmed.ncbi.nlm.nih.gov/33962979/"
LAUNDRY_MALODOUR = "https://pubmed.ncbi.nlm.nih.gov/39924526/"
SILICONE_STUDY = "https://doi.org/10.1016/j.jics.2024.101197"


def section(title: str, first: str, second: str) -> tuple[str, list[str]]:
    return title, [first, second]


COMMON_FRAGRANCE_COMMERCE = {
    "category_title": "Objavte parfumy do prania",
    "category_body": (
        "Keď už rozumiete princípu vône, vyberajte podľa charakteru kompozície "
        "a začnite menšou dávkou podľa návodu na obale."
    ),
    "category_href": "/c/vevo-fragrance/parfum-do-prania",
    "product_title": "Parfum do prania Vevo No.07 Ylang Absolute",
    "product_body": (
        "Konkrétna voľba pre domácnosť, ktorá chce výraznejší kvetinovo-orientálny "
        "charakter. Intenzitu vždy prispôsobte náplni, materiálu a vlastnej citlivosti."
    ),
    "product_href": "/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute",
}

COMMON_LAUNDRY_COMMERCE = {
    "category_title": "Pracie gély pre čistý základ",
    "category_body": (
        "Vôňa patrí na dobre vypranú bielizeň. Prací gél voľte podľa farby, "
        "znečistenia, tvrdosti vody a pokynov na textilnom štítku."
    ),
    "category_href": "/c/vevo-home-care/pranie/praci-gel",
    "product_title": "Prací gél z Marseillského mydla",
    "product_body": (
        "Praktický základ bežného prania bez potreby kombinovať priveľa rôznych "
        "prostriedkov. Dávkujte podľa návodu a nepreplňujte bubon."
    ),
    "product_href": "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
}

DRYER_COMMERCE = {
    "category_title": "Vône určené do sušičky",
    "category_body": (
        "Do sušičky používajte iba riešenie určené na tento spôsob aplikácie "
        "a dodržte návod výrobcu aj pokyny k spotrebiču."
    ),
    "category_href": "/c/vevo-fragrance/parfum-do-susicky",
    "product_title": "Parfum do sušičky Vevo No.07 Ylang Absolute",
    "product_body": (
        "Konkrétna vôňa pre sušenie bielizne. Použite ju iba spôsobom uvedeným "
        "na výrobku, nie priamym nalievaním na textil alebo do spotrebiča."
    ),
    "product_href": "/p-1609/parfum-do-susicky-vevo-no-07-ylang-absolute",
}


CONFIGS: list[dict[str, Any]] = [
    {
        "post_id": "1579",
        "expected_title": "Vonné esencie – Srdce každého parfumu",
        "expected_slug": "vonne-esencie-srdce-kazdeho-parfumu",
        "title": "Vonné esencie – Srdce každého parfumu",
        "short": (
            "Vonné esencie sú suroviny a kompozície, z ktorých parfumér skladá výslednú vôňu. "
            "Spoznajte prírodné aj syntetické zložky, vonnú pyramídu, bezpečnosť a použitie v praní."
        ),
        "description": (
            "Čo sú vonné esencie, ako vznikajú, ako sa líšia prírodné a syntetické zložky "
            "a čo rozhoduje o ich výdrži v parfume aj na bielizni."
        ),
        "quick": (
            "Vonná esencia nie je jedna konkrétna látka ani automaticky čistý esenciálny olej. "
            "V praxi môže ísť o prírodnú surovinu, syntetickú aromatickú molekulu alebo hotovú "
            "parfumovú kompozíciu. O výsledku rozhoduje ich pomer, stabilita, bezpečné použitie "
            "a to, pre aký výrobok bola kompozícia navrhnutá."
        ),
        "intro": (
            "Keď sa povie esencia, veľa ľudí si predstaví olej vytlačený z kvetov. Parfuméria "
            "je však širšia. Pracuje s destilátmi, absolútmi, extraktmi, izolovanými prírodnými "
            "molekulami aj látkami vytvorenými syntézou. Parfumér ich neskladá podľa pôvodu, "
            "ale podľa vône, stability a funkcie v kompozícii."
        ),
        "points": [
            "prírodný pôvod automaticky neznamená vyššiu bezpečnosť ani lepšiu výdrž",
            "vrchné, srdcové a základné tóny opisujú vývoj vône, nie tri oddelené tekutiny",
            "vôňa na pokožku a vôňa na bielizeň musia zvládať iné prostredie",
            "hotový výrobok sa dávkuje podľa návodu; samotná esencia sa na textil neleje",
        ],
        "sections": [
            section(
                "Čo presne znamená pojem vonná esencia",
                (
                    "V bežnej reči sa slovom esencia označuje takmer všetko koncentrovane voňavé. "
                    "Odborne je presnejšie rozlišovať vonnú surovinu a parfumovú kompozíciu. "
                    "Surovina je jednotlivý materiál, napríklad bergamotový olej alebo konkrétna "
                    "aromatická molekula. Kompozícia je premyslená zmes desiatok surovín, ktorá "
                    "vytvára jednotný dojem."
                ),
                (
                    "Toto rozlíšenie pomáha aj pri nákupe. Označenie parfum, fragrance alebo aroma "
                    "na etikete zvyčajne pomenúva zmes, nie jedinú ingredienciu. Ak chcete rozumieť "
                    "tomu, prečo vôňa pôsobí najprv sviežo a neskôr mäkko či drevito, prečítajte si "
                    "aj článok o <a href=\"/n/koncentracia-parfumu-edp-edt-edc-vysvetlene\">"
                    "koncentrácii a intenzite parfumu</a>."
                ),
            ),
            section(
                "Prírodné, syntetické a prírodne identické zložky",
                (
                    "Prírodné materiály vznikajú destiláciou, lisovaním alebo extrakciou rastlín. "
                    "Ich vôňa býva komplexná, ale mení sa podľa úrody, pôvodu a skladovania. "
                    "Syntetické molekuly umožňujú vytvoriť stabilné tóny, rozšíriť parfumérovu paletu "
                    "a nahradiť vzácne alebo problematické suroviny. Obe skupiny môžu byť kvalitné."
                ),
                (
                    "Bezpečnosť nemožno posudzovať iba podľa slova prírodný. Aj prirodzené citrusové "
                    "terpény sa pri kontakte so vzduchom menia. Preto sa formulácie hodnotia podľa "
                    "konkrétnej látky, dávky a spôsobu použitia. IFRA opisuje systém štandardov, "
                    "ktoré môžu vybrané materiály zakázať, obmedziť alebo určiť požiadavky na čistotu."
                ),
            ),
            section(
                "Ako funguje vrchný tón, srdce a základ",
                (
                    "Vonná pyramída je pomôcka na opis času. Vrchné tóny sa prejavia rýchlo a často "
                    "obsahujú ľahké citrusové či aromatické zložky. Srdce určuje hlavný charakter "
                    "kompozície a môže byť kvetinové, korenisté alebo ovocné. Základ nastupuje pomalšie "
                    "a býva drevitý, pižmový, balzamový alebo ambrový."
                ),
                (
                    "Pyramída neznamená, že sa vrstvy aktivujú presne po minútach. Zložky sa odparujú "
                    "súbežne a navzájom sa ovplyvňujú. Na bielizni je vývoj iný než na pokožke, pretože "
                    "vlákno nemá telesnú teplotu ani kožný maz. Výsledok mení aj pranie, oplachovanie "
                    "a sušenie."
                ),
            ),
            section(
                "Prečo musí byť kompozícia prispôsobená výrobku",
                (
                    "Parfum do prania prechádza vodou, detergentom, mechanickým pohybom a pláchaním. "
                    "Interiérová vôňa sa zasa rozptyľuje do vzduchu a parfum na pokožku pracuje s teplom "
                    "tela. Rovnaký nápad vône preto nemožno bez úprav preniesť do každého výrobku. "
                    "Mení sa nosič, rozpustnosť aj požadovaný profil uvoľňovania."
                ),
                (
                    "Pri bielizni má najväčší zmysel sledovať hotový výrobok a jeho návod. Ak chcete "
                    "praktický základ, pokračujte témou <a href=\"/n/parfum-do-prania-co-to-je-a-ako-funguje\">"
                    "čo je parfum do prania a ako funguje</a>. Samostatnú parfumovú surovinu bez pokynov "
                    "výrobcu nepridávajte do práčky ani sušičky."
                ),
            ),
            section(
                "Stabilita, svetlo, vzduch a skladovanie",
                (
                    "Suroviny môžu reagovať na kyslík, teplo a svetlo. Preto sa vonné výrobky skladujú "
                    "uzavreté, mimo priameho slnka a extrémnych teplôt. Nejde len o slabšiu intenzitu. "
                    "Oxidáciou sa môže meniť celý profil: svieži tón môže pôsobiť plocho, ostro alebo "
                    "inak než pri prvom otvorení."
                ),
                (
                    "Praktická domácnosť nepotrebuje laboratórne podmienky. Stačí pevne uzatvoriť obal, "
                    "nenechávať ho na rozpálenom parapete či pri radiátore a neprelievať výrobok do "
                    "neoznačenej nádoby. Pri citlivej pokožke používajte len odporúčanú dávku a sledujte "
                    "informácie na etikete."
                ),
            ),
            section(
                "Ako vôňu hodnotiť bez unáhleného záveru",
                (
                    "Prvý nádych ukazuje najmä najprchavejšie zložky. Vôňu preto neposudzujte len z hrdla "
                    "fľaše. Pri praní ju vyskúšajte na jednej bežnej náplni, po usušení a znovu po dni. "
                    "Tak zistíte, či vám vyhovuje prvý dojem aj stopa, ktorá zostáva na textile."
                ),
                (
                    "Nepridávajte ďalšiu dávku hneď preto, že si nos na vôňu zvykol. Čuchová adaptácia "
                    "zníži vnímanú intenzitu, no iný človek ju môže cítiť výrazne. Na porovnanie viacerých "
                    "kompozícií je praktická <a href=\"/p-249/sada-vsetkych-6-vzoriek-po-1ks\">sada vzoriek</a>."
                ),
            ),
        ],
        "table": {
            "headers": ["Pojem", "Čo označuje", "Praktický význam"],
            "rows": [
                ["Vonná surovina", "Jedna prírodná alebo syntetická zložka", "Stavebný prvok kompozície"],
                ["Parfumová kompozícia", "Zmes viacerých surovín", "Hotový vonný charakter"],
                ["Vrchný tón", "Rýchlo vnímaná časť vône", "Prvý dojem, často sviežosť"],
                ["Srdce", "Hlavný charakter kompozície", "Kvetinový, ovocný či korenistý profil"],
                ["Základ", "Pomalšie sa uvoľňujúce tóny", "Hĺbka a dlhšia stopa"],
            ],
        },
        "steps": [
            "Vyberte hotový výrobok určený na konkrétny spôsob použitia.",
            "Prečítajte etiketu, dávkovanie a prípadné upozornenia.",
            "Prvú skúšku urobte s menšou odporúčanou dávkou na bežnej bielizni.",
            "Vôňu hodnoťte až po úplnom usušení a s odstupom niekoľkých hodín.",
            "Obal pevne zatvorte a skladujte mimo tepla a priameho svetla.",
            "Ak vôňa dráždi, výrobok prestaňte používať a riaďte sa údajmi na etikete.",
        ],
        "expert": [
            (
                "IFRA vysvetľuje, že bezpečné použitie vonných zložiek sa hodnotí podľa konkrétnej "
                "aplikácie a expozície. Štandard môže látku obmedziť, zakázať alebo určiť podmienky "
                "jej použitia. Nejde teda o jednoduché delenie prírodné verzus syntetické."
            ),
            (
                "Pri citrusových zložkách je dôležité aj skladovanie. Experimentálna práca o limonéne "
                "a linaloole ukázala, že ich oxidované formy môžu byť dráždivejšie než neoxidované. "
                "Pre spotrebiteľa z toho vyplýva jednoduchý záver: používať hotový výrobok podľa návodu "
                "a chrániť ho pred zbytočným teplom a vzduchom."
            ),
        ],
        "sources": [
            ("Ako vzniká vôňa", IFRA_MAKING),
            ("IFRA Code of Practice", IFRA_STANDARDS),
            ("Štúdia o oxidácii limonénu a linaloolu", LIMONENE_STUDY),
        ],
        "faq": [
            ("Je vonná esencia to isté ako esenciálny olej?", "Nie. Esenciálny olej je konkrétny typ prírodného extraktu, kým vonná esencia môže označovať širšiu surovinu alebo celú parfumovú kompozíciu."),
            ("Sú syntetické vône automaticky horšie?", "Nie. Kvalitu a bezpečnosť určuje konkrétna látka, čistota, dávka a vhodnosť pre daný výrobok, nie iba pôvod."),
            ("Prečo vôňa na bielizni vonia inak než vo fľaši?", "Pranie, oplach, typ vlákna a sušenie menia pomer prchavých zložiek, preto sa výsledná stopa môže líšiť."),
            ("Môžem dať čistú vonnú esenciu priamo do práčky?", "Nie bez výslovného návodu výrobcu. Používajte hotový výrobok určený na pranie."),
            ("Ako dlho vonné suroviny vydržia?", "Závisí to od zloženia a skladovania. Výrobok uchovávajte uzavretý a riaďte sa dobou použiteľnosti na obale."),
        ],
        **COMMON_FRAGRANCE_COMMERCE,
    },
    {
        "post_id": "1611",
        "expected_title": "Vonné esencie – Srdce každého parfumu",
        "expected_slug": "vonne-esencie-srdce-kazdeho-parfumu1",
        "title": "Ako vzniká parfumová kompozícia: od suroviny po hotovú vôňu",
        "short": (
            "Parfumová kompozícia vzniká spojením surovín, akordov, skúšok stability a bezpečnostného "
            "hodnotenia. Pozrite sa na praktický proces od prvého zadania po hotovú vôňu."
        ),
        "description": (
            "Ako parfumér skladá kompozíciu, čo je akord, prečo sa vôňa testuje v konkrétnom výrobku "
            "a ako sa od laboratórnej vzorky dostane k hotovému produktu."
        ),
        "quick": (
            "Hotová vôňa nevzniká zmiešaním troch náhodných olejov. Začína zadaním, pokračuje výberom "
            "surovín a tvorbou akordov, potom nasledujú opakované úpravy, skúšky stability a hodnotenie "
            "v konkrétnom výrobku. Kompozícia pre pranie sa musí skúšať pri praní, nie iba na papieriku."
        ),
        "intro": (
            "Tento článok sa sústreďuje na proces tvorby. Základnú definíciu surovín nájdete v článku "
            "<a href=\"/n/vonne-esencie-srdce-kazdeho-parfumu\">vonné esencie – srdce každého parfumu</a>. "
            "Tu sledujeme cestu od nápadu cez laboratórne modifikácie až po kompozíciu, ktorá funguje "
            "v reálnom praní, na textile a po usušení."
        ),
        "points": [
            "zadanie opisuje cieľ, publikum, intenzitu aj prostredie použitia",
            "akord je menšia harmonická jednotka, z ktorej možno stavať celú vôňu",
            "hodnotenie na papieriku nestačí pre výrobok určený do vody a na textil",
            "poslednou fázou nie je kreativita, ale stabilita, bezpečnosť a opakovateľnosť",
        ],
        "sections": [
            section(
                "Prvý krok: presné zadanie vône",
                (
                    "Parfumér potrebuje vedieť, čo má kompozícia vyjadrovať. Čistota môže znamenať "
                    "mydlový, bavlnený, citrusový alebo vzdušný dojem. Zadanie preto opisuje emóciu, "
                    "cieľového používateľa, očakávanú intenzitu, sezónu a výrobok, v ktorom sa bude "
                    "vôňa používať."
                ),
                (
                    "Pri parfume do prania sa pridáva praktická otázka: čo má zostať po pláchaní a "
                    "sušení. Veľmi prchavá kompozícia môže vo fľaši pôsobiť sviežo, ale na suchej "
                    "bielizni rýchlo zmizne. Naopak príliš ťažký základ môže pri vyššej dávke pôsobiť "
                    "únavne."
                ),
            ),
            section(
                "Od jednotlivých surovín k akordu",
                (
                    "Akord je kombinácia niekoľkých materiálov, ktorá vytvorí nový rozpoznateľný dojem. "
                    "Napríklad pocit čistej bavlny nie je vôňou jednej rastliny. Môže ho tvoriť súhra "
                    "pižmových, kvetinových, aldehydických a jemne drevitých tónov. Dobrý akord pôsobí "
                    "ucelene, hoci má viac zložiek."
                ),
                (
                    "Parfumér pripravuje malé varianty a mení vždy len časť vzorca. Sleduje prvý dojem, "
                    "srdce, základ aj to, či niektorá surovina neprekryje ostatné. Zmeny môžu byť veľmi "
                    "malé, pretože pri koncentrovaných materiáloch dokáže rozdiel v zlomku pomeru zmeniť "
                    "celý charakter."
                ),
            ),
            section(
                "Prečo sa vzorka musí skúšať v nosiči",
                (
                    "Kompozícia sa správa inak v alkohole, vodnom základe, detergente alebo výrobku na "
                    "textil. Nosič ovplyvňuje rozpustnosť, rýchlosť uvoľňovania a stabilitu. V praní "
                    "vstupuje do hry voda rôznej tvrdosti, prací prostriedok, teplota a pláchanie."
                ),
                (
                    "Preto sa kandidát nehodnotí iba čuchom z laboratórnej fľaštičky. Skúša sa v "
                    "zamýšľanom výrobku, pri odporúčanej dávke a na typických textíliách. Pri VEVO blogu "
                    "nájdete samostatný praktický návod <a href=\"/n/teplota-prania-a-vona-kompletny-sprievodca\">"
                    "ako teplota prania ovplyvňuje vôňu</a>."
                ),
            ),
            section(
                "Stabilita a opakovateľnosť",
                (
                    "Vôňa musí zostať prijateľná počas skladovania. Sleduje sa zmena farby, oddelenie "
                    "zložiek, strata sviežosti a vznik nežiaduceho tónu. Testy pri rôznych teplotách "
                    "pomáhajú odhaliť problém skôr, než sa produkt dostane do domácnosti."
                ),
                (
                    "Opakovateľnosť znamená, že ďalšia výrobná šarža má voňať rovnako. Pri prírodných "
                    "materiáloch môže úroda kolísať, preto sa suroviny analyticky aj senzoricky kontrolujú "
                    "a receptúra sa riadi presnými špecifikáciami."
                ),
            ),
            section(
                "Bezpečnostné posúdenie patrí do tvorby",
                (
                    "Bezpečnosť sa nepridáva až na konci ako formalita. Výber surovín a ich množstiev "
                    "musí zohľadniť zamýšľanú aplikáciu. IFRA štandardy rozlišujú kategórie použitia, "
                    "preto rovnaká surovina nemusí mať rovnaké podmienky vo všetkých produktoch."
                ),
                (
                    "Spotrebiteľ nemusí počítať koncentrácie jednotlivých molekúl. Má však používať "
                    "hotový výrobok podľa etikety, neprekračovať odporúčanú dávku a nespoliehať sa na "
                    "to, že prírodná zložka je bez rizika za každých okolností."
                ),
            ),
            section(
                "Ako sa vyberá finálny variant",
                (
                    "Rozhoduje rovnováha medzi tvorivým zámerom a reálnym výkonom. Finálna kompozícia "
                    "má byť rozpoznateľná, príjemná pri odporúčanej dávke, stabilná a vhodná pre daný "
                    "výrobok. Najsilnejšia vzorka nemusí byť najlepšia; pri bielizni je dôležité, aby "
                    "vôňa nerušila pri celodennom nosení."
                ),
                (
                    "Pri domácom výbere postupujte podobne v menšom. Porovnajte vzorky na rovnakej "
                    "bielizni, pri rovnakom programe a dávke. Jednu premennú meňte až v ďalšom praní. "
                    "Tak rozlíšite samotnú kompozíciu od vplyvu sušenia alebo dávkovania."
                ),
            ),
        ],
        "table": {
            "headers": ["Fáza", "Hlavná otázka", "Výstup"],
            "rows": [
                ["Zadanie", "Aký dojem a použitie má vôňa mať?", "Jasný tvorivý a technický cieľ"],
                ["Akordy", "Ktoré suroviny spolu vytvoria charakter?", "Prvé funkčné moduly vône"],
                ["Modifikácie", "Čo posilniť, ubrať alebo stabilizovať?", "Séria porovnateľných variantov"],
                ["Aplikačný test", "Ako vôňa funguje vo výrobku?", "Výsledok pri reálnom použití"],
                ["Kontrola", "Je bezpečná, stabilná a opakovateľná?", "Finálna kompozícia"],
            ],
        },
        "steps": [
            "Porovnávajte vône vždy pri rovnakej dávke a pracom programe.",
            "Na prvú skúšku použite bežnú bavlnenú náplň bez silného zvyškového pachu.",
            "Hodnoťte vôňu po usušení, po jednom dni a pri nosení.",
            "Zapíšte si, čo cítite na začiatku a čo zostáva neskôr.",
            "Až v ďalšom praní upravte dávku alebo spôsob sušenia.",
            "Vybraný výrobok skladujte uzavretý a mimo zdroja tepla.",
        ],
        "expert": [
            (
                "IFRA pri opise výroby vôní zdôrazňuje spojenie prírodných surovín, syntetických "
                "molekúl, práce parfuméra, kontroly kvality a finálneho bezpečnostného hodnotenia. "
                "Tvorba je teda zároveň senzorický aj technický proces."
            ),
            (
                "Pre domácnosť je dôležité, že názov tónu neodhaľuje celý recept. Označenie bavlna, "
                "citrus alebo drevo pomenúva výsledný dojem. Presné zloženie môže kombinovať viacero "
                "materiálov, ktoré spolu vytvárajú požadovaný akord."
            ),
        ],
        "sources": [
            ("Ako vzniká vôňa", IFRA_MAKING),
            ("IFRA Code of Practice", IFRA_STANDARDS),
        ],
        "faq": [
            ("Koľko surovín obsahuje parfumová kompozícia?", "Počet nie je znakom kvality a môže sa výrazne líšiť. Dôležitejšia je funkcia každej zložky a ich rovnováha."),
            ("Čo je parfumový akord?", "Menšia kombinácia surovín, ktorá vytvára jednotný dojem a môže byť stavebným prvkom celej vône."),
            ("Prečo kompozícia vonia inak v pracom výrobku?", "Voda, nosič, detergent, textil a sušenie menia uvoľňovanie jednotlivých zložiek."),
            ("Je najsilnejší variant automaticky najkvalitnejší?", "Nie. Kvalitná vôňa má byť vyvážená a príjemná pri odporúčanom použití, nie iba čo najintenzívnejšia."),
            ("Ako doma porovnať dve vône férovo?", "Použite rovnakú náplň, program, dávku a sušenie a výsledok hodnoťte až na suchej bielizni."),
        ],
        **COMMON_FRAGRANCE_COMMERCE,
    },
    {
        "post_id": "1610",
        "expected_title": "Koncentrácia parfumu – EdP, EdT, EdC vysvetlené",
        "expected_slug": "koncentracia-parfumu-edp-edt-edc-vysvetlene",
        "title": "Koncentrácia parfumu – EdP, EdT, EdC vysvetlené",
        "short": (
            "EdP, EdT a EdC orientačne opisujú podiel parfumovej kompozície, nie garantovanú silu. "
            "Spoznajte rozdiel medzi koncentráciou, intenzitou, projekciou a výdržou."
        ),
        "description": (
            "Rozdiel medzi parfumom, EdP, EdT a EdC: orientačné koncentrácie, výdrž, projekcia "
            "a dôvod, prečo dve vône s rovnakým označením nemusia pôsobiť rovnako."
        ),
        "quick": (
            "EdP, EdT a EdC sú zaužívané parfumérske označenia, ktoré orientačne súvisia s množstvom "
            "parfumovej kompozície v nosiči. Nie sú však univerzálnou zárukou intenzity ani presnej "
            "výdrže. Výsledok mení receptúra, prchavosť zložiek, dávka, prostredie a spôsob použitia."
        ),
        "intro": (
            "Koncentrácia je číslo o zložení, zatiaľ čo intenzita je zmyslový vnem. Ľahká citrusová "
            "kompozícia s vyšším podielom môže pôsobiť kratšie než nižšia dávka kompozície s výrazným "
            "drevito-pižmovým základom. Preto má zmysel čítať označenie ako orientáciu, nie ako sľub "
            "presného počtu hodín."
        ),
        "points": [
            "hranice medzi EdP, EdT a EdC sú orientačné a medzi značkami sa môžu líšiť",
            "vyšší podiel kompozície nemusí znamenať väčšiu projekciu v každej fáze",
            "výdrž ovplyvňuje prchavosť surovín, nosič, povrch a okolité podmienky",
            "parfum do prania sa posudzuje podľa návodu a výkonu na textile, nie podľa skratky EdP",
        ],
        "sections": [
            section(
                "Koncentrácia verzus vnímaná sila",
                (
                    "Koncentrácia vyjadruje pomer parfumovej kompozície k zvyšku výrobku. Vnímaná sila "
                    "závisí od toho, aké molekuly kompozícia obsahuje a ako rýchlo sa uvoľňujú. Niektoré "
                    "materiály sú čuchovo výrazné už v malom množstve, iné vytvárajú jemný efekt aj pri "
                    "vyššom podiele."
                ),
                (
                    "Preto sa pri výbere nepýtajte iba, či ide o EdP alebo EdT. Sledujte aj charakter "
                    "vône, odporúčanú aplikáciu a vlastnú citlivosť. Označenie pomáha porovnať formáty "
                    "v rámci jednej línie, ale medzi rôznymi receptúrami nemusí predpovedať výsledok."
                ),
            ),
            section(
                "Orientačné parfumérske kategórie",
                (
                    "Parfum alebo extrait býva najkoncentrovanejší, po ňom zvyčajne nasleduje EdP, EdT "
                    "a EdC. V literatúre aj obchodnej praxi sa uvádzajú intervaly, ktoré sa prekrývajú. "
                    "Neexistuje jedna globálna tabuľka, ktorá by každému výrobcovi prikazovala rovnakú "
                    "hranicu pre všetky názvy."
                ),
                (
                    "Dôležité je aj to, čo sa počíta do parfumovej kompozície. Zmes môže obsahovať "
                    "vonné suroviny aj funkčné zložky potrebné pre stabilitu. Percento preto bez znalosti "
                    "receptúry nehovorí všetko o tom, ako bude vôňa pôsobiť na človeka alebo textile."
                ),
            ),
            section(
                "Výdrž, projekcia a vonná stopa",
                (
                    "Výdrž opisuje, ako dlho vôňu ešte rozpoznáte. Projekcia hovorí, ako ďaleko pôsobí "
                    "od zdroja, a vonná stopa opisuje dojem, ktorý zostáva v priestore. Vôňa môže byť "
                    "dlhotrvajúca, ale blízka povrchu; iná môže mať silný úvod a rýchlejší pokles."
                ),
                (
                    "Na bielizni sa tieto vlastnosti menia pri pohybe, zahrievaní tela a skladovaní v "
                    "skrini. Preto výsledok hodnotíte inak než parfum na zápästí. Pri praní sa navyše "
                    "časť kompozície odplaví a časť ovplyvní sušenie."
                ),
            ),
            section(
                "Prečo sa EdP nedá priamo porovnať s parfumom do prania",
                (
                    "EdP je pomenovanie formátu osobnej parfumérie. Parfum do prania má iný nosič, "
                    "dávkovanie aj cestu k povrchu. Prechádza pracím cyklom a výsledok sa posudzuje na "
                    "suchom textile. Prenášať percentá alebo očakávanú výdrž z pokožky na bielizeň by "
                    "bolo zavádzajúce."
                ),
                (
                    "Pri pracom výrobku sa riaďte konkrétnym návodom. Začnite menšou odporúčanou dávkou "
                    "a upravujte ju po jednom kroku. Praktické vysvetlenie nájdete v článku "
                    "<a href=\"/n/parfum-do-prania-co-to-je-a-ako-funguje\">čo je parfum do prania</a>."
                ),
            ),
            section(
                "Ako koncentráciu čítať pri nákupe",
                (
                    "Najprv si určte, či chcete ľahké osvieženie, blízku osobnú vôňu alebo výraznejší "
                    "dojem. Potom sledujte tónový profil a vyskúšajte kompozíciu na povrchu, pre ktorý "
                    "je určená. Jednorazový nádych z fľaše zvýrazní vrchné tóny a môže skresliť výdrž."
                ),
                (
                    "Ak porovnávate dve vône do prania, použite rovnaký program, náplň a dávku. Po "
                    "usušení odložte vzorky oddelene a vráťte sa k nim na ďalší deň. Tak odlíšite "
                    "počiatočnú intenzitu od stopy, ktorá skutočne zostáva."
                ),
            ),
            section(
                "Dávka nie je koncentrácia",
                (
                    "Koncentrácia je vlastnosť výrobku, dávka je množstvo, ktoré použijete. Zdvojnásobiť "
                    "dávku neznamená automaticky zdvojnásobiť príjemnosť alebo výdrž. Môžete len zvýšiť "
                    "intenzitu, spotrebu a riziko zvyškov pri nesprávnom použití."
                ),
                (
                    "Rozumný postup je meniť iba jednu premennú. Ak bielizeň nevonia, najprv skontrolujte "
                    "čistotu práčky, veľkosť náplne, pláchanie a sušenie. Až potom jemne upravte dávku "
                    "v rámci návodu výrobcu."
                ),
            ),
        ],
        "table": {
            "headers": ["Označenie", "Bežný charakter", "Ako ho čítať"],
            "rows": [
                ["Parfum / Extrait", "Vyšší podiel kompozície", "Často blízka, bohatá a dlhšia stopa"],
                ["Eau de Parfum", "Plnší každodenný formát", "Rovnováha úvodu, srdca a základu"],
                ["Eau de Toilette", "Ľahší a prchavejší dojem", "Často výraznejší svieži úvod"],
                ["Eau de Cologne", "Krátke osvieženie", "Nízka očakávaná výdrž nie je chyba"],
                ["Parfum do prania", "Samostatná aplikačná kategória", "Riaďte sa dávkou a výsledkom na textile"],
            ],
        },
        "steps": [
            "Rozhodnite sa, akú intenzitu chcete pri bežnom používaní.",
            "Porovnávajte vône v rovnakom formáte a na rovnakom povrchu.",
            "Nevyhodnocujte výdrž iba podľa prvých minút.",
            "Pri praní nemeníte naraz dávku, program aj spôsob sušenia.",
            "Ak ste na vôňu citliví, začnite na dolnej hranici odporúčanej dávky.",
            "Výrobok skladujte uzavretý, aby sa profil zbytočne nemenil.",
        ],
        "expert": [
            (
                "IFRA štandardy sa venujú bezpečnému použitiu konkrétnych vonných materiálov v "
                "aplikačných kategóriách. Skratka EdP alebo EdT sama osebe nepredstavuje bezpečnostné "
                "hodnotenie ani univerzálny prísľub výkonu."
            ),
            (
                "Pri komunikácii koncentrácie je preto presnejšie hovoriť o orientačnom formáte a "
                "zároveň vysvetliť projekciu, výdrž a dávku. Spotrebiteľ tak neporovnáva čísla, ktoré "
                "vznikli pre rozdielne výrobky a podmienky."
            ),
        ],
        "sources": [("IFRA Code of Practice", IFRA_STANDARDS)],
        "faq": [
            ("Je EdP vždy silnejšie než EdT?", "Často má vyšší podiel kompozície, ale vnímanú silu a výdrž môže zmeniť samotná receptúra."),
            ("Koľko percent má EdP?", "Uvádzajú sa orientačné intervaly, ktoré sa medzi zdrojmi a značkami prekrývajú. Označenie preto nie je presná globálna norma."),
            ("Znamená vyššia koncentrácia dlhšiu výdrž?", "Môže k nej prispieť, no rozhoduje aj prchavosť zložiek, povrch, teplota a spôsob aplikácie."),
            ("Má parfum do prania označenie EdP?", "Nie je to porovnateľná kategória. Parfum do prania sa hodnotí podľa návodu a výsledku na textile."),
            ("Môžem slabšiu vôňu vyriešiť dvojnásobnou dávkou?", "Nie automaticky. Najprv skontrolujte pranie a sušenie a dávku upravujte iba v rámci pokynov výrobcu."),
        ],
        **COMMON_FRAGRANCE_COMMERCE,
    },
    {
        "post_id": "1607",
        "expected_title": "Koncentrácia parfumu – EdP, EdT, EdC vysvetlené",
        "expected_slug": "koncentracia-parfumu-edp-edt-edc-vysvetlene1",
        "title": "Ako intenzita vône súvisí s koncentráciou, dávkou a použitím",
        "short": (
            "Intenzitu vône neurčuje iba koncentrácia. Rozhoduje aj dávka, prchavosť tónov, povrch, "
            "teplota, čuchová adaptácia a spôsob aplikácie."
        ),
        "description": (
            "Prečo vyššia koncentrácia nemusí vždy voňať silnejšie a ako oddeliť vplyv receptúry, "
            "dávky, textilu, teploty a čuchovej adaptácie."
        ),
        "quick": (
            "Koncentrácia opisuje zloženie výrobku, dávka množstvo pri použití a intenzita to, čo "
            "skutočne vnímate. Tieto tri veci spolu súvisia, ale nie sú totožné. Ak chcete vôňu "
            "nastaviť rozumne, meňte vždy iba jednu premennú a výsledok hodnoťte s odstupom."
        ),
        "intro": (
            "Tento praktický článok nadväzuje na slovníkové vysvetlenie "
            "<a href=\"/n/koncentracia-parfumu-edp-edt-edc-vysvetlene\">EdP, EdT a EdC</a>. "
            "Namiesto kategórií rieši situáciu, keď je vôňa doma príliš slabá, príliš silná alebo "
            "po krátkom čase prestane byť vnímateľná."
        ),
        "points": [
            "rovnaká dávka dvoch kompozícií môže pôsobiť úplne odlišne",
            "textil, miestnosť a pokožka uvoľňujú vôňu iným spôsobom",
            "čuchová adaptácia môže vytvoriť dojem, že vôňa zmizla",
            "správna úprava dávky je malý krok, nie náhodné zdvojnásobenie",
        ],
        "sections": [
            section(
                "Štyri premenné, ktoré si ľudia zamieňajú",
                (
                    "Koncentrácia je pomer kompozície vo výrobku. Dávka je množstvo výrobku pri jednom "
                    "použití. Projekcia opisuje priestorový dosah a výdrž čas, počas ktorého vôňu ešte "
                    "rozpoznáte. Jedna hodnota nevypočíta ostatné bez znalosti receptúry a podmienok."
                ),
                (
                    "Pri praní je užitočné začať stabilným základom: rovnaká hmotnosť bielizne, program "
                    "a sušenie. Ak zmeníte naraz gél, parfum, teplotu a náplň, nezistíte, čo spôsobilo "
                    "lepší alebo horší výsledok."
                ),
            ),
            section(
                "Prečo ľahké tóny pôsobia silno a krátko",
                (
                    "Citrusové a niektoré aromatické zložky sa uvoľňujú rýchlo. Vytvoria jasný prvý "
                    "dojem, no nemusia zostať dominantné. Drevité, pižmové alebo balzamové materiály "
                    "sa zvyčajne prejavujú bližšie k povrchu a dlhšie. To nie je hodnotenie kvality, "
                    "ale odlišná funkcia v kompozícii."
                ),
                (
                    "Ak chcete sviežu vôňu s lepšou stopou, nehľadajte iba viac citrusov. Dôležité je, "
                    "ako sú podporené srdcom a základom. Samostatný prehľad nájdete v článku "
                    "<a href=\"/n/citrusove-vone-sviezost-a-energia-pre-vase-pradlo\">citrusové vône</a>."
                ),
            ),
            section(
                "Povrch a teplota menia uvoľňovanie",
                (
                    "Na pokožke sa vôňa zahrieva a mieša s kožným mazom. V miestnosti sa rozptyľuje "
                    "do objemu vzduchu. Na textile sa zachytáva na vláknach a uvoľňuje pri pohybe. "
                    "Preto nie je férové hodnotiť praciu vôňu iba z fľaše alebo na papieriku."
                ),
                (
                    "Aj pri bielizni sa mení výsledok podľa materiálu. Bavlna, polyester, vlna a "
                    "mikrovlákno majú odlišnú štruktúru a účel. Pri funkčných textíliách má vždy "
                    "prednosť zachovanie vlastností a pokyny výrobcu pred snahou o čo najsilnejšiu vôňu."
                ),
            ),
            section(
                "Čuchová adaptácia a únava",
                (
                    "Po určitom čase mozog stabilný pach potlačí. Človek v miestnosti alebo vo svojom "
                    "oblečení ho vníma menej než návšteva, ktorá práve prišla. Tento jav vedie k "
                    "zbytočnému pridávaniu ďalšej dávky."
                ),
                (
                    "Pri hodnotení si dajte prestávku, vyvetrajte a porovnajte bielizeň v inom čase. "
                    "Ak niekto v domácnosti cíti vôňu výrazne, no vy nie, problém nemusí byť v slabej "
                    "koncentrácii."
                ),
            ),
            section(
                "Ako bezpečne hľadať vhodnú dávku",
                (
                    "Začnite na dolnej časti odporúčania výrobcu. Pri ďalšom rovnakom praní upravte "
                    "množstvo iba mierne. Sledujte intenzitu po usušení, po dni v skrini a pri nosení. "
                    "Ak sa objavia zvyšky alebo ťažký dojem, dávku znížte."
                ),
                (
                    "Vôňou neprekryjete nedostatočne vypraný pot, mastnotu alebo zatuchnutú práčku. "
                    "Najprv odstráňte príčinu zápachu, potom dolaďte intenzitu. Pri citlivosti alebo "
                    "bolesti hlavy je správnym krokom menej produktu a vetranie, nie experiment s "
                    "ešte silnejšou kompozíciou."
                ),
            ),
            section(
                "Jednoduchý domáci porovnávací test",
                (
                    "Rozdeľte rovnaký typ bielizne do dvoch podobných praní. Zachovajte gél, program, "
                    "teplotu a sušenie. V druhom praní zmeňte iba dávku vône. Výsledok si označte, ale "
                    "nehodnoťte bezprostredne po otvorení práčky."
                ),
                (
                    "Po úplnom usušení porovnajte prvý dojem, intenzitu zblízka a stopu po jednom dni. "
                    "Ak rozdiel nie je príjemnejší, vyššia dávka nemá praktický prínos. Takýto postup "
                    "je presnejší než pridávanie podľa momentálneho pocitu."
                ),
            ),
        ],
        "table": {
            "headers": ["Premenná", "Čo mení", "Ako ju testovať"],
            "rows": [
                ["Koncentrácia výrobku", "Potenciál kompozície", "Porovnávajte podobné formáty"],
                ["Dávka", "Množstvo pri jednom použití", "Meňte v malých krokoch"],
                ["Povrch", "Rýchlosť uvoľňovania", "Hodnoťte na zamýšľanom materiáli"],
                ["Teplota", "Prchavosť a správanie produktu", "Dodržte štítok a návod"],
                ["Čuchová adaptácia", "Subjektívne vnímanie", "Urobte prestávku a porovnanie"],
            ],
        },
        "steps": [
            "Stabilizujte prací gél, program, náplň a sušenie.",
            "Začnite nižšou odporúčanou dávkou vône.",
            "Výsledok hodnoťte až na úplne suchej bielizni.",
            "Zapíšte si intenzitu bezprostredne a na ďalší deň.",
            "V ďalšom praní zmeňte iba jednu premennú.",
            "Pri podráždení alebo bolesti hlavy dávku znížte a vyvetrajte.",
        ],
        "expert": [
            (
                "Vnímanie vône nie je lineárny merač percent. Závisí od prahu jednotlivých molekúl, "
                "ich vzájomného pôsobenia a od adaptácie čuchu. Preto sa kompozícia hodnotí senzoricky "
                "aj technicky v konkrétnom použití."
            ),
            (
                "IFRA štandardy pracujú s aplikáciou a expozíciou, nie s marketingovou skratkou "
                "formátu. Pre domácnosť je preto najbezpečnejšie dodržať etiketu a nepoužívať "
                "koncentrovanú surovinu mimo určeného výrobku."
            ),
        ],
        "sources": [("IFRA Code of Practice", IFRA_STANDARDS)],
        "faq": [
            ("Prečo vôňu po chvíli necítim?", "Často ide o čuchovú adaptáciu. Dajte si prestávku a overte intenzitu s odstupom."),
            ("Zvýši dvojnásobná dávka dvojnásobne výdrž?", "Nie. Výsledok nie je lineárny a vyššia dávka môže byť iba ťažšia alebo zanechať zvyšky."),
            ("Prečo rovnaká vôňa pôsobí inak na bavlne a polyesteri?", "Vlákna majú inú povrchovú chémiu, štruktúru a spôsob zadržiavania vlhkosti aj vonných zložiek."),
            ("Mám dávku upraviť pri polovičnej náplni?", "Riaďte sa návodom konkrétneho výrobku a množstvom bielizne; nepoužívajte automaticky plnú dávku."),
            ("Ako rozlíšim slabú vôňu od zle vypranej bielizne?", "Ak ostáva pot, mastnota alebo zatuchnutie, najprv riešte prací proces a práčku, nie intenzitu parfumu."),
        ],
        **COMMON_FRAGRANCE_COMMERCE,
    },
    {
        "post_id": "1609",
        "expected_title": "Silikóny v avivážach – Skrytý nepriateľ vášho prádla",
        "expected_slug": "silikony-v-avivazach-skryty-nepriatel-vasho-pradla",
        "title": "Silikóny v avivážach – Skrytý nepriateľ vášho prádla",
        "short": (
            "Nie každá aviváž obsahuje rovnaké zložky a nie každý silikón sa správa rovnako. "
            "Zistite, ako zmäkčovadlá ovplyvňujú savosť, priedušnosť a funkčné textílie."
        ),
        "description": (
            "Silikóny a ďalšie zmäkčovadlá v avivážach: ako pôsobia na vlákna, kedy môžu znižovať "
            "zmáčavosť uterákov a prečo treba pri funkčných textíliách čítať štítok."
        ),
        "quick": (
            "Aviváž zmäkčuje povrch vlákien pomocou látok, ktoré sa na textil viažu. Niektoré "
            "formulácie môžu pri opakovanom alebo nadmernom používaní zhoršiť zmáčavosť uterákov "
            "či vlastnosti funkčného oblečenia. Nie je však presné tvrdiť, že každá aviváž obsahuje "
            "rovnaký silikón alebo že každý silikón vytvorí úplne nepriedušnú vrstvu."
        ),
        "intro": (
            "Najpraktickejšia otázka nie je, či je názov zložky strašidelný, ale či výrobok vyhovuje "
            "konkrétnemu textilu a či ho dávkujete správne. Uterák potrebuje savosť, športová vrstva "
            "odvod vlhkosti a mikrovlákno čistý aktívny povrch. Pri týchto materiáloch má funkcia "
            "prednosť pred pocitom maximálnej mäkkosti."
        ),
        "points": [
            "zmäkčovadlá sa líšia chemickou štruktúrou aj účinkom na zmáčavosť",
            "najčastejším domácim problémom je priveľká dávka a postupné nánosy",
            "uteráky, mikrovlákno a funkčné oblečenie majú osobitné požiadavky",
            "ocot sa nikdy nemieša s chlórovým bielidlom a nie je univerzálnou opravou",
        ],
        "sections": [
            section(
                "Ako aviváž vytvára pocit mäkkosti",
                (
                    "Po praní sú vlákna drsnejšie a pri sušení sa môžu navzájom spájať. Zmäkčovacie "
                    "látky sa adsorbujú na ich povrch, znižujú trenie a menia dotyk. Domáce aviváže "
                    "často využívajú katiónové povrchovo aktívne látky; niektoré formulácie obsahujú "
                    "aj silikónové zložky."
                ),
                (
                    "Výsledok závisí od chémie, dávky, materiálu a počtu cyklov. Preto nemožno každú "
                    "formuláciu posúdiť jednou vetou. Rozhodujú údaje na obale a odporúčania výrobcu "
                    "textilu, najmä pri membránach, elastických vláknach a technických úpravách."
                ),
            ),
            section(
                "Prečo môže utrpieť savosť uteráka",
                (
                    "Uterák saje vďaka slučkám a hydrofilnému povrchu bavlnených vlákien. Ak sa na "
                    "povrchu nahromadí priveľa zmäkčovacích alebo pracích zvyškov, voda sa môže "
                    "rozlievať pomalšie a uterák pôsobí síce hladko, ale menej funkčne."
                ),
                (
                    "Jedno použitie ešte neznamená trvalé poškodenie. Problém sa častejšie objaví pri "
                    "opakovanom predávkovaní, nedostatočnom pláchaní alebo príliš plnom bubne. "
                    "Súvisiaci praktický postup nájdete v článku "
                    "<a href=\"/n/ako-zmaekcit-uteraky\">ako zmäkčiť uteráky</a>. Pri diagnostike sa "
                    "riaďte aj štítkom a skúšobným praním bez aviváže."
                ),
            ),
            section(
                "Funkčné oblečenie, mikrovlákno a membrány",
                (
                    "Funkčná textília pracuje s prenosom vlhkosti, pórovitosťou alebo povrchovou úpravou. "
                    "Zmäkčovadlo určené na bežnú bavlnu nemusí byť vhodné pre športové tričko, čistiace "
                    "mikrovlákno či nepremokavú membránu. Najvyššiu autoritu má ošetrovací štítok."
                ),
                (
                    "Pri mikrovláknových utierkach môže povlak znížiť kontakt jemných vlákien s povrchom. "
                    "Pri športovom oblečení sa môžu zvyšky spájať s kožným mazom a pachmi. Tieto kúsky "
                    "perte oddelene, nepredávkujte gél a nepoužívajte aviváž, ak ju výrobca neodporúča."
                ),
            ),
            section(
                "Ako rozpoznať nánosy a odlíšiť inú príčinu",
                (
                    "Typickým signálom je uterák, po ktorom voda najprv steká, alebo športové oblečenie, "
                    "ktoré po zahriatí rýchlo zapácha. Rovnaký výsledok však môže spôsobiť aj priveľa "
                    "pracieho gélu, tvrdá voda, zanesená práčka alebo vlhké sušenie."
                ),
                (
                    "Urobte kontrolované pranie bez aviváže, s primeranou dávkou gélu a dostatkom miesta "
                    "v bubne. Ak sa stav po niekoľkých cykloch zlepší, nánosy boli pravdepodobným faktorom. "
                    "Ak nie, skontrolujte materiál, opotrebovanie a práčku."
                ),
            ),
            section(
                "Obnova textilu bez agresívnych experimentov",
                (
                    "Začnite bežným praním podľa štítku bez ďalšieho zmäkčovadla. Zvoľte správnu dávku "
                    "detergentu a podľa potreby dodatočné pláchanie. Uteráky sušte tak, aby neostali dlho "
                    "vlhké, no nepresušujte ich opakovane pri zbytočne vysokej teplote."
                ),
                (
                    "Nemiešajte domáce chemikálie. Ocot a chlórové bielidlo spolu môžu uvoľniť nebezpečný "
                    "plyn. Kyslé prípravky navyše nemusia byť vhodné pre každý spotrebič alebo textil. "
                    "Ak výrobca odporúča špeciálny prací prostriedok, má prednosť."
                ),
            ),
            section(
                "Vôňa bez zmäkčovania povrchu",
                (
                    "Ak chcete bielizeň prevoňať, ale pri konkrétnom textile nechcete používať aviváž, "
                    "zvoľte výrobok určený na vôňu a postupujte podľa jeho návodu. Parfum do prania "
                    "nenahrádza detergent a nemá prekryť zvyšky potu či biofilmu."
                ),
                (
                    "Pri uterákoch a športových veciach začnite nízkou intenzitou. Najprv zabezpečte "
                    "čistotu a rýchle usušenie, až potom dolaďte vôňu. Kategóriu nájdete medzi "
                    "<a href=\"/c/vevo-fragrance/parfum-do-prania\">parfumami do prania</a>."
                ),
            ),
        ],
        "table": {
            "headers": ["Textil", "Hlavná funkcia", "Praktické odporúčanie"],
            "rows": [
                ["Froté uterák", "Rýchle nasávanie vody", "Aviváž obmedzte a nepredávkujte"],
                ["Športové oblečenie", "Odvod vlhkosti", "Riaďte sa štítkom, často bez aviváže"],
                ["Mikrovlákno", "Kontakt jemných vlákien s povrchom", "Perte bez povlakotvorných prísad"],
                ["Membránové oblečenie", "Priedušnosť a ochranná úprava", "Použite špeciálny návod výrobcu"],
                ["Bežná bavlna", "Pohodlie a mäkkosť", "Primeraná dávka podľa etikety"],
            ],
        },
        "steps": [
            "Skontrolujte štítok textilu a návod pracieho výrobku.",
            "Pri funkčnom probléme vynechajte aviváž na niekoľko cyklov.",
            "Dávkujte gél podľa tvrdosti vody a nepreplňujte bubon.",
            "Podľa potreby pridajte pláchanie, nie ďalší náhodný prostriedok.",
            "Bielizeň vyberte hneď a dôkladne usušte.",
            "Výsledok porovnajte až po dvoch až troch kontrolovaných praniach.",
        ],
        "expert": [
            (
                "Výskum silikónových zmäkčovadiel ukazuje, že rozdielna molekulová štruktúra môže "
                "meniť mäkkosť aj hydrofilitu textilu. To podporuje presnejší záver: účinok nemožno "
                "zovšeobecniť na všetky silikóny, no formulácia a dávka majú pre zmáčavosť význam."
            ),
            (
                "Pri zápachu treba oddeliť povrchovú úpravu od mikrobiálneho problému. Odborný prehľad "
                "prania opisuje mechanické, chemické a teplotné faktory, ktoré spoločne ovplyvňujú "
                "odstránenie nečistôt a mikroorganizmov."
            ),
        ],
        "sources": [
            ("Štúdia silikónových zmäkčovadiel a hydrofility textilu", SILICONE_STUDY),
            ("Odborný prehľad hygieny prania", LAUNDRY_HYGIENE),
        ],
        "faq": [
            ("Obsahuje každá aviváž silikóny?", "Nie. Formulácie sa líšia a často používajú najmä katiónové zmäkčovadlá. Rozhoduje etiketa konkrétneho výrobku."),
            ("Zničí jedno použitie aviváže uterák?", "Zvyčajne nie. Problémom býva skôr opakované alebo nadmerné používanie a slabé pláchanie."),
            ("Ako obnoviť savosť uterákov?", "Perte ich podľa štítku bez aviváže, správne dávkujte detergent, nepreplňujte bubon a podľa potreby pridajte pláchanie."),
            ("Môžem používať aviváž na športové oblečenie?", "Iba ak to povoľuje výrobca textilu. Pri mnohých funkčných materiáloch sa neodporúča."),
            ("Môžem kombinovať ocot s bielidlom?", "Nie. Kyseliny sa nesmú miešať s chlórovými prípravkami."),
        ],
        **COMMON_LAUNDRY_COMMERCE,
    },
    {
        "post_id": "1606",
        "expected_title": "Silikóny v avivážach – Skrytý nepriateľ vášho prádla",
        "expected_slug": "silikony-v-avivazach-skryty-nepriatel-vasho-pradla1",
        "title": "Prečo uteráky strácajú savosť: nánosy aviváže a obnova vlákien",
        "short": (
            "Tvrdý alebo málo savý uterák nemusí byť zničený. Naučte sa odlíšiť nánosy aviváže "
            "a pracieho gélu od tvrdej vody, presušenia a opotrebovania."
        ),
        "description": (
            "Prečo uteráky po praní nenasávajú vodu, ako otestovať nánosy aviváže a detergentu "
            "a ako obnovovať savosť bez agresívnych chemických pokusov."
        ),
        "quick": (
            "Ak uterák prestal sať, najprv na niekoľko praní vynechajte aviváž, znížte nadmernú dávku "
            "gélu, nechajte v bubne viac priestoru a podľa potreby pridajte pláchanie. Zlepšenie sa "
            "nemusí ukázať po jednom cykle. Ak sa stav nemení, príčinou môže byť tvrdá voda, poškodené "
            "slučky, presušenie alebo prirodzené opotrebovanie."
        ),
        "intro": (
            "Tento článok rieši diagnostiku uterákov. Chemické vysvetlenie rôznych zmäkčovadiel nájdete "
            "v samostatnej téme <a href=\"/n/silikony-v-avivazach-skryty-nepriatel-vasho-pradla\">"
            "silikóny v avivážach</a>. Tu postupujeme podľa príznakov: voda steká, uterák je tvrdý, "
            "zapácha alebo sa jeho slučky mechanicky sploštili."
        ),
        "points": [
            "málo savý a tvrdý uterák nie sú vždy ten istý problém",
            "nánosy vznikajú aj z priveľa detergentu, nielen zo zmäkčovadla",
            "obnova je séria šetrných praní, nie jednorazový agresívny kúpeľ",
            "nový lacný uterák a opotrebovaný uterák môžu mať limit daný konštrukciou",
        ],
        "sections": [
            section(
                "Test: uterák je tvrdý alebo skutočne málo savý?",
                (
                    "Tvrdosť hodnotíte dotykom, savosť kontaktom s vodou. Kvapnite malé množstvo vody "
                    "na suchý uterák. Ak zostane na povrchu a pomaly sa vsakuje, problém je v zmáčavosti. "
                    "Ak sa vsiakne rýchlo, ale uterák pôsobí drsne, riešte skôr sušenie, minerály a "
                    "mechanickú pružnosť slučiek."
                ),
                (
                    "Test robte na úplne suchom uteráku bez nedávno naneseného kozmetického produktu. "
                    "Porovnajte stred a okraj. Lokálny rozdiel môže znamenať olej, krém alebo poškodenie, "
                    "kým rovnomerný problém skôr súvisí s praním."
                ),
            ),
            section(
                "Štyri najčastejšie príčiny",
                (
                    "Prvou je nadmerné zmäkčovadlo, druhou priveľa detergentu, treťou tvrdá voda a "
                    "štvrtou presušenie. K nim sa pridáva preplnený bubon, ktorý obmedzí mechanické "
                    "uvoľnenie nečistôt aj pláchanie. Nie je rozumné automaticky viniť jedinú zložku."
                ),
                (
                    "Mastné uteráky na vlasy či telo môžu niesť aj oleje a silikóny z kozmetiky. "
                    "Kuchynské utierky zasa zachytávajú tuk. Takéto znečistenie vyžaduje správny "
                    "detergent a teplotu podľa štítku, nie iba ďalšie pláchanie."
                ),
            ),
            section(
                "Šetrný obnovovací postup",
                (
                    "Uteráky roztrieďte podľa farby a štítku. Perte primeranú náplň bez aviváže a s "
                    "odmeraným množstvom gélu. Pri viditeľnej pene po praní alebo dlhodobom predávkovaní "
                    "pridajte extra pláchanie. Tento postup zopakujte pri ďalších bežných cykloch."
                ),
                (
                    "Nesnažte sa nánosy odstrániť miešaním viacerých chemikálií. Silné kyseliny, zásady "
                    "a bielidlá môžu poškodiť farbu, vlákno aj práčku. Ak chcete použiť pomocný výrobok, "
                    "musí byť určený na pranie a kompatibilný s textilom."
                ),
            ),
            section(
                "Tvrdá voda a minerálne zvyšky",
                (
                    "Tvrdá voda zvyšuje nároky na dávkovanie detergentu a môže prispieť k drsnému "
                    "pocitu. Riešením nie je odhadovať množstvo naslepo. Zistite tvrdosť vody od "
                    "dodávateľa alebo testom a dávkujte podľa tabuľky na pracom prostriedku."
                ),
                (
                    "Ak používate viac gélu, než vyžaduje miestna voda a znečistenie, môžete vytvoriť "
                    "opačný problém so zvyškami. Správna dávka je kompromis medzi účinnosťou a "
                    "opláchnutím, nie maximálne množstvo."
                ),
            ),
            section(
                "Sušenie, ktoré zachová slučky",
                (
                    "Vzdušné sušenie môže uterák stvrdnúť, ak sa nehýbe a voda obsahuje minerály. "
                    "Pred zavesením ho pretrepte a rozložte. Sušička dokáže slučky mechanicky nadýchať, "
                    "ale používajte teplotu podľa štítku a nenechávajte cyklus zbytočne pokračovať po "
                    "úplnom vysušení."
                ),
                (
                    "Dlhé vlhké schnutie je ďalší problém, pretože podporuje zatuchnutý pach. Výskum "
                    "pracieho cyklu spája vlhké podmienky sušenia s typickými pachmi a rastom niektorých "
                    "baktérií. Uterák preto sušte rýchlo a s prúdením vzduchu."
                ),
            ),
            section(
                "Kedy už pranie nepomôže",
                (
                    "Ak sú slučky zošúchané, vlákna polámané alebo textil dlhodobo poškodený vysokou "
                    "teplotou, pôvodnú štruktúru pranie nevráti. Starší uterák môže zostať použiteľný "
                    "na upratovanie, no jeho výkon pri osušení bude obmedzený."
                ),
                (
                    "Rovnako skontrolujte gramáž a konštrukciu nového výrobku. Veľmi hladký dekoratívny "
                    "uterák nemusí mať rovnakú savosť ako kvalitné froté. Údržba zlepší podmienky, ale "
                    "nezmení základnú stavbu textilu."
                ),
            ),
        ],
        "table": {
            "headers": ["Príznak", "Pravdepodobná príčina", "Prvý test"],
            "rows": [
                ["Voda zostáva na povrchu", "Povlak alebo mastnota", "Pranie bez aviváže"],
                ["Uterák je drsný, ale saje", "Tvrdá voda alebo sušenie", "Pretrepať a zmeniť sušenie"],
                ["Po namočení zapácha", "Nedostatočné pranie alebo pomalé schnutie", "Menšia náplň a rýchle sušenie"],
                ["Na povrchu je pena", "Priveľa detergentu", "Nižšia dávka a extra pláchanie"],
                ["Slučky sú sploštené", "Mechanické opotrebovanie", "Skontrolovať stav vlákien"],
            ],
        },
        "steps": [
            "Kvapkou vody odlíšte tvrdosť od slabej zmáčavosti.",
            "Na ďalšie prania vynechajte aviváž.",
            "Odmerajte gél podľa vody, náplne a znečistenia.",
            "Bubon nepreplňte a podľa potreby pridajte pláchanie.",
            "Uterák po praní pretrepte a rýchlo usušte.",
            "Po dvoch až troch cykloch znovu otestujte savosť.",
        ],
        "expert": [
            (
                "Štúdie zmäkčovadiel ukazujú, že molekulová štruktúra silikónovej zložky môže meniť "
                "hydrofilitu aj pocit mäkkosti. Domáca diagnostika preto musí pracovať s konkrétnym "
                "výrobkom a výsledkom, nie iba s jedným názvom ingrediencie."
            ),
            (
                "Novší výskum textilného mikrobiomu zároveň ukazuje význam sušenia. Dlhé vlhké "
                "podmienky môžu podporiť pachy aj mikrobiálnu aktivitu, takže savosť a sviežosť treba "
                "riešiť v celom cykle pranie–pláchanie–sušenie."
            ),
        ],
        "sources": [
            ("Štúdia silikónových zmäkčovadiel a hydrofility", SILICONE_STUDY),
            ("Výskum pachov pri praní a vlhkom sušení", LAUNDRY_MALODOUR),
        ],
        "faq": [
            ("Koľko praní potrebuje uterák na obnovu?", "Závisí to od množstva nánosov. Zmenu posudzujte po dvoch až troch správne nastavených cykloch."),
            ("Pomôže viac pracieho gélu?", "Nie pri probléme so zvyškami. Priveľa gélu môže oplachovanie ešte zhoršiť."),
            ("Je extra pláchanie vždy potrebné?", "Nie. Má zmysel pri podozrení na zvyšky, citlivosti alebo viditeľnej pene, no zvyšuje spotrebu vody."),
            ("Môžem uteráky sušiť v sušičke?", "Ak to povoľuje štítok. Použite primeranú teplotu a nepresušujte ich."),
            ("Kedy uterák vymeniť?", "Keď sú slučky mechanicky zničené, textil sa trhá alebo ani po správnej údržbe neplní svoju funkciu."),
        ],
        **COMMON_LAUNDRY_COMMERCE,
    },
    {
        "post_id": "1608",
        "expected_title": "Teplota prania a vôňa – Kompletný sprievodca",
        "expected_slug": "teplota-prania-a-vona-kompletny-sprievodca",
        "title": "Teplota prania a vôňa – Kompletný sprievodca",
        "short": (
            "Teplotu prania vyberajte najprv podľa štítku, materiálu a hygienickej potreby. "
            "Až potom riešte, ako 30, 40 alebo 60 °C ovplyvní vôňu a jej výdrž."
        ),
        "description": (
            "Ako teplota prania ovplyvňuje čistotu, mikroorganizmy, škvrny a vôňu bielizne. "
            "Praktická voľba medzi 20, 30, 40 a 60 °C podľa textilu."
        ),
        "quick": (
            "Neexistuje jedna teplota, ktorá je najlepšia pre všetku bielizeň aj vôňu. Najprv rešpektujte "
            "ošetrovací štítok, materiál a mieru znečistenia. Nižšia teplota šetrí citlivé vlákna a "
            "energiu, vyššia môže pomôcť pri hygiene a mastnote, ak ju textil znesie. Vôňa je až druhé "
            "kritérium a nesmie nahradiť čistotu."
        ),
        "intro": (
            "Jednoduchá poučka, že 30 °C vždy zachová vôňu a 60 °C ju vždy zničí, je príliš hrubá. "
            "Výsledok mení zloženie vône, program, čas, detergent, náplň, pláchanie a sušenie. "
            "Rozumné rozhodnutie preto začína otázkou, čo periete a čo z bielizne potrebujete odstrániť."
        ),
        "points": [
            "ošetrovací štítok určuje bezpečný strop, nie povinnú teplotu pre každý cyklus",
            "hygiena vzniká kombináciou teploty, času, detergentu, mechaniky a prípadných prísad",
            "nižšia teplota môže zachovať prchavé tóny, no nevyrieši zle vypraný pach",
            "vôňu posudzujte až po správnom a úplnom usušení",
        ],
        "sections": [
            section(
                "Čo teplota robí pri praní",
                (
                    "Teplejšia voda zvyčajne zlepšuje rozpúšťanie tukov a zrýchľuje niektoré chemické "
                    "procesy, no môže poškodiť farbu, elastan, vlnu alebo špeciálnu úpravu. Moderné "
                    "detergenty sú navrhnuté aj pre nižšie teploty, ale ich výkon závisí od typu škvrny "
                    "a dĺžky programu."
                ),
                (
                    "Teplota na displeji navyše neznamená, že celý cyklus prebieha stále pri rovnakej "
                    "hodnote. Spotrebič vodu ohrieva a program kombinuje čas, pohyb a pláchanie. "
                    "Porovnávajte preto celé programy, nie iba jedno číslo."
                ),
            ),
            section(
                "20 a 30 °C: farby, jemnosť a bežné nosenie",
                (
                    "Nízke teploty sa hodia na ľahko znečistené farebné a citlivé kúsky, ak ich povoľuje "
                    "štítok a detergent. Obmedzujú tepelné namáhanie a spotrebu energie. Pri spotenom "
                    "polyesteri, mastných utierkach alebo infekčnom znečistení však môžu vyžadovať "
                    "dlhší program alebo iný hygienický postup."
                ),
                (
                    "Svieže vrchné tóny môžu pri nižšej teplote pôsobiť výraznejšie, ale iba na čistej "
                    "bielizni. Ak ostáva kožný maz alebo biofilm, parfum sa zmieša s pachom a výsledok "
                    "bude horší bez ohľadu na dávku."
                ),
            ),
            section(
                "40 °C: univerzálna voľba s výnimkami",
                (
                    "Štyridsať stupňov býva praktickou voľbou pre bežnú bavlnu, zmesové textílie a "
                    "stredné znečistenie, ak to povoľuje štítok. Poskytuje vyšší prací výkon než veľmi "
                    "studený cyklus bez extrémneho zaťaženia mnohých materiálov."
                ),
                (
                    "Nie je však automaticky vhodná pre vlnu, hodváb, niektoré membrány alebo kúsky "
                    "s lepenými prvkami. Naopak pri posteľnej bielizni chorého človeka môže byť potrebný "
                    "vyšší hygienický režim. Vždy rozhoduje konkrétny textil a situácia."
                ),
            ),
            section(
                "60 °C a hygienické pranie",
                (
                    "Vyššia teplota má význam pri odolnej bavlne, uterákoch, posteľnej bielizni alebo "
                    "hygienickej potrebe, ak ju štítok povoľuje. Odborné prehľady ukazujú, že teplota "
                    "patrí medzi dôležité faktory mikrobiálnej redukcie, no účinok závisí aj od chémie "
                    "a času."
                ),
                (
                    "Pri 60 °C môžu ľahké vonné zložky pôsobiť slabšie, ale cieľom takého cyklu je "
                    "predovšetkým čistota. Ak chcete vôňu, použite kompatibilný výrobok podľa návodu "
                    "a nesnažte sa kompenzovať teplotu nadmernou dávkou."
                ),
            ),
            section(
                "Pláchanie a sušenie rozhodujú o výsledku",
                (
                    "Priveľa gélu alebo parfumu môže po slabom pláchaní zanechať zvyšky. Preplnený "
                    "bubon obmedzí pohyb aj prietok vody. Bielizeň potom môže pôsobiť ťažko, lepkavo "
                    "alebo zatuchnuto, hoci bola nastavená správna teplota."
                ),
                (
                    "Po praní ju vyberte bez odkladu. Vlhké textílie ponechané v bubne alebo sušené "
                    "bez prúdenia vzduchu môžu získať pach, ktorý prekryje pôvodnú kompozíciu. "
                    "Samostatne sa tomu venuje článok <a href=\"/n/susicka-a-vone-ako-zachovat-vonu-po-suseni\">"
                    "sušička a vône</a>."
                ),
            ),
            section(
                "Ako nastavenie testovať doma",
                (
                    "Vyberte jednu opakovanú náplň, napríklad tričká podobného materiálu. Zachovajte "
                    "gél, dávku vône a sušenie a porovnajte dva programy, ktoré povoľuje štítok. "
                    "Hodnoťte čistotu, pach po zahriatí pri nosení, stav farby a vôňu na ďalší deň."
                ),
                (
                    "Ak nižšia teplota zabezpečí rovnakú čistotu a lepšie chráni textil, má praktický "
                    "zmysel. Ak pach alebo mastnota zostáva, problém nevyrieši ďalší parfum. Upravte "
                    "program, dávku detergentu alebo predčistenie škvrny."
                ),
            ),
        ],
        "table": {
            "headers": ["Teplota", "Typické použitie", "Na čo si dať pozor"],
            "rows": [
                ["20–30 °C", "Jemné, farebné a ľahko znečistené kúsky", "Výkon pri mastnote a hygiene"],
                ["40 °C", "Bežná bavlna a zmesi", "Výnimky na štítku a citlivé úpravy"],
                ["60 °C", "Odolné uteráky a posteľná bielizeň", "Farba, zrážanie a spotreba energie"],
                ["90 °C", "Iba odolný textil a osobitná potreba", "Vysoké tepelné zaťaženie"],
            ],
        },
        "steps": [
            "Prečítajte najnižší povolený údaj na štítkoch celej náplne.",
            "Zohľadnite materiál, farbu, škvrny a hygienickú situáciu.",
            "Vyberte detergent účinný pri zvolenom programe.",
            "Bubon naplňte tak, aby sa bielizeň mohla pohybovať.",
            "Vôňu dávkujte podľa návodu, nie podľa teploty naslepo.",
            "Bielizeň vyberte hneď a úplne usušte.",
        ],
        "expert": [
            (
                "Odborný prehľad hygieny prania opisuje mechanické, chemické a fyzikálne faktory. "
                "Teplota je dôležitá, ale výsledok nevzniká izolovane. Pri nižších teplotách môže byť "
                "významnejší správny detergent, čas a vhodná prísada."
            ),
            (
                "Zároveň nejde o odporúčanie prať všetko horúco. Domáce riziko a odolnosť materiálu "
                "sa líšia. Pri bežnej zdravej domácnosti je cieľom čistota bez zbytočného poškodzovania "
                "textilu; pri chorobe alebo kontaminácii môže byť potrebný osobitný postup."
            ),
        ],
        "sources": [
            ("Laundry Hygiene and Odor Control: State of the Science", LAUNDRY_HYGIENE),
            ("Výskum pachov v cykle pranie a sušenie", LAUNDRY_MALODOUR),
        ],
        "faq": [
            ("Pri akej teplote zostane vôňa najsilnejšia?", "Často pri šetrnejšom cykle, ale iba ak je bielizeň skutočne čistá. Najprv rešpektujte štítok a hygienickú potrebu."),
            ("Môžem všetko prať na 30 °C?", "Nie. Niektoré škvrny, odolné textílie a hygienické situácie vyžadujú iný program."),
            ("Zničí 60 °C parfum do prania?", "Môže oslabiť prchavé tóny, no výsledok závisí od výrobku. Používajte ho podľa návodu."),
            ("Je dlhý program pri nízkej teplote účinnejší?", "Čas môže zvýšiť prací účinok, no závisí to od detergentu, škvrny a programu."),
            ("Prečo bielizeň zapácha aj po teplom praní?", "Skontrolujte dávkovanie, preplnenie, práčku, pláchanie a sušenie. Teplota sama nevyrieši každý biofilm alebo zvyšok."),
        ],
        **COMMON_LAUNDRY_COMMERCE,
    },
    {
        "post_id": "1604",
        "expected_title": "Teplota prania a vôňa – Kompletný sprievodca",
        "expected_slug": "teplota-prania-a-vona-kompletny-sprievodca1",
        "title": "Pranie na 30, 40 a 60 °C: vplyv teploty na čistotu a vôňu",
        "short": (
            "Praktické porovnanie prania na 30, 40 a 60 °C podľa typu bielizne, znečistenia, "
            "hygienickej potreby, spotreby energie a výslednej vône."
        ),
        "description": (
            "Kedy prať na 30, 40 alebo 60 °C. Rozhodovacia tabuľka pre farby, uteráky, športové "
            "oblečenie, posteľnú bielizeň, škvrny a vôňu."
        ),
        "quick": (
            "Na 30 °C perte ľahko znečistené farebné a citlivejšie kúsky, na 40 °C bežnú odolnú "
            "bielizeň a na 60 °C najmä textil, ktorý túto teplotu povoľuje a potrebuje vyšší hygienický "
            "alebo odmasťovací výkon. Ak sa štítky v jednej náplni líšia, riaďte sa najcitlivejším kusom."
        ),
        "intro": (
            "Tento článok je rozhodovacia pomôcka. Odborné vysvetlenie teploty a vonných molekúl nájdete "
            "v kanonickom sprievodcovi <a href=\"/n/teplota-prania-a-vona-kompletny-sprievodca\">"
            "teplota prania a vôňa</a>. Tu vyberáme konkrétny program pre tri najbežnejšie teploty."
        ),
        "points": [
            "30 °C chráni farby a citlivejšie zmesi, ak je znečistenie bežné",
            "40 °C je praktický stred pre mnohé bavlnené a zmesové kúsky",
            "60 °C patrí iba textilu, ktorého štítok a situácia to odôvodňujú",
            "silnejšia vôňa nie je dôvod znížiť teplotu pod hygienickú potrebu",
        ],
        "sections": [
            section(
                "Kedy zvoliť 30 °C",
                (
                    "Tridsať stupňov je vhodných pre tmavé tričká, blúzky, syntetické zmesi a bežne "
                    "nosené kúsky bez odolnej mastnoty, ak to povoľuje štítok. Pomáha obmedziť blednutie "
                    "a tepelné namáhanie elastických vlákien."
                ),
                (
                    "Pri spotenom športovom textile použite dostatočne dlhý program a správny detergent. "
                    "Ak pach po zahriatí pri nosení zostáva, skúste predčistenie, menšiu náplň alebo "
                    "program odporúčaný výrobcom, nie iba viac vône."
                ),
            ),
            section(
                "Kedy zvoliť 40 °C",
                (
                    "Štyridsať stupňov sa hodí na bežnú bavlnu, spodnú bielizeň a zmesové materiály, "
                    "ktoré túto teplotu povoľujú. Je to praktický kompromis medzi ochranou textilu a "
                    "výkonom pri kožnom maze či každodennom znečistení."
                ),
                (
                    "Pri farebnej bielizni skontrolujte stálosť farieb a oddeľte nové sýte kúsky. "
                    "Pri zmesovej náplni nepozerajte iba na väčšinu; jeden citlivý kus môže vyžadovať "
                    "nižšiu teplotu alebo samostatné pranie."
                ),
            ),
            section(
                "Kedy zvoliť 60 °C",
                (
                    "Šesťdesiat stupňov využite pri odolných uterákoch, utierkach alebo posteľnej "
                    "bielizni, keď je dôležitejšia hygiena a štítok to povoľuje. Má význam aj pri "
                    "mastnejšom znečistení, no nie je vhodná pre každý materiál či farbu."
                ),
                (
                    "V domácnosti chorého človeka sa riaďte odporúčaniami pre konkrétnu situáciu. "
                    "Teplota je iba časť postupu; pomáha aj správny detergent, celý cyklus a úplné "
                    "usušenie."
                ),
            ),
            section(
                "Čo urobiť so zmiešanou náplňou",
                (
                    "Ak máte bavlnené uteráky, elastické športové kúsky a jemnú bielizeň v jednom koši, "
                    "nerozhodujte podľa pohodlia. Rozdeľte ich podľa materiálu, farby a potreby. "
                    "Získate lepší prací výkon a znížite riziko zrážania či poškodenia."
                ),
                (
                    "Menšie logické skupiny neznamenajú prať každý kus samostatne. Zbierajte podobné "
                    "textílie do primeranej náplne. Bubon potrebuje priestor, no energeticky nevýhodná "
                    "je aj takmer prázdna práčka."
                ),
            ),
            section(
                "Ako nastaviť vôňu pri rôznych teplotách",
                (
                    "Pri 30 a 40 °C začnite odporúčanou menšou dávkou. Pri 60 °C nezvyšujte parfum "
                    "automaticky. Najprv overte, či je výrobok vhodný a či sa bielizeň po vypraní a "
                    "usušení cíti čisto bez pridanej vône."
                ),
                (
                    "Ak používate parfum do prania, nalejte ho iba spôsobom uvedeným na obale. "
                    "Nedávajte koncentrovanú tekutinu priamo na suchú bielizeň a nekombinujte viac "
                    "vonných produktov bez potreby."
                ),
            ),
            section(
                "Rozhodujte podľa výsledku, nie zvyku",
                (
                    "Ak ste všetko prali na 40 °C zo zvyku, urobte kontrolované porovnanie. Ľahko "
                    "znečistené tmavé veci môžu zvládnuť 30 °C, zatiaľ čo uteráky môžu pri správnom "
                    "štítku profitovať zo samostatného 60-stupňového cyklu."
                ),
                (
                    "Sledujte čistotu golierov a podpazušia, pach po zahriatí, farbu, zrážanie a stav "
                    "elastanu. Vôňa je iba jeden ukazovateľ a môže zakryť problém, ktorý sa prejaví "
                    "neskôr."
                ),
            ),
        ],
        "table": {
            "headers": ["Náplň", "Prvá voľba", "Kedy ju zmeniť"],
            "rows": [
                ["Tmavé tričká", "30 °C", "Podľa štítku a odolného pachu"],
                ["Bežná bavlna", "40 °C", "Pri citlivej farbe alebo hygiene"],
                ["Froté uteráky", "40–60 °C", "Podľa farby, štítku a použitia"],
                ["Posteľná bielizeň", "40–60 °C", "Podľa materiálu a hygienickej potreby"],
                ["Športová syntetika", "30–40 °C", "Podľa návodu výrobcu"],
            ],
        },
        "steps": [
            "Rozdeľte bielizeň podľa materiálu, farby a hygienickej potreby.",
            "Nájdite najnižšiu povolenú teplotu na štítkoch náplne.",
            "Vyberte program, ktorý zodpovedá znečisteniu, nie iba času.",
            "Odmerajte detergent podľa tvrdosti vody.",
            "Vôňu použite iba podľa návodu a v primeranej dávke.",
            "Výsledok vyhodnoťte po úplnom usušení a pri nosení.",
        ],
        "expert": [
            (
                "Odborná literatúra upozorňuje, že kontrola mikroorganizmov pri praní závisí od "
                "mechaniky, chémie aj fyzikálnych podmienok. Vyššia teplota môže zvýšiť účinok, no "
                "nemožno ju oddeliť od detergentu a času."
            ),
            (
                "Z pohľadu vône je dôležité nepliesť si svieži pach s hygienickým výsledkom. Bielizeň "
                "môže voňať a stále niesť zvyšky nečistôt; rovnako môže byť čistá a mať iba jemnú "
                "vonnú stopu."
            ),
        ],
        "sources": [("Laundry Hygiene and Odor Control", LAUNDRY_HYGIENE)],
        "faq": [
            ("Je 30 °C dosť na spodnú bielizeň?", "Závisí od materiálu, použitia, zdravotnej situácie a pokynov na štítku. Pri vyššej hygienickej potrebe zvoľte vhodný režim."),
            ("Môžem uteráky vždy prať na 60 °C?", "Iba ak to povoľuje ich štítok a farba. Nie každý uterák potrebuje taký cyklus pri každom praní."),
            ("Ktorá teplota najviac šetrí farbu?", "Nižšia povolená teplota spravidla znižuje tepelné namáhanie, no dôležité je aj triedenie a detergent bez bielidiel."),
            ("Mám pri 60 °C pridať viac parfumu?", "Nie automaticky. Dodržte návod a najprv overte čistotu, pláchanie a sušenie."),
            ("Čo ak sa pri 30 °C nevyperie pach?", "Znížte náplň, zvoľte vhodnejší program alebo predčistenie a skontrolujte práčku. Vôňou pach neprekryte."),
        ],
        **COMMON_LAUNDRY_COMMERCE,
    },
    {
        "post_id": "1605",
        "expected_title": "Sušička a vône – Ako zachovať vôňu po sušení",
        "expected_slug": "susicka-a-vone-ako-zachovat-vonu-po-suseni",
        "title": "Sušička a vône – Ako zachovať vôňu po sušení",
        "short": (
            "Teplo, prúdenie vzduchu a presušenie môžu zmeniť vonnú stopu bielizne. "
            "Naučte sa nastaviť sušičku, čistotu filtra a vôňu určenú priamo na sušenie."
        ),
        "description": (
            "Ako zachovať príjemnú vôňu bielizne po sušičke: správny program, nepresušiť, "
            "čistiť filter a používať iba výrobok určený do sušičky."
        ),
        "quick": (
            "Vôňu po sušení najviac chráni správne vypraná bielizeň, najnižšia účinná teplota podľa "
            "štítku, ukončenie cyklu bez zbytočného presušenia a čistý filter. Do sušičky používajte "
            "iba výrobok určený na tento účel a presne spôsobom uvedeným v návode."
        ),
        "intro": (
            "Sušička odstraňuje vodu teplom a prúdením vzduchu. Rovnaké podmienky odnášajú aj časť "
            "prchavých vonných zložiek. Niektoré tóny zoslabnú, iné sa prejavia až pri zahriatí textilu. "
            "Cieľom nie je čo najsilnejšia vôňa, ale čistá bielizeň s primeranou a stabilnou stopou."
        ),
        "points": [
            "presušenie môže oslabiť vôňu aj zbytočne namáhať vlákna",
            "upchatý filter predlžuje cyklus a zhoršuje prúdenie vzduchu",
            "vôňa do prania a vôňa do sušičky nie sú automaticky zameniteľné",
            "na vlnenej guli používajte iba výrobok a množstvo určené výrobcom",
        ],
        "sections": [
            section(
                "Prečo sa vôňa v sušičke mení",
                (
                    "Vonné zložky majú rôznu prchavosť. Horúci vzduch uprednostní tie, ktoré sa "
                    "odparujú rýchlejšie, a výsledný pomer tónov sa môže posunúť. Svieži úvod často "
                    "zoslabne skôr než drevitý alebo pižmový základ."
                ),
                (
                    "To neznamená, že vyššia teplota vždy úplne odstráni vôňu. Rozhoduje receptúra, "
                    "množstvo vody, dĺžka cyklu, materiál a spôsob aplikácie. Preto sa riaďte výrobkom "
                    "určeným pre konkrétny proces."
                ),
            ),
            section(
                "Najnižšia účinná teplota podľa štítku",
                (
                    "Vyberte program podľa najcitlivejšieho kusu v náplni. Syntetika, elastan a jemné "
                    "materiály často vyžadujú nižšiu teplotu než odolná bavlna. Automatický senzorický "
                    "program môže cyklus ukončiť skôr než pevne nastavený dlhý čas."
                ),
                (
                    "Nižšia teplota môže znížiť stratu prchavých tónov, no bielizeň musí byť úplne suchá. "
                    "Ak ostane vlhká a dlho leží v koši, vznikajú podmienky pre zatuchnutý pach. "
                    "Nesnažte sa preto sušenie skrátiť za každú cenu."
                ),
            ),
            section(
                "Ako spoznať presušenie",
                (
                    "Presušená bielizeň je veľmi horúca, statická, pokrčená a niekedy drsnejšia. "
                    "Cyklus pokračoval aj po odvedení potrebnej vlhkosti. Tým sa zvyšuje mechanické "
                    "a tepelné namáhanie aj čas, počas ktorého prúdi cez textil horúci vzduch."
                ),
                (
                    "Použite senzorový program, správnu veľkosť náplne a cieľovú úroveň suchosti. "
                    "Bielizeň po skončení vyberte a nechajte krátko vychladnúť rozloženú, nie stlačenú "
                    "v bubne."
                ),
            ),
            section(
                "Filter, kondenzátor a čistota spotrebiča",
                (
                    "Zanesený filter znižuje prietok vzduchu, predlžuje sušenie a zvyšuje energetickú "
                    "náročnosť. Čistite ho podľa návodu po príslušnom počte cyklov. Kondenzátor alebo "
                    "výmenník udržiavajte spôsobom, ktorý uvádza výrobca spotrebiča."
                ),
                (
                    "Ak sušička sama zapácha, parfum problém nevyrieši. Skontrolujte nádrž na vodu, "
                    "tesnenie, priestor filtra a vetranie. Zápach môže pochádzať aj z nedostatočne "
                    "vypranej bielizne, ktorá sa pri teple znovu rozvonia nepríjemným smerom."
                ),
            ),
            section(
                "Vôňa určená do sušičky",
                (
                    "Výrobok do sušičky má vlastný spôsob aplikácie, napríklad na kompatibilnú vlnenú "
                    "guľu. Nepoužívajte ľubovoľný esenciálny olej, interiérový sprej ani prací parfum "
                    "mimo jeho návodu. Horľavosť, škvrny a poškodenie spotrebiča sú zbytočné riziká."
                ),
                (
                    "Pozrite si <a href=\"/c/vevo-fragrance/parfum-do-susicky\">parfumy do sušičky</a> "
                    "a pri každom produkte skontrolujte dávku. Guľu alebo aplikátor nechajte nasiaknuť "
                    "presne podľa pokynov a neprikladajte mokrý koncentrát priamo na jemný textil."
                ),
            ),
            section(
                "Ako porovnať výsledok po sušení",
                (
                    "Vôňu nehodnoťte pri otvorení horúceho bubna. Nechajte bielizeň vychladnúť, potom "
                    "ju skontrolujte zblízka aj pri pohybe. Znovu ju posúďte po dni v skrini, pretože "
                    "časť prchavého úvodu prirodzene ustúpi."
                ),
                (
                    "Ak chcete upraviť výsledok, zmeňte v ďalšom cykle iba jednu vec: teplotu, úroveň "
                    "suchosti alebo dávku výrobku do sušičky. Tak zistíte, čo má reálny vplyv a "
                    "nebudete zvyšovať spotrebu bez výsledku."
                ),
            ),
        ],
        "table": {
            "headers": ["Problém", "Možná príčina", "Prvý krok"],
            "rows": [
                ["Vôňa úplne zmizla", "Dlhý horúci cyklus", "Nižšia účinná teplota"],
                ["Bielizeň je zatuchnutá", "Slabé pranie alebo vlhké státie", "Skontrolovať celý cyklus"],
                ["Textil je veľmi statický", "Presušenie a materiál", "Senzorový program"],
                ["Sušička zapácha", "Filter, voda alebo tesnenie", "Údržba podľa návodu"],
                ["Na textile je škvrna", "Nesprávna priama aplikácia", "Používať určený aplikátor"],
            ],
        },
        "steps": [
            "Skontrolujte symbol sušenia na každom kuse.",
            "Rozdeľte hrubú bavlnu a citlivú syntetiku.",
            "Vyčistite filter a nastavte primeranú náplň.",
            "Zvoľte najnižšiu účinnú teplotu a senzorovú suchosť.",
            "Vôňu do sušičky aplikujte iba podľa návodu.",
            "Po skončení bielizeň vyberte, nechajte vychladnúť a až potom hodnoťte.",
        ],
        "expert": [
            (
                "Novší výskum cyklu nosenie–pranie–sušenie ukazuje, že vlhké podmienky sušenia "
                "súvisia s typickými pachmi a rastom vybraných baktérií. Úplné a včasné usušenie je "
                "preto dôležitejšie než snaha za každú cenu zachovať maximálnu intenzitu parfumu."
            ),
            (
                "Z pohľadu prchavých látok je teplo iba jedna premenná. Výsledok mení aj prúdenie "
                "vzduchu, čas a povrch vlákna. Preto má zmysel hodnotiť konkrétny program a hotový "
                "výrobok namiesto všeobecného sľubu, že každá vôňa vydrží sušičku."
            ),
        ],
        "sources": [("Výskum pachov a mikrobiomu v pracom cykle", LAUNDRY_MALODOUR)],
        "faq": [
            ("Môžem dať parfum do prania priamo do sušičky?", "Iba ak to výslovne povoľuje návod daného výrobku. Inak použite produkt určený do sušičky."),
            ("Prečo bielizeň vonia po praní, ale nie po sušení?", "Teplo, prúdenie vzduchu a presušenie môžu odstrániť prchavé tóny."),
            ("Pomôže nižšia teplota?", "Často áno, ak bielizeň úplne vysuší a štítok ju povoľuje."),
            ("Ako často čistiť filter?", "Riaďte sa návodom spotrebiča; pri bežných modeloch sa kontroluje veľmi často, často po každom cykle."),
            ("Môžem použiť esenciálny olej na guľu?", "Nie bez výslovného povolenia výrobcu gule, oleja aj sušičky. Použite riešenie určené na tento účel."),
        ],
        **DRYER_COMMERCE,
    },
    {
        "post_id": "1602",
        "expected_title": "Sušička a vône – Ako zachovať vôňu po sušení",
        "expected_slug": "susicka-a-vone-ako-zachovat-vonu-po-suseni1",
        "title": "Prečo vôňa po sušičke slabne: presušenie, teplota a správny program",
        "short": (
            "Diagnostický návod pre bielizeň, ktorá po praní vonia, ale po sušičke nie. "
            "Skontrolujte teplotu, čas, filter, náplň, zvyškovú vlhkosť a spôsob aplikácie vône."
        ),
        "description": (
            "Prečo bielizeň po sušičke prestane voňať a ako krok za krokom odhaliť presušenie, "
            "nesprávny program, zanesený filter alebo nevhodnú aplikáciu vône."
        ),
        "quick": (
            "Ak vôňa zmizne až v sušičke, porovnajte jednu rovnakú náplň usušenú na nižšej povolenej "
            "teplote a bez zbytočného dosúšania. Vyčistite filter, nepreplňte bubon a hodnoťte bielizeň "
            "po vychladnutí. Ak chcete pridať vôňu počas sušenia, použite iba produkt určený do sušičky."
        ),
        "intro": (
            "Tento článok je diagnostický. Všeobecný postup nájdete v kanonickej téme "
            "<a href=\"/n/susicka-a-vone-ako-zachovat-vonu-po-suseni\">sušička a vône</a>. "
            "Tu riešime konkrétnu situáciu: mokrá bielizeň po praní vonia, no po skončení cyklu je "
            "vôňa slabá, zmenená alebo ju prekryl teplý zatuchnutý tón."
        ),
        "points": [
            "najprv overte, či problém vzniká naozaj v sušičke",
            "horúca bielizeň sa hodnotí inak než vychladnutý textil",
            "presušenie a zanesený filter často pôsobia súčasne",
            "vôňu nepridávajte neovereným olejom priamo na bielizeň",
        ],
        "sections": [
            section(
                "Kontrolný test: sušička alebo pranie?",
                (
                    "Z rovnakej náplne usušte jeden malý kus na vzduchu a zvyšok v sušičke. Po úplnom "
                    "vychladnutí ich porovnajte. Ak oba zapáchajú alebo sú bez vône, problém vznikol "
                    "už pri praní. Ak rozdiel nastal iba v sušičke, pokračujte programom a údržbou."
                ),
                (
                    "Test má zmysel len pri podobných materiáloch. Hrubý uterák a tenká syntetika "
                    "zadržiavajú vodu aj vôňu inak. Vyberte dva bavlnené kusy alebo dve tričká rovnakej "
                    "konštrukcie."
                ),
            ),
            section(
                "Presušenie krok za krokom",
                (
                    "Skontrolujte, či nastavujete čas ručne a pridávate rezervu. Pri senzorovom programe "
                    "overte čistotu senzorov podľa návodu spotrebiča. Veľmi malá alebo zmiešaná náplň "
                    "môže snímač vyhodnotiť nepresne."
                ),
                (
                    "V ďalšom cykle zvoľte nižšiu cieľovú suchosť, ak je bielizeň určená na okamžité "
                    "odloženie alebo žehlenie. Nesmie však ostať vlhká v uzavretom koši. Po skončení "
                    "ju rozložte a nechajte odísť zvyškové teplo."
                ),
            ),
            section(
                "Príliš vysoká teplota pre danú náplň",
                (
                    "Program na bavlnu môže byť nevhodný pre syntetické alebo elastické kúsky. Vyššia "
                    "teplota urýchli odparovanie ľahkých zložiek a môže poškodiť textil. Skontrolujte "
                    "symboly a rozdeľte náplň podľa najnižšej povolenej teploty."
                ),
                (
                    "Ak prejdete na šetrnejší program, sledujte aj dĺžku. Niektoré tepelné čerpadlá "
                    "sušia nižšie, ale dlhšie. Výsledok preto porovnajte v praxi, nie iba podľa názvu "
                    "programu."
                ),
            ),
            section(
                "Filter a prúdenie vzduchu",
                (
                    "Chlpy a textilný prach vo filtri obmedzujú prúdenie. Spotrebič potrebuje dlhší "
                    "čas a bielizeň je viac vystavená teplu. Filter vyčistite podľa návodu a skontrolujte, "
                    "či správne sedí; neprevádzkujte sušičku bez neho."
                ),
                (
                    "Ak má model prístupný výmenník alebo kondenzátor, postupujte presne podľa manuálu. "
                    "Nestriekajte dovnútra ľubovoľný čistič či parfum. Zvyšky môžu poškodiť komponenty "
                    "alebo sa dostať späť na textil."
                ),
            ),
            section(
                "Zvyškový pach, ktorý teplo zvýrazní",
                (
                    "Teplo môže uvoľniť kožný maz a pachy, ktoré pranie neodstránilo. Často to vidno pri "
                    "polyesteri, športových veciach a utierkach. Riešením je lepší prací základ, menšia "
                    "náplň, vhodný detergent a úplné pláchanie."
                ),
                (
                    "Parfum nie je dezodorant na biofilm. Ak sa nepríjemný pach objaví pri každom zahriatí, "
                    "vyčistite práčku podľa návodu a venujte sa konkrétnemu materiálu. Nový parfum pridajte "
                    "až po vyriešení príčiny."
                ),
            ),
            section(
                "Kedy pridať vôňu do sušičky",
                (
                    "Až keď je bielizeň správne vypraná a spotrebič čistý. Vyberte produkt z kategórie "
                    "<a href=\"/c/vevo-fragrance/parfum-do-susicky\">parfum do sušičky</a> a prečítajte "
                    "spôsob aplikácie. Dodržte množstvo aj čas potrebný na vsiaknutie."
                ),
                (
                    "Pri prvej skúške použite menšiu odporúčanú dávku a jednu bežnú náplň. Výsledok "
                    "posúďte po vychladnutí a na ďalší deň. Ak je vôňa príliš výrazná, znížte dávku "
                    "namiesto skracovania sušenia pod bezpečnú úroveň."
                ),
            ),
        ],
        "table": {
            "headers": ["Kontrola", "Dobré znamenie", "Rizikové znamenie"],
            "rows": [
                ["Program", "Zodpovedá štítku", "Univerzálny horúci cyklus na všetko"],
                ["Suchosť", "Cyklus končí bez prehriatia", "Bielizeň je pálivá a statická"],
                ["Filter", "Čistý a správne osadený", "Vrstva chlpov a prachu"],
                ["Pach", "Čistý aj pri zahriatí", "Pot alebo zatuchnutie sa vracia"],
                ["Aplikácia vône", "Výrobok určený do sušičky", "Neznámy olej priamo na textil"],
            ],
        },
        "steps": [
            "Porovnajte vzduchom a strojovo usušený kus z rovnakej náplne.",
            "Vyčistite filter a skontrolujte pokyny k údržbe.",
            "Rozdeľte bavlnu a syntetiku.",
            "Znížte teplotu alebo cieľovú suchosť v rámci štítku.",
            "Po cykle nechajte bielizeň vychladnúť a až potom hodnoťte.",
            "Vôňu do sušičky skúšajte až po odstránení technickej príčiny.",
        ],
        "expert": [
            (
                "Vlhké a pomalé sušenie podporuje pachy, no zbytočne horúce presušenie odnáša prchavé "
                "zložky a namáha textil. Optimálny výsledok je medzi týmito extrémami: úplne suchá "
                "bielizeň pri primeranej teplote a čase."
            ),
            (
                "Výskum textilného mikrobiomu zdôrazňuje, že výsledný pach je produktom celého cyklu. "
                "Diagnostika preto začína praním a pokračuje sušením; samotná intenzita vonného výrobku "
                "je až posledná premenná."
            ),
        ],
        "sources": [("Výskum pachov a mikrobiomu v pracom cykle", LAUNDRY_MALODOUR)],
        "faq": [
            ("Ako zistím, že bielizeň presušujem?", "Je neprimerane horúca, statická, pokrčená a cyklus pokračuje dlho po strate vlhkosti."),
            ("Môže zanesený filter oslabiť vôňu?", "Nepriamo áno, pretože zhorší prúdenie a predĺži vystavenie teplu."),
            ("Prečo syntetika po sušení zapácha?", "Kožný maz a mikrobiálne zvyšky sa môžu pri teple zvýrazniť. Skontrolujte pranie určené pre syntetiku."),
            ("Môžem nastriekať interiérový parfum do bubna?", "Nie. Používajte iba výrobok určený do sušičky a podľa jeho návodu."),
            ("Kedy vôňu hodnotiť?", "Po úplnom vychladnutí bielizne a znova po jednom dni."),
        ],
        **DRYER_COMMERCE,
    },
    {
        "post_id": "1603",
        "expected_title": "Citrusové vône – Sviežosť a energia pre vaše prádlo",
        "expected_slug": "citrusove-vone-sviezost-a-energia-pre-vase-pradlo",
        "title": "Citrusové vône – Sviežosť a energia pre vaše prádlo",
        "short": (
            "Citrón, bergamot, pomaranč, mandarínka a grep patria do jednej rodiny, no voňajú odlišne. "
            "Spoznajte ich prchavosť, kombinácie, skladovanie a použitie na bielizni."
        ),
        "description": (
            "Kompletný sprievodca citrusovými vôňami: citrón, bergamot, pomaranč, mandarínka, grep, "
            "vrchné tóny, výdrž, kombinácie a bezpečné používanie."
        ),
        "quick": (
            "Citrusové vône pôsobia sviežo najmä v úvode kompozície, pretože mnohé ich zložky sú "
            "prchavé. Citrón býva ostrejší, pomaranč sladší, bergamot elegantne aromatický, grep "
            "trpkejší a mandarínka mäkšia. O výdrži rozhoduje nielen citrus, ale aj srdce, základ, "
            "textil, teplota a sušenie."
        ),
        "intro": (
            "Citrusová rodina nie je jedna vôňa. Dva produkty môžu mať v názve citrus a pritom pôsobiť "
            "úplne odlišne. Jeden pripomína čistú kôru a čerstvý vzduch, druhý sladkú šťavu a tretí "
            "horkastý kolínsky akord. Preto má zmysel poznať jednotlivé suroviny a ich partnerov."
        ),
        "points": [
            "citrusy sú často vrchné tóny a prirodzene ustupujú skôr než základ",
            "bergamot, citrón, pomaranč, grep a mandarínka nie sú zameniteľné",
            "dobrý základ predĺži dojem bez toho, aby citrus musel zostať dominantný",
            "obal chráňte pred teplom, svetlom a zbytočným prístupom vzduchu",
        ],
        "sections": [
            section(
                "Prečo citrusy pôsobia ako čistota",
                (
                    "Citrusové šupky uvoľňujú jasné prchavé látky, ktoré si spájame s čerstvosťou, "
                    "kuchyňou a čistením. Parfuméria tento kultúrny signál využíva v kolínskych, "
                    "športových, čajových aj bavlnených kompozíciách."
                ),
                (
                    "Pocit čistoty však nevzniká iba citrónom. Často ho podporujú aldehydické, zelené, "
                    "mydlové a pižmové tóny. Výsledok je akord, ktorý pôsobí čisto aj po tom, čo "
                    "najprchavejší citrus ustúpi."
                ),
            ),
            section(
                "Citrón, limetka a grep",
                (
                    "Citrón býva jasný, kyslastý a priamočiary. Limetka môže pôsobiť zelenšie a ostrejšie. "
                    "Grep pridáva horkastý, suchší a moderný charakter. Tieto tóny sa hodia k športovým, "
                    "bylinkovým a vzdušným kompozíciám."
                ),
                (
                    "Ak vám čistý citrón pripomína čistiaci prostriedok, hľadajte kompozíciu, v ktorej "
                    "je zjemnený kvetinovým srdcom alebo čajovým akordom. Grep s drevitým základom môže "
                    "pôsobiť dospelejšie a menej sladko."
                ),
            ),
            section(
                "Pomaranč, mandarínka a bergamot",
                (
                    "Pomaranč prináša sladší a šťavnatejší dojem, mandarínka býva mäkká a hravá. "
                    "Bergamot má citrusový základ, ale aj aromatický, jemne horký a čajový charakter. "
                    "Preto je tradičnou súčasťou elegantných kolínskych kompozícií."
                ),
                (
                    "Na bielizni môžu mäkšie citrusy pôsobiť prívetivejšie v spálni, kým grep alebo "
                    "bergamot vytvoria čistejší unisex dojem. Nejde o pravidlo pre pohlavie, ale o "
                    "charakter, ktorý si môžete vybrať podľa priestoru a textilu."
                ),
            ),
            section(
                "Ako sa citrus predlžuje v kompozícii",
                (
                    "Parfumér môže citrus podporiť kvetinovým srdcom, aromatickými bylinami, drevom, "
                    "pižmom alebo ambrovými materiálmi. Cieľom nie je zastaviť prirodzený vývoj, ale "
                    "zachovať svieži dojem aj v neskorších fázach."
                ),
                (
                    "Pri bielizni výsledok ovplyvní vlákno a sušenie. Ak citrusový úvod po sušičke "
                    "zoslabne, nemusí to byť chyba produktu. Sledujte, či ostal čistý základ, ktorý je "
                    "pri nosení príjemný a nerušivý."
                ),
            ),
            section(
                "Oxidácia a správne skladovanie",
                (
                    "Limonén a linalool patria medzi bežné terpény. Pri dlhom kontakte so vzduchom "
                    "môžu oxidovať a zmeniť vlastnosti. Výrobok preto zatvárajte, neprelievajte do "
                    "otvorenej dekoratívnej nádoby a nenechávajte pri okne či radiátore."
                ),
                (
                    "Výskum na koži ukázal vyššiu dráždivosť oxidovaných foriem oproti čistým terpénom. "
                    "Tento údaj neznamená, že každý citrusový prací výrobok spôsobí problém. Zdôrazňuje "
                    "význam správnej formulácie, skladovania, etikety a odporúčanej dávky."
                ),
            ),
            section(
                "Ako si citrusovú vôňu vyskúšať",
                (
                    "Porovnajte dve kompozície na rovnakom textile. Hodnoťte prvý dojem po usušení, "
                    "potom stopu na ďalší deň. Zapíšte si, či ostala čistota, sladkosť, horkosť alebo "
                    "drevitý základ. Tak sa vyhnete výberu iba podľa názvu ovocia."
                ),
                (
                    "Ak neviete, či vám viac vyhovuje jasný alebo mäkký citrus, začnite vzorkami. "
                    "Jedna celá fľaša vybraná podľa prvých sekúnd môže byť menej praktická než malé "
                    "porovnanie v reálnom praní."
                ),
            ),
        ],
        "table": {
            "headers": ["Citrus", "Typický dojem", "Časté spojenie"],
            "rows": [
                ["Citrón", "Jasný, kyslastý, čistý", "Bylinky, bavlna, ľahké drevo"],
                ["Bergamot", "Elegantný, čajový, aromatický", "Kvety, čaj, drevo"],
                ["Pomaranč", "Sladší a šťavnatý", "Kvety, korenie, vanilka"],
                ["Grep", "Horkastý a moderný", "Zelené tóny, drevo, pižmo"],
                ["Mandarínka", "Mäkká a hravá", "Jemné kvety a ovocie"],
            ],
        },
        "steps": [
            "Rozhodnite sa, či chcete ostrú, sladkú, horkú alebo čajovú sviežosť.",
            "Porovnajte vzorky pri rovnakej dávke a programe.",
            "Hodnoťte vôňu po usušení aj na ďalší deň.",
            "Pri slabej výdrži najprv skontrolujte sušenie a čistotu bielizne.",
            "Obal zatvárajte a skladujte mimo svetla a tepla.",
            "Pri citlivosti použite menšiu dávku a riaďte sa etiketou.",
        ],
        "expert": [
            (
                "Limonén a linalool na vzduchu oxidujú. Experimentálna štúdia ukázala, že oxidované "
                "materiály boli dráždivejšie než neoxidované. V praxi to podporuje správne skladovanie "
                "a používanie hotových formulácií, nie strach zo všetkých citrusových vôní."
            ),
            (
                "IFRA štandardy môžu pre konkrétne citrusové materiály určovať obmedzenia alebo "
                "požiadavky. Zodpovednosť za bezpečnú formuláciu má výrobca; spotrebiteľ má dodržať "
                "určený spôsob aplikácie."
            ),
        ],
        "sources": [
            ("Štúdia oxidovaného limonénu a linaloolu", LIMONENE_STUDY),
            ("IFRA štandardy", IFRA_STANDARDS),
        ],
        "faq": [
            ("Prečo citrusová vôňa rýchlo zoslabne?", "Mnohé citrusové zložky sú prchavé vrchné tóny. Dlhší dojem vytvára celá kompozícia a jej základ."),
            ("Je bergamot to isté ako citrón?", "Nie. Bergamot má aromatický a čajový charakter, kým citrón býva ostrejší a priamejší."),
            ("Sú citrusové vône iba letné?", "Nie. S drevom, korením alebo pižmom môžu fungovať celoročne."),
            ("Môžem citrusový olej naliať do práčky?", "Nie bez výslovného návodu výrobcu. Použite hotový prací výrobok."),
            ("Ako skladovať citrusový parfum?", "Pevne uzavretý, mimo priameho svetla, tepla a veľkých teplotných zmien."),
        ],
        **COMMON_FRAGRANCE_COMMERCE,
    },
    {
        "post_id": "1601",
        "expected_title": "Citrusové vône – Sviežosť a energia pre vaše prádlo",
        "expected_slug": "citrusove-vone-sviezost-a-energia-pre-vase-pradlo1",
        "title": "Citrón, bergamot, grep a mandarínka: ako sa líšia citrusové vône",
        "short": (
            "Detailné porovnanie citrusových tónov bez všeobecných skratiek. Zistite, ktorý citrus "
            "pôsobí ostro, sladko, horkasto alebo čajovo a s čím ho kombinovať."
        ),
        "description": (
            "Citrón vs. bergamot vs. grep vs. mandarínka: rozdiely vo vôni, prchavosti, kombináciách "
            "a vhodnosti pre bielizeň, domácnosť a rôzne ročné obdobia."
        ),
        "quick": (
            "Citrón je najpriamejšie čistý a kyslastý, bergamot elegantne čajový, grep horkastý a "
            "suchší, mandarínka mäkká a sladšia. Pri výbere nesledujte iba ovocie v názve. Dôležité "
            "je kvetinové srdce, drevitý či pižmový základ a to, ako kompozícia pôsobí po usušení."
        ),
        "intro": (
            "Všeobecný slovníkový prehľad nájdete v článku "
            "<a href=\"/n/citrusove-vone-sviezost-a-energia-pre-vase-pradlo\">citrusové vône</a>. "
            "Tento článok je porovnávací katalóg najčastejších citrusových charakterov a pomôže "
            "vybrať konkrétny smer bez očakávania, že každý citrus bude voňať rovnako."
        ),
        "points": [
            "citrón a limetka prinášajú najostrejší pocit čistoty",
            "bergamot spája citrus s čajovou a aromatickou eleganciou",
            "grep je horkastejší a dobre funguje v suchých unisex kompozíciách",
            "mandarínka a pomaranč zjemňujú kompozíciu sladším dojmom",
        ],
        "sections": [
            section(
                "Citrón: jasná čistota bez zbytočnej sladkosti",
                (
                    "Citrónový tón je ľahko rozpoznateľný, ostrý a energický. V pracích kompozíciách "
                    "vie zdôrazniť pocit čerstvo vypranej bielizne. Ak je použitý bez mäkšieho srdca, "
                    "môže niekomu pripomínať kuchynský čistič."
                ),
                (
                    "Hľadajte spojenie s bavlneným, kvetinovým alebo jemne drevitým akordom. Tak ostane "
                    "svieži signál, ale výsledok nebude jednorozmerný. Citrón sa hodí na uteráky, "
                    "košele a bežnú dennú bielizeň, ak vám vyhovuje jasný profil."
                ),
            ),
            section(
                "Bergamot: citrus s čajovým charakterom",
                (
                    "Bergamot je základom mnohých kolínskych kompozícií a arómy čaju Earl Grey. "
                    "Okrem sviežosti má jemne horkú, zelenú a aromatickú stránku. Preto pôsobí "
                    "elegantnejšie a menej doslovne ovocne než sladký pomaranč."
                ),
                (
                    "Dobre sa spája s levanduľou, čajom, bielymi kvetmi, cédrom a pižmom. Na bielizni "
                    "môže vytvoriť čistý unisex dojem vhodný aj do spálne či šatníka, ak je intenzita "
                    "primeraná."
                ),
            ),
            section(
                "Grep: horkastá a moderná sviežosť",
                (
                    "Grep pridáva trpkosť, suchosť a mierne zelený dojem. Menej pripomína sladký džús "
                    "a viac modernú športovú alebo drevitú vôňu. V kombinácii s vetiverom, cédrom alebo "
                    "aromatickými bylinami pôsobí čisto bez cukrovej sladkosti."
                ),
                (
                    "Je dobrou voľbou pre človeka, ktorému pomaranč pripadá príliš sladký. Pri praní "
                    "ho hodnoťte na suchej bielizni, pretože horkastý vrchný tón môže po usušení "
                    "ustúpiť a nechať výraznejší drevitý základ."
                ),
            ),
            section(
                "Mandarínka a pomaranč: mäkkosť a optimizmus",
                (
                    "Mandarínka býva jemnejšia, šťavnatejšia a hravejšia. Sladký pomaranč prináša "
                    "teplejší a plnší ovocný dojem. Obe suroviny zjemnia ostrý citrusový úvod a dobre "
                    "fungujú s kvetmi, korením alebo vanilkovým náznakom."
                ),
                (
                    "Na posteľnej bielizni môžu pôsobiť útulnejšie než čistý citrón. Pri vyššej dávke "
                    "však sladkosť zosilnie, preto začnite opatrne. Do kuchynských textílií zas nemusí "
                    "byť sladký profil ideálny, ak chcete úplne neutrálny pocit čistoty."
                ),
            ),
            section(
                "Limetka a petitgrain ako zelené alternatívy",
                (
                    "Limetka je prenikavejšia a zelenšia, petitgrain sa získava z listov a vetvičiek "
                    "horkého pomarančovníka a prináša listový, drevitejší charakter. Pomáhajú vytvoriť "
                    "sviežosť, ktorá nepôsobí ako sladké ovocie."
                ),
                (
                    "Tieto tóny sa dobre kombinujú s bylinkami, levanduľou a suchým drevom. Ak hľadáte "
                    "vôňu do pracovného oblečenia alebo uterákov, zelený citrusový smer môže pôsobiť "
                    "praktickejšie než dezertná sladkosť."
                ),
            ),
            section(
                "Ako vybrať citrus podľa miestnosti a textilu",
                (
                    "Do spálne voľte mäkší bergamot alebo mandarínku s jemným základom. Na uteráky sa "
                    "hodí jasný citrón či grep, ak bielizeň zostáva skutočne čistá. Pri oblečení "
                    "vyberajte podľa toho, či chcete energický športový alebo elegantný čajový dojem."
                ),
                (
                    "V každom prípade porovnávajte pri rovnakej dávke. Názov ovocia je iba orientácia; "
                    "celú vôňu môže výrazne zmeniť srdce a základ. Vzorkovanie je presnejšie než výber "
                    "podľa farby obalu."
                ),
            ),
        ],
        "table": {
            "headers": ["Tón", "Intenzita úvodu", "Charakter", "Vhodné spojenie"],
            "rows": [
                ["Citrón", "Jasná", "Kyslastý a čistý", "Bavlna, bylinky, ľahké drevo"],
                ["Bergamot", "Stredná", "Čajový a elegantný", "Kvety, levanduľa, céder"],
                ["Grep", "Jasná", "Horkastý a suchý", "Vetiver, zelené tóny, pižmo"],
                ["Mandarínka", "Mäkká", "Sladšia a hravá", "Jemné kvety, ovocie"],
                ["Pomaranč", "Plnšia", "Teplý a šťavnatý", "Korenie, kvety, vanilka"],
            ],
        },
        "steps": [
            "Pomenujte, či chcete ostrú, čajovú, horkú alebo sladšiu sviežosť.",
            "Vyberte dve vzorky s odlišným citrusovým smerom.",
            "Použite rovnakú náplň, program a dávku.",
            "Hodnoťte úvod po usušení aj základ na ďalší deň.",
            "Všímajte si, či vám vôňa vyhovuje pri dlhšom nosení.",
            "Obľúbený výrobok skladujte uzavretý a mimo tepla.",
        ],
        "expert": [
            (
                "Citrusový profil často obsahuje limonén a ďalšie terpény, ktoré podliehajú oxidácii. "
                "Štúdie kožnej expozície ukazujú rozdiel medzi čerstvými a oxidovanými formami. "
                "Preto má význam obal pevne zatvárať a rešpektovať dobu použiteľnosti."
            ),
            (
                "IFRA zároveň posudzuje konkrétne materiály a aplikácie. Domáce porovnanie má zostať "
                "pri hotových výrobkoch; čisté éterické oleje nie sú automatickou náhradou parfumu "
                "určeného do prania."
            ),
        ],
        "sources": [
            ("Štúdia oxidovaného limonénu a linaloolu", LIMONENE_STUDY),
            ("IFRA Code of Practice", IFRA_STANDARDS),
        ],
        "faq": [
            ("Ktorý citrus je najmenej sladký?", "Grep, limetka a niektoré bergamotové kompozície bývajú suchšie než pomaranč alebo mandarínka."),
            ("Ktorý citrus pôsobí najelegantnejšie?", "Bergamot často pôsobí čajovo a aromaticky, no rozhoduje celá kompozícia."),
            ("Prečo citrón pripomína čistiaci prostriedok?", "Je silno spojený s čistotou a v jednoduchej kompozícii môže pôsobiť doslovne. Kvetinové či drevité srdce ho zjemní."),
            ("Je mandarínka vhodná do spálne?", "Môže byť, ak je kompozícia jemná a dávka nízka. Rozhoduje osobná citlivosť."),
            ("Ako predĺžiť citrusový dojem?", "Vyberte vyváženú kompozíciu s vhodným srdcom a základom a správne ju skladujte."),
        ],
        **COMMON_FRAGRANCE_COMMERCE,
    },
]


HIDE_ACTION = {
    "post_id": "1520",
    "expected_title": "Ako správne vyžehliť záclonu – Kompletný sprievodca",
    "expected_slug": "111111111111111111",
    "canonical_post_id": "1682",
    "canonical_slug": "ako-spravne-vyzehlit-zaclonu-kompletny-sprievodca",
}


def render_table(table: dict[str, Any]) -> str:
    headers = "".join(
        f'<th style="border:1px solid #e5e5e5;padding:10px;text-align:left;">{html.escape(value)}</th>'
        for value in table["headers"]
    )
    rows = []
    for row in table["rows"]:
        cells = "".join(
            f'<td style="border:1px solid #e5e5e5;padding:10px;vertical-align:top;">{html.escape(value)}</td>'
            for value in row
        )
        rows.append(f"<tr>{cells}</tr>")
    return (
        '<table style="width:100%;border-collapse:collapse;margin:20px 0;">'
        f"<thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>"
    )


def render_verification_protocol(config: dict[str, Any]) -> str:
    topic = html.escape(config["title"])
    first_step = html.escape(config["steps"][0])
    last_step = html.escape(config["steps"][-1])
    key_point = html.escape(config["points"][0])
    return f"""
<div style="border:1px solid #eadfce;border-radius:8px;padding:18px;margin:24px 0;background:#fffdf9;">
<h2 style="margin-top:0;">Ako si výsledok overiť v domácej praxi</h2>
<p>Pri téme <strong>{topic}</strong> má väčšiu hodnotu porovnateľný malý test než náhodná zmena viacerých vecí naraz. Zvoľte jednu bežnú náplň, materiál alebo vôňu a zapíšte si východiskový stav. Začnite krokom: {first_step} Počas skúšky nemeňte súčasne dávku, program, teplotu aj spôsob sušenia. Inak nebude jasné, čo výsledok zlepšilo alebo zhoršilo.</p>
<p>Pri hodnotení myslite najmä na zásadu, že {key_point}. Výsledok posudzujte až v stave, v ktorom výrobok alebo textíliu reálne používate, nie iba bezprostredne po otvorení balenia či vybratí z bubna. Do poznámky pridajte typ materiálu alebo výrobku, veľkosť náplne a podmienky po skončení; práve tieto premenné často vysvetlia rozdiel medzi dvoma skúškami. Skúšku uzavrite krokom: {last_step} Ak sa výsledok zopakuje aspoň pri dvoch porovnateľných cykloch alebo skúškach, máte podstatne spoľahlivejší podklad pre ďalšie rozhodnutie.</p>
</div>
""".strip()


def render_decision_framework(config: dict[str, Any]) -> str:
    topic = html.escape(config["title"])
    labels = [html.escape(str(row[0])) for row in config["table"]["rows"][:4]]
    while len(labels) < 4:
        labels.append("ďalší porovnateľný variant")
    points = [html.escape(value) for value in config["points"]]
    steps = [html.escape(value) for value in config["steps"]]
    return f"""
<h2>Modelová situácia a rozhodovací rámec</h2>
<p>Predstavte si domácnosť, ktorá rieši tému <strong>{topic}</strong>, ale pri každom pokuse zmení viacero podmienok. Raz použije inú dávku, inokedy iný program alebo materiál a výsledok hodnotí v odlišnom čase. Takéto porovnanie môže viesť k chybnému záveru. Najprv preto určte jednu konkrétnu otázku a jednu premennú. Kontrolný bod pre tento článok znie: {points[0]}. Ostatné podmienky počas prvého porovnania ponechajte čo najpodobnejšie.</p>
<div style="border:1px solid #e1e6df;border-radius:8px;padding:18px;margin:22px 0;background:#fafcf9;">
<h3 style="margin-top:0;">Štyri body, ktoré si zapíšte</h3>
<ul>
<li><strong>Východiskový variant:</strong> {labels[0]}.</li>
<li><strong>Porovnávací variant:</strong> {labels[1]}.</li>
<li><strong>Hraničná situácia:</strong> {labels[2]}.</li>
<li><strong>Kontrola výsledku:</strong> {labels[3]}.</li>
</ul>
</div>
<p>Prvý pokus pripravte podľa rovnakých podmienok, aké používate bežne. Začnite týmto krokom: {steps[0]} Následne pokračujte: {steps[1]} Zaznamenajte materiál, veľkosť náplne alebo množstvo výrobku, zvolený program, teplotu a spôsob sušenia či aplikácie. Pri vôňach pridajte aj čas hodnotenia, pretože prvý dojem nemusí zodpovedať tomu, čo zostane po niekoľkých hodinách. Pri textíliách zas oddeľte pocit na dotyk od funkcie, napríklad savosti, pružnosti alebo odvodu vlhkosti.</p>
<p>V druhom pokuse zmeňte iba jeden parameter a dodržte krok: {steps[2]} Dôležitý je pritom ďalší kontrolný bod: {points[1]}. Ak výsledok nie je jednoznačný, nevytvárajte záver po jedinom cykle. Zopakujte porovnanie na podobnej náplni alebo vzorke. Tak odlíšite skutočný účinok od rozdielu spôsobeného tvrdosťou vody, mierou znečistenia, zvyškovou vlhkosťou, vetraním alebo prirodzenou variabilitou čuchového vnemu.</p>
<p>Rozhodnutie urobte až po poslednom kroku: {steps[-1]} Zároveň rešpektujte zásadu: {points[2]}. Ak sa výsledok zhoršuje, vráťte sa k poslednému funkčnému nastaveniu a skontrolujte etiketu výrobku, ošetrovací štítok textilu alebo návod spotrebiča. Ak je výsledok stabilne lepší, zapíšte si konkrétnu kombináciu podmienok. Vznikne vám opakovateľný domáci postup, ktorý je užitočnejší než všeobecná rada bez znalosti materiálu, dávky a spôsobu použitia.</p>
""".strip()


def render_expansion(config: dict[str, Any]) -> str:
    marker = f"{MARKER_PREFIX}-{config['post_id']}"
    points = "".join(f"<li>{html.escape(value)}</li>" for value in config["points"])
    sections = []
    for title, paragraphs in config["sections"]:
        sections.append(f"<h2>{html.escape(title)}</h2>")
        sections.extend(f"<p>{paragraph}</p>" for paragraph in paragraphs)
    steps = "".join(f"<li>{html.escape(value)}</li>" for value in config["steps"])
    expert = "".join(f"<p>{value}</p>" for value in config["expert"])
    sources = "".join(
        (
            f'<li><a rel="noopener" href="{html.escape(url)}" target="_blank">'
            f"{html.escape(label)}</a></li>"
        )
        for label, url in config["sources"]
    )
    faq = "".join(
        f"<h3>{html.escape(question)}</h3><p>{html.escape(answer)}</p>"
        for question, answer in config["faq"]
    )

    return f"""
<!-- {marker} -->
<p><strong>Rýchla odpoveď:</strong> {config['quick']}</p>
<p>{config['intro']}</p>
<div style="border:1px solid #e6ded2;border-radius:8px;padding:18px;margin:22px 0;background:#fffaf5;">
<h2 style="margin-top:0;">Najdôležitejšie body</h2>
<ul>{points}</ul>
</div>
{''.join(sections)}
<h2>Porovnanie v skratke</h2>
{render_table(config['table'])}
{render_verification_protocol(config)}
{render_decision_framework(config)}
<div style="border:1px solid #d9e2ea;border-radius:8px;padding:18px;margin:24px 0;background:#f8fbfd;">
<h2 style="margin-top:0;">Praktický postup krok za krokom</h2>
<ol>{steps}</ol>
</div>
<h2>Odbornejší pohľad</h2>
{expert}
<div style="border:1px solid #e5e5e5;border-radius:8px;padding:16px;margin:20px 0;background:#fff;">
<h3 style="margin-top:0;">Použité odborné zdroje</h3>
<ul>{sources}</ul>
</div>
<div style="border:1px solid #dbe5de;border-radius:8px;padding:18px;margin:24px 0;background:#f7fbf8;">
<h2 style="margin-top:0;">{html.escape(config['category_title'])}</h2>
<p>{html.escape(config['category_body'])}</p>
<p><a style="display:inline-block;padding:11px 16px;border-radius:6px;border:1px solid #111;color:#111;text-decoration:none;" href="{html.escape(config['category_href'])}">Pozrieť kategóriu</a></p>
<div style="border:1px solid #e5e5e5;border-radius:8px;padding:16px;background:#fff;margin:14px 0;">
<h3 style="margin-top:0;">{html.escape(config['product_title'])}</h3>
<p>{html.escape(config['product_body'])}</p>
<p><a style="display:inline-block;padding:11px 16px;border-radius:6px;background:#111;color:#fff;text-decoration:none;" href="{html.escape(config['product_href'])}">Pozrieť produkt</a></p>
</div>
</div>
<h2>Najčastejšie otázky</h2>
{faq}
""".strip()


def normalize_admin_unicode(value: str) -> str:
    # The legacy admin endpoint can return astral characters as UTF-16
    # surrogate pairs. Normalize them before emitting UTF-8 JSON or HTML.
    return (value or "").encode("utf-16", "surrogatepass").decode("utf-16", "replace")


def sanitize_legacy(markup: str) -> str:
    markup = normalize_admin_unicode(markup)
    markup = re.sub(r"(?is)<script\b[^>]*>.*?</script>", "", markup or "")
    markup = re.sub(r"(?is)<!--.*?-->", "", markup)
    markup = public_guard.sanitize_text(markup)
    markup = re.sub(
        r"(?i)\bCena\s*:",
        "Aktuálnu cenu nájdete na stránke produktu.",
        markup,
    )
    markup = re.sub(
        r"(?i)\b\d{1,4}[,.]\d{2}\s*(?:€|EUR)",
        "",
        markup,
    )
    markup = re.sub(r"(?is)<(strong|span)\b[^>]*>\s*</\1>", "", markup)
    return re.sub(r"[ \t]{2,}", " ", markup).strip()


def validate_config(config: dict[str, Any], existing: dict[str, Any]) -> dict[str, Any]:
    if str(existing.get("title") or "").strip() not in {
        config["expected_title"],
        config["title"],
    }:
        raise RuntimeError(
            f"post {config['post_id']} title drift: {existing.get('title')!r}"
        )
    if str(existing.get("link") or "").strip() != config["expected_slug"]:
        raise RuntimeError(
            f"post {config['post_id']} slug drift: {existing.get('link')!r}"
        )
    if str(existing.get("active") or "0") != "1":
        raise RuntimeError(f"post {config['post_id']} is not public")

    marker = f"{MARKER_PREFIX}-{config['post_id']}"
    legacy = sanitize_legacy(str(existing.get("long") or ""))
    if marker in str(existing.get("long") or ""):
        long_body = normalize_admin_unicode(str(existing["long"]))
        already_applied = True
    else:
        long_body = (
            render_expansion(config)
            + "\n<h2>Ďalší pôvodný prehľad témy</h2>\n"
            + legacy
        )
        already_applied = False

    article = {
        "post_id": config["post_id"],
        "title": config["title"],
        "short": config["short"],
        "long": long_body,
        "link": config["expected_slug"],
        "title_tag": config["title"],
        "description": config["description"],
        "active": True,
    }
    html_result = html_guard.analyze(article)
    public_hits = public_guard.find_hits(article)
    metrics = depth_guard.article_metrics(article)
    failures = list(html_result["failures"])
    if public_hits:
        failures.append(f"forbidden public wording: {public_hits}")
    if metrics["words"] < 1500:
        failures.append(f"visible word count {metrics['words']} < 1500")
    if metrics["h2_count"] < 12:
        failures.append(f"h2 count {metrics['h2_count']} < 12")
    if metrics["table_count"] < 2:
        failures.append(f"table count {metrics['table_count']} < 2")
    if metrics["styled_block_count"] < 6:
        failures.append(f"styled blocks {metrics['styled_block_count']} < 6")
    if metrics["faq_question_count"] < 5:
        failures.append(f"FAQ count {metrics['faq_question_count']} < 5")
    if failures:
        raise RuntimeError(
            f"post {config['post_id']} failed content guards: "
            + json.dumps(failures, ensure_ascii=False)
        )
    return {
        **article,
        "already_applied": already_applied,
        "title_changed": norm(config["title"]) != norm(config["expected_title"]),
        "metrics": metrics,
        "html_safety": html_result,
    }


def fetch_and_prepare() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    all_ids = [config["post_id"] for config in CONFIGS] + [
        HIDE_ACTION["post_id"],
        HIDE_ACTION["canonical_post_id"],
    ]
    snapshot = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "posts": {
            post_id: content_mcp.admin_get_news_post(post_id)
            for post_id in all_ids
        },
    }
    prepared = [
        validate_config(config, snapshot["posts"][config["post_id"]])
        for config in CONFIGS
    ]

    hide_post = snapshot["posts"][HIDE_ACTION["post_id"]]
    canonical = snapshot["posts"][HIDE_ACTION["canonical_post_id"]]
    if str(hide_post.get("title") or "").strip() != HIDE_ACTION["expected_title"]:
        raise RuntimeError("bad-slug curtain duplicate title drift")
    if str(hide_post.get("link") or "").strip() != HIDE_ACTION["expected_slug"]:
        raise RuntimeError("bad-slug curtain duplicate slug drift")
    if str(canonical.get("link") or "").strip() != HIDE_ACTION["canonical_slug"]:
        raise RuntimeError("curtain canonical slug drift")
    if norm(str(hide_post.get("long") or "")) != norm(str(canonical.get("long") or "")):
        raise RuntimeError("curtain duplicate body no longer matches canonical")
    if content_mcp.public_status_for_slug(HIDE_ACTION["canonical_slug"]).get("status_code") != 200:
        raise RuntimeError("curtain canonical is not publicly available")

    active_titles: dict[str, list[str]] = {}
    for block_id in ("1905", "774", "765"):
        for row in content_mcp.admin_list_news_posts(block_id, limit=content_mcp.DUPLICATE_SCAN_LIMIT):
            if str(row.get("active") or "0") == "1":
                active_titles.setdefault(norm(str(row.get("title") or "")), []).append(
                    str(row.get("news_id") or "")
                )
    planned_titles = {
        str(article["post_id"]): norm(str(article["title"])) for article in prepared
    }
    for article in prepared:
        conflicting = [
            post_id
            for post_id in active_titles.get(norm(article["title"]), [])
            if post_id != article["post_id"]
            and planned_titles.get(post_id, norm(article["title"]))
            == norm(article["title"])
        ]
        if conflicting:
            raise RuntimeError(
                f"new title for {article['post_id']} conflicts with {conflicting}"
            )

    return prepared, snapshot


def new_links(prepared: list[dict[str, Any]]) -> list[str]:
    links = set()
    for article in prepared:
        marker_end = article["long"].find("<h2>Ďalší pôvodný prehľad témy</h2>")
        expansion = article["long"][:marker_end] if marker_end >= 0 else article["long"]
        for href in re.findall(r'href=["\']([^"\']+)["\']', expansion):
            links.add(requests.compat.urljoin("https://www.vevo.sk", href))
    return sorted(links)


def check_links(urls: list[str]) -> list[dict[str, Any]]:
    results = []
    failures = []
    for url in urls:
        response = requests.get(
            url,
            headers={"User-Agent": "Codex VEVO exact duplicate remediation"},
            timeout=45,
            allow_redirects=True,
        )
        result = {
            "url": url,
            "status_code": response.status_code,
            "final_url": response.url,
            "ok": response.status_code < 400,
        }
        results.append(result)
        if not result["ok"]:
            failures.append(result)
    if failures:
        raise RuntimeError(
            "link preflight failed: " + json.dumps(failures, ensure_ascii=False)
        )
    return results


def write_prepared(prepared: list[dict[str, Any]], snapshot: dict[str, Any], links: list[dict[str, Any]]) -> None:
    PREPARED.write_text(
        json.dumps(
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "articles": prepared,
                "hide_action": HIDE_ACTION,
                "link_preflight": links,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    if not BACKUP.exists():
        BACKUP.write_text(
            json.dumps(snapshot, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )


def execute(prepared: list[dict[str, Any]], link_results: list[dict[str, Any]]) -> dict[str, Any]:
    report: dict[str, Any] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "mode": "execute-live",
        "updates": [],
        "hide": None,
        "link_preflight": link_results,
        "all_ok": False,
    }
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    try:
        # Retitle the secondary copy first so the API duplicate-title guard can
        # safely accept the canonical article update afterwards.
        for article in sorted(prepared, key=lambda item: not item["title_changed"]):
            if article["already_applied"]:
                post = content_mcp.admin_get_news_post(article["post_id"])
                public_status = content_mcp.public_status_for_slug(article["link"])
                result = {
                    "post_id": article["post_id"],
                    "title": post.get("title"),
                    "slug": post.get("link"),
                    "action": "already-applied",
                    "public_status": public_status,
                    "metrics": article["metrics"],
                    "ok": public_status.get("status_code") == 200,
                }
            else:
                updated = content_mcp.tool_update_news_post(
                    {
                        "post_id": article["post_id"],
                        "title": article["title"],
                        "short": article["short"],
                        "long": article["long"],
                        "title_tag": article["title_tag"],
                        "description": article["description"],
                        "active": True,
                        "confirm_visible": True,
                    }
                )
                result = {
                    "post_id": article["post_id"],
                    "title": updated["news_post"].get("title"),
                    "slug": updated["news_post"].get("link"),
                    "action": "updated",
                    "public_status": updated["public_status"],
                    "metrics": article["metrics"],
                    "ok": updated["public_status"].get("status_code") == 200,
                }
            report["updates"].append(result)
            REPORT.write_text(
                json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

        hide_post = content_mcp.admin_get_news_post(HIDE_ACTION["post_id"])
        if str(hide_post.get("active") or "0") == "1":
            hidden = content_mcp.tool_update_news_post(
                {"post_id": HIDE_ACTION["post_id"], "active": False}
            )
            hide_result = {
                "post_id": HIDE_ACTION["post_id"],
                "action": "hidden",
                "slug": hidden["news_post"].get("link"),
                "public_status": hidden["public_status"],
                "ok": hidden["public_status"].get("status_code") == 404,
            }
        else:
            public_status = content_mcp.public_status_for_slug(HIDE_ACTION["expected_slug"])
            hide_result = {
                "post_id": HIDE_ACTION["post_id"],
                "action": "already-hidden",
                "slug": HIDE_ACTION["expected_slug"],
                "public_status": public_status,
                "ok": public_status.get("status_code") == 404,
            }
        report["hide"] = hide_result
        report["all_ok"] = all(item["ok"] for item in report["updates"]) and hide_result["ok"]
    finally:
        REPORT.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Apply the prepared updates and hide the exact bad-slug duplicate.",
    )
    args = parser.parse_args()

    prepared, snapshot = fetch_and_prepare()
    link_results = check_links(new_links(prepared))
    write_prepared(prepared, snapshot, link_results)

    summary = {
        "article_count": len(prepared),
        "min_words": min(item["metrics"]["words"] for item in prepared),
        "max_words": max(item["metrics"]["words"] for item in prepared),
        "link_count": len(link_results),
        "prepared_file": str(PREPARED),
        "backup_file": str(BACKUP),
    }
    if not args.execute_live:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    report = execute(prepared, link_results)
    print(json.dumps({**summary, "results_file": str(REPORT), "all_ok": report["all_ok"]}, ensure_ascii=False, indent=2))
    return 0 if report["all_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
