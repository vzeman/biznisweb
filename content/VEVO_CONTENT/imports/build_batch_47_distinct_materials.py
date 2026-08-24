#!/usr/bin/env python3
"""Build and validate VEVO batch 47 distinct-material articles."""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

from build_batch_46_special_materials import (
    BASE,
    FIXED_PRICE_RE,
    FORBIDDEN_PUBLIC_RE,
    WORD_RE,
    article_hrefs,
    fetch_status,
    render_article,
    visible_text,
)


PUBLISH_DATE = "2026-08-24"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-47-candidates-2026-08-24.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-47-2026-08-24-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-47-2026-08-24-link-preflight.json")

FIT_FABRIC = "https://www.fitnyc.edu/museum/exhibitions/fabric-in-fashion.php"
FIT_UNRAVELED = "https://exhibitions.fitnyc.edu/fashion-unraveled/"
COTTONWORKS_PIQUE = "https://cottonworks.com/encyclopedia-item/pique-woven/"
COTTONWORKS_DOUBLE = "https://cottonworks.com/learning-hub/knitting/single-and-double-knits/"
COTTONWORKS_PONTE = "https://cottonworks.com/encyclopedia-item/ponte-de-roma/"
LUREX_ABOUT = "https://www.lurex.com/who-we-are/"
LUREX_PRODUCTS = "https://www.lurex.com/products/"
LUREX_DYE = "https://www.lurex.com/innovation/new-dye-resist-lurex/"
AATCC_DIMENSION = "https://members.aatcc.org/store/tm135/543/"
AATCC_COLOR = "https://members.aatcc.org/store/tm61/495/"
ASTM_SNAG = "https://store.astm.org/d3939_d3939m-26.html"
ASTM_PILLING = "https://store.astm.org/d3512_d3512m-22.html"
EU_FIBRE_LABEL = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A02011R1007-20180215"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"

ARTICLE_CHIFFON = "/n/co-je-sifon-jemna-priesvitna-latka-a-bezpecna-starostlivost"
ARTICLE_NEOPRENE = "/n/co-je-neopren-penovy-material-zapach-cistenie-a-susenie"
ARTICLE_COTTON = "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"
ARTICLE_POLYESTER = "/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal"
ARTICLE_BLEND = "/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni"
ARTICLE_SNAGGING = "/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_GEL = "/n/ako-vybrat-praci-gel-podla-typu-bielizne"
ARTICLE_ODOR = "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"
ARTICLE_IRONING = "/n/ako-vyzehlit-koselu-kompletny-sprievodca-pre-dokonaly-vysledok"


ORGANZA: dict[str, object] = {
    "title": "Čo je organza: priesvitná tuhá látka, záhyby a šetrné pranie",
    "link": "co-je-organza-priesvitna-tuha-latka-zahyby-a-setrne-pranie",
    "meta": "Čo je organza, ako ju odlíšiť od šifónu a tylu a ako prať, sušiť, žehliť a uložiť organzové šaty, závoj, mašľu či záclonu.",
    "short": "Organza je ľahká priesvitná tkanina s pevnejším, chrumkavým omakom. Môže byť hodvábna, polyesterová, polyamidová alebo zmesová, preto jej názov nestačí na voľbu programu. Zistite, ako chrániť jemné nite, pevné záhyby, ozdoby a lesk bez drhnutia a prudkého tepla.",
    "name": "organza",
    "genitive": "organzy",
    "locative": "organze",
    "construction_summary": "jemnú priesvitnú tkaninu, najčastejšie v plátnovej väzbe, ktorej pevnejší omak vytvára priadza, hustota a dokončenie",
    "label_details": "presné vlákno, tuhú povrchovú úpravu, podšívku, výstuž, výšivku, koráliky, lepené aplikácie, kostice, tvarované sklady a spôsob zavesenia",
    "residue_place": "jemných medzerách väzby, pri švoch, v riasení, pod výšivkou a na povrchovej úprave",
    "friction_risk": "tenké nite, okraj výšivky, surový lem alebo vystupujúci povrchový detail",
    "drying_advice": "Ľahký nezdobený kus rozložte alebo zaveste na širokú hladkú oporu podľa etikety; viacvrstvové šaty a závoj podoprite tak, aby mokrá hmotnosť neťahala jeden šev. Záclonu zaveste až vtedy, keď kovanie ani mokrá dĺžka nepoškodia tkaninu.",
    "heat_risk": "tuhosť dokončenia, lesk, syntetické vlákno, tvar skladov, lepidlo, výšivku a rozmer podšívky",
    "failure_sign": "rozostúpenie nití, zatrhnutie povrchu, praskanie tuhého filmu alebo oddeľovanie ozdoby",
    "answer": "Organza je priesvitná, ľahká, ale pomerne pevná tkanina, ktorá drží objem a ostrejšiu siluetu. Nie je to názov jedného vlákna: môže byť z hodvábu, polyesteru, polyamidu alebo zo zmesi. Pred čistením preto rozhoduje celý štítok, podšívka, výšivka, lepidlo a tvar odevu. Prateľnú nezdobenú organzu perte oddelene od zipsov a drsných textílií, v šetrnom režime povolenom výrobcom, bez krútenia a s dôkladným oplachom. Sušte ju s rovnomernou oporou a žehlite len pri povolenom symbole, z rubu cez ochrannú tkaninu a bez silného tlaku. Hodvábne, svadobné, plisované, bohato zdobené alebo profesionálne čistené kusy neperte podľa všeobecného návodu. Záhyb najprv uvoľnite vlhkosťou a nízkym povoleným teplom, nie pritlačením horúcej žehličky.",
    "intro": "Pri otázke ako prať organzu sa často hľadá jediná teplota, no dve podobne priesvitné látky môžu mať úplne odlišné zloženie a dokončenie. Organza na detskej mašli, polyesterovej záclone, svadobných šatách a historickom hodvábnom rukáve nie je rovnaký výrobok. Práve schopnosť držať objem vzniká jemnou konštrukciou a pevnejším omakom, ktoré môže voda, trenie alebo teplo zmeniť. Bezpečný postup preto začína rozlíšením organzy od šifónu a tylu, kontrolou nosných švov a skúškou farby. Až potom má zmysel riešiť program, sušenie a vyrovnanie záhybov.",
    "quick": [
        "<strong>Organza je konštrukcia, nie jedno vlákno:</strong> hodváb a polyester môžu vyzerať podobne, ale nemajú rovnaké hranice vody a tepla.",
        "<strong>Priesvitnosť neznamená mäkkosť:</strong> organza býva pevnejšia a drží objem, zatiaľ čo šifón spravidla viac splýva.",
        "<strong>Najväčším rizikom je lokálne trenie:</strong> zips, korálik alebo necht môže vytiahnuť niť a rozostúpiť väzbu.",
        "<strong>Záhyb sa nevyrovnáva silou:</strong> vysoká teplota môže syntetiku zdeformovať a hodváb či dokončenie zmeniť.",
        "<strong>Svadobné a zdobené kusy sú samostatná kategória:</strong> o domácom praní nerozhoduje iba vrchná organza.",
        "<strong>Úplné vysušenie chráni tvar aj vôňu:</strong> vlhkosť môže zostať v riasení, podšívke a pod aplikáciami.",
    ],
    "overview_heading": "Čo je organza a prečo pôsobí súčasne ľahko aj pevne",
    "overview": [
        "Organza sa tradične spája s jemnou plátnovou väzbou a hodvábnou priadzou, no dnešné výrobky môžu používať polyester, polyamid, viskózu alebo zmes. Tenké nite a priesvitná plocha vytvárajú vzdušný vzhľad, kým vysoký zákrut, hustota a dokončenie dodávajú chrumkavý omak. Tento rozpor vysvetľuje, prečo vrstva drží objem sukne alebo rukáva, ale zároveň sa môže ľahko zachytiť o ostrý predmet.",
        "Museum at FIT pri historickom odeve z hodvábnej organzy upozorňuje na jemne tkanú priesvitnú štruktúru a lesk dlhých hladkých hodvábnych vlákien. Zbierkové príklady však nie sú návodom na domáce pranie moderného polyesterového kusa. Ukazujú, že rovnaký názov prežil naprieč obdobiami, vláknami a účelmi. Spotrebiteľ musí preto čítať zloženie a ošetrovacie symboly hotového výrobku.",
        "Pevnosť na dotyk nie je dôkaz vysokej odolnosti. Organza môže držať ostrý sklad, no pri zatiahnutí jednej nite sa väzba lokálne otvorí. Lem, šev a miesto pod korálikom znášajú inú záťaž než voľná plocha. Pri výbere si všimnite, či sa pri jemnom ohnutí objavujú biele línie, či nite pri šve ustupujú a či povrchová úprava zostáva rovnomerná.",
    ],
    "table1_heading": "Organza, šifón, tyl a organdy: čo si nezamieňať",
    "table1_intro": "Priesvitný vzhľad spája viac materiálov, no ich konštrukcia, pád a riziká sa líšia. Označenie na e-shope si vždy overte štítkom a pohľadom na väzbu.",
    "table1_headers": ["Materiál", "Typická konštrukcia", "Omak a pád", "Hlavné riziko pri starostlivosti"],
    "table1_rows": [
        ("Organza", "Jemná priesvitná tkanina, často plátnová väzba.", "Pevnejšia, ľahká, drží objem a ostrejší záhyb.", "Zatrhnutie nití, rozostúpenie väzby, zmena tuhosti a tepelné poškodenie."),
        ("Šifón", "Veľmi ľahká tkanina z jemných často vysoko zakrútených priadzí.", "Mäkkší, splývavý, zrnitý alebo pieskový dotyk.", "Posun nití, deformácia mokrou hmotnosťou a vodné mapy."),
        ("Tyl", "Otvorená sieťovaná konštrukcia s pravidelnými okami.", "Od mäkkého závojového po tuhý objemový variant.", "Zachytenie a roztrhnutie oka, deformácia sieťky teplom."),
        ("Organdy", "Jemná tuho upravená tkanina, tradične bavlnená.", "Priesvitná a chrumkavá, no vláknovo odlišná od hodvábnej organzy.", "Zmena dokončenia, zrazenie bavlny a ostré trvalé lomy."),
    ],
    "sections": [
        {
            "heading": "Hodvábna, polyesterová a polyamidová organza",
            "paragraphs": [
                "Hodvábna organza využíva proteínové vlákno s prirodzeným leskom a citlivosťou na nevhodné zásady, vysoké teplo, pot a dlhé svetlo. Polyesterová organza môže byť rozmerovo stabilnejšia a rýchlejšie schnúť, ale termoplastické vlákno sa pri horúcej žehličke môže lesknúť, zmrštiť alebo lokálne roztaviť. Polyamidový variant býva pevný a ľahký, no tiež vyžaduje opatrnosť pri teple.",
                "Zmes mení správanie celej plochy a údaj o vrchnej vrstve ešte nehovorí nič o podšívke. Pri organzových šatách môže byť najcitlivejší saténový spodok, výstuž živôtika, lepidlo pod aplikáciou alebo farebná výšivka. Ak etiketa povoľuje iba profesionálne čistenie, nevyberajte domáci cyklus podľa toho, že samostatná polyesterová metráž by ho teoreticky zniesla.",
            ],
        },
        {
            "heading": "Ako prať organzové šaty bez poškodenia objemu",
            "paragraphs": [
                "Najprv skontrolujte nosné švy, zips, háčiky, podšívku, kostice, vrstvy sukne a ozdoby. Odev obráťte naruby iba vtedy, ak sa pritom neohýba pevne vytvarovaný diel. Odnímateľné stuhy a kovové doplnky odstráňte podľa návodu. Škvrny fotografujte a lokálny prostriedok skúšajte na vnútornom prídavku šva, pretože zmena lesku môže byť viditeľnejšia než samotný zvyšok škvrny.",
                "Pri povolenom ručnom praní použite veľkú nádobu, aby sa šaty nemuseli stláčať. Vodu jemne pretláčajte cez vrstvy a materiál netrite o seba. Pri povolenej práčke zvoľte ochranu a ľahkú náplň bez tvrdého kovania. Mokré šaty vyberajte s oporou sukne aj živôtika, nie za ramienka, a nikdy ich neskrúcajte do povrazu.",
            ],
        },
        {
            "heading": "Ako prať organzovú záclonu a dekoračnú látku",
            "paragraphs": [
                "Zo záclony odstráňte háčiky, krúžky, závažia a voľný prach. Prach pred vodou jemne vytraste alebo odsajte cez ochrannú sieťku podľa pevnosti materiálu. Dlhá organza sa v bubne ľahko zauzlí, preto ju vložte voľne do veľkého pracieho vrecka alebo zvoľte spôsob uvedený výrobcom. Nekombinujte ju s uterákmi, posteľnou bielizňou ani odevmi so zipsom.",
                "Záclonu možno pri povolení zavesiť mierne vlhkú, aby vlastná hmotnosť uvoľnila drobné záhyby. Garniža, štipce a šev však musia mokrú hmotnosť bezpečne zniesť. Dlhú tkaninu neťahajte po podlahe a nezatvárajte pri nej okno tak, aby sa prilepila na studené sklo. Pri vrstvenej alebo potlačenej dekorácii postupujte ako pri kombinovanom výrobku.",
            ],
        },
        {
            "heading": "Svadobné šaty, závoj, výšivka a lepené ozdoby",
            "paragraphs": [
                "Svadobný kus môže spájať organzu, tyl, satén, čipku, kovové nite, koráliky, lepidlo, kostice a viac druhov podšívky. Voda môže byť prijateľná pre jednu vrstvu a nevhodná pre druhú. Bodová škvrna sa môže po domácom zmáčaní rozšíriť do mapy alebo presunúť farbu z výšivky. Pri vysokej hodnote a neznámej konštrukcii je profesionálne posúdenie menším rizikom než pokus v celej vani.",
                "Závoj chyťte po celej šírke, nie za hrebeň alebo jediný roh. Kovový hrebeň a kamienky môžu poškriabať susednú plochu, preto ich pri ukladaní oddeľte mäkkou nekyslou vrstvou. Museum at FIT dokumentuje použitie organzy v historických a módnych odevoch; pri takomto alebo rodinnom predmete treba čistenie chápať ako nevratný zásah, nie ako bežnú údržbu.",
            ],
            "callout": {
                "title": "Kedy organzový kus nepatrí do domáceho cyklu",
                "items": [
                    "Etiketa povoľuje iba profesionálne čistenie alebo chýba pri hodnotnom odeve.",
                    "Výšivka, koráliky, lepidlo, kovová niť alebo podšívka nemajú overenú stálofarebnosť.",
                    "Tuhý živôtik, kostice alebo lepený lem by sa pri ponorení mohli zdeformovať.",
                    "Hodváb je krehký, povrch už praská alebo sa nite pri šve rozostupujú.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Ako vyrovnať pokrčenú organzu bez spálenia",
            "paragraphs": [
                "Najprv nechajte tkaninu voľne visieť v suchom vetranom priestore. Mierny záhyb sa môže uvoľniť bez zásahu. Ak etiketa povoľuje žehlenie, pracujte z rubu cez čistú bavlnenú ochrannú tkaninu, nastavte teplotu podľa najcitlivejšieho vlákna a vyskúšajte vnútorný lem. Žehličku prikladajte krátko, bez silného šmýkania a bez vytvárania nového ostrého lomu.",
                f"Naparovač nie je automaticky bezpečnejší: horúca para môže zmäkčiť dokončenie, zmeniť lepidlo a kondenzovať do mapy. Držte sa vzdialenosti a pokynov výrobcu a nepracujte na zavesenom odeve priamo proti pokožke. Všeobecné zásady teploty rozoberá návod <a href=\"{ARTICLE_IRONING}\">ako žehliť oblečenie podľa materiálu</a>, no symbol konkrétnej organzy má prednosť.",
            ],
        },
        {
            "heading": "Škvrny od make-upu, jedla, vína a potu",
            "paragraphs": [
                "Čerstvú tekutinu odsajte bielou savou handričkou bez trenia. Pevný zvyšok nadvihnite tupou hranou. Make-up a mastnota vyžadujú kompatibilný tenzid, pigmentový nápoj zas kontrolu farby a oplach. Na hodvábnej organze nepoužívajte bez overenia alkalický prostriedok ani bielidlo. Bodové mokré miesto rozširujte iba kontrolovane, aby nevznikol ostrý okraj.",
                "Pot sa sústreďuje pri podpazuší, golieri a popruhoch a môže meniť farbu ešte pred viditeľnou škvrnou. Odev po nosení nenechávajte uzavretý vo vaku. Vyvetrajte ho a zvoľte povolené čistenie skôr, než sa pot a kozmetika zoxidujú. Vôňa neodstráni maz ani soli a pri citlivej pokožke môže iba pridať ďalšie látky na povrch.",
            ],
        },
        {
            "heading": "Zatrhnutie, posun nití a rozostúpený šev",
            "paragraphs": [
                "Zatrhnutá niť sa nesmie odstrihnúť bez posúdenia, pretože môže byť súčasťou celej osnovy alebo útku. Látku položte naplocho a jemne rozložte napätie do okolia bez ťahania slučky. Ak sa niť pretrhla alebo sa väzba pri šve otvorila, pranie problém zväčší. Opravu zverte človeku, ktorý vie pracovať s jemnou transparentnou tkaninou a vhodnou ihlou.",
                f"Posun nití môže vyzerať ako svetlá čiara alebo dierka bez pretrhnutia. Často vzniká pri tesnom šve, ťahu a šmyku priadzí. Podrobnejší mechanizmus vysvetľuje článok <a href=\"{ARTICLE_SNAGGING}\">prečo vznikajú vytiahnuté očká a zatrhnutia</a>. Pri novom odeve stav zdokumentujte skôr, než ho budete rozťahovať späť alebo opakovane prať.",
            ],
        },
        {
            "heading": "Sušenie viacvrstvovej organzy a kontrola vlhkosti",
            "paragraphs": [
                "Jedna vrstva organzy vyschne rýchlo, no riasenie, podšívka, pás a miesto pod aplikáciou môžu zostať vlhké. Odev otočte a skontrolujte vnútorné švy. Prúdenie vzduchu je účinnejšie než prudké teplo. Ventilátor môže zlepšiť výmenu vzduchu v miestnosti, ale nesmie odfukovať ľahkú tkaninu o drsný povrch alebo ohrievač.",
                "Pred uložením porovnajte teplotu hrubších a tenších miest dotykom suchej ruky a nechajte kus ešte ustáliť. Zatuchnutie po uložení často nevzniká z nedostatku parfumácie, ale z vlhkej podšívky alebo nepriedušného vaku. Ak sa pach vracia iba na jednom mieste, skontrolujte výstuž a hrubé švy, nie iba viditeľnú organzu.",
            ],
        },
        {
            "heading": "Ako skladovať organzové šaty, závoj a mašle",
            "paragraphs": [
                "Ľahké šaty zaveste na široký polstrovaný vešiak so závesnými pútkami, ak to strih povoľuje. Ťažký zdobený kus uložte naplocho s mäkkými prekladmi, aby sa ramená nevyťahali. Závoj voľne preložte cez veľké oblúky a miesto preloženia občas zmeňte. Ostré koráliky a hrebene oddeľte od transparentnej plochy.",
                "Krabica a prekladový materiál musia byť čisté, suché a vhodné na textil. Nevkladajte organzu priamo k drevu, farebnému papieru alebo PVC obalu. Tesný plast môže zachytiť zvyškovú vlhkosť a tlak môže vytvoriť tvrdé lomy. Pred ďalším použitím kus rozložte s predstihom, aby sa mierne záhyby uvoľnili bez náhleho tepelného zásahu.",
            ],
        },
        {
            "heading": "Ako vybrať organzu na šaty, dekoráciu alebo záclonu",
            "paragraphs": [
                "Na objemovú sukňu sledujte návrat po stlačení, pevnosť pri šve a schopnosť vrstiev kĺzať bez zachytávania. Na rukáv je dôležitý dotyk na koži a drsnosť okraja. Pri záclone skontrolujte UV expozíciu, prateľnosť, rozmery a nosnosť horného lemu. Metráž s krásnym leskom nemusí byť vhodná na každodenné trenie alebo časté pranie.",
                "Pred šitím vzorku predčistite presne spôsobom, ktorý bude používaný neskôr, ak to dodávateľ povoľuje. Sledujte rozmer, tuhosť, farbu a strapkanie okraja. Pri hotovom výrobku žiadajte čitateľnú etiketu. Označenie hodvábny vzhľad nie je hodvábne zloženie a slovo svadobná nehovorí nič o povolenej vode, teplote ani nosnosti ozdôb.",
            ],
        },
    ],
    "table2_heading": "Organza po praní: príčina zmeny a bezpečný ďalší krok",
    "table2_intro": "Najprv nechajte kus úplne vyschnúť a až potom rozlišujte zvyšok produktu, zmenu dokončenia, deformáciu a skutočné mechanické poškodenie.",
    "table2_headers": ["Prejav", "Pravdepodobná skupina príčin", "Čo skontrolovať", "Ďalší krok"],
    "table2_rows": [
        ("Látka je mäkšia a nedrží objem", "Uvoľnenie alebo poškodenie tuhej úpravy, teplo, dlhé máčanie.", "Etiketu, teplotu, použitý produkt a rovnomernosť zmeny.", "Nepridávať škrob naslepo; pri hodnotnom kuse konzultovať obnovu dokončenia."),
        ("Na povrchu sú biele zlomy", "Ostré preloženie, tlak, poškodenie filmu alebo termoplastického vlákna.", "Či je niť celá a či sa zmena ukáže proti svetlu.", "Nežehliť silou; uvoľniť zavesením a nízkym povoleným teplom."),
        ("Pri šve vznikla medzera", "Posun nití, tesný strih, ťažká mokrá sukňa alebo poškodený steh.", "Stehy, smer ťahu a zdravú plochu okolo šva.", "Pred ďalším praním stabilizovať a odborne opraviť."),
        ("Povrch je klzký alebo fľakatý", "Nadbytok prostriedku, nedostatočný oplach, farbivo alebo lepidlo.", "Skrytú skúšku, vôňu po úplnom vysušení a stav ozdôb.", "Pri povolení jemne opláchnuť; pri lepení zásah zastaviť."),
        ("Farba alebo lesk sa lokálne zmenili", "Trenie, vysoká teplota, pot, svetlo alebo nestálofarebná dekorácia.", "Rozhranie škvrny, podšívku a skúšku bielou handričkou.", "Nedrhnúť ani nebieliť; zdokumentovať a zvoliť odborné čistenie."),
    ],
    "steps_heading": "Ako bezpečne vyčistiť organzu krok za krokom",
    "steps": [
        "Prečítajte zloženie a všetky symboly a zapíšte si podšívku, ozdoby, výstuž, lepidlo a tvarované sklady.",
        "Prezrite švy proti svetlu, odstráňte odnímateľné kovanie a lokálny produkt otestujte na skrytom mieste.",
        "Oddeľte organzu od uterákov, zipsov, suchých zipsov a ťažkých kusov; citlivú plochu primerane chráňte.",
        "Použite iba povolené ručné, strojové alebo profesionálne čistenie, primeranú dávku a nízku mechanickú záťaž.",
        "Materiál netrite, nekrúťte a mokrý odev vyberajte s oporou všetkých ťažších vrstiev.",
        "Sušte v tieni s prúdením vzduchu a kontrolujte podšívku, riasenie, pás, švy a priestor pod aplikáciami.",
        "Záhyby vyrovnávajte až po skúške, z rubu, pri najnižšom povolenom teple a bez silného tlaku.",
        "Úplne suchý kus uložte bez ostrého preloženia a oddeľte kovové alebo drsné ozdoby od organzy.",
    ],
    "remember": [
        "Je organza hodvábna, polyesterová, polyamidová alebo zmesová?",
        "Drží tvar samotnou väzbou, zákrutom priadze alebo tuhou povrchovou úpravou?",
        "Má podšívka, výšivka, lepidlo, kostica alebo plisovanie nižší limit než vrchná vrstva?",
        "Sú švy a nite pri pohľade proti svetlu celistvé bez rozostupovania?",
        "Povoľuje etiketa vodu, práčku, sušičku, paru a konkrétny stupeň žehlenia?",
        "Bude mať mokrý viacvrstvový kus počas sušenia rovnomernú oporu?",
    ],
    "mistakes": [
        "Považovať každú organzu za polyester a zvoliť teplotu iba podľa vzhľadu.",
        "Prať závoj alebo šaty spolu so zipsami, uterákmi a suchými zipsami.",
        "Drhnúť lokálnu škvrnu a tým vytiahnuť nite alebo zmeniť lesk.",
        "Zavesiť ťažkú mokrú sukňu za úzke ramienka a vyťahať švy.",
        "Priložiť horúcu žehličku na záhyb bez skúšky vlákna a dokončenia.",
        "Uložiť organzu vlhkú v tesnom plastovom vaku alebo pod ostrými ozdobami.",
    ],
    "expert_heading": "Odbornejší pohľad: priesvitnosť, konštrukcia a overovanie zmien",
    "expert": [
        "Priesvitnosť vzniká tým, koľko svetla prejde medzi niťami a cez samotné vlákna. Neurčuje ju iba gramáž. Jemná priadza, otvorenosť väzby, index lomu, farba a povrch menia výsledok. Preto môže pevná organza prepustiť viac svetla než mäkká hustejšia látka. Domáci pohľad proti oknu pomôže odhaliť posun nití, nie však presné materiálové parametre.",
        "Museum at FIT dokumentuje jemne tkanú hodvábnu organzu ako priesvitnú vrstvu s leskom dlhých hladkých vlákien a zároveň ukazuje syntetické textílie, ktoré môžu napodobniť hodvábny vzhľad. To podporuje základnú hranicu článku: estetika nie je dôkaz vláknového zloženia. Presnú informáciu poskytuje označenie podľa pravidiel pre textilné vlákna a ošetrovací štítok.",
        "AATCC TM61 hodnotí zmenu farby a povrchu pri definovanom zrýchlenom praní, ASTM D3939 zachytávanie a AATCC TM135 rozmerovú zmenu. Ide o odlišné mechanizmy. Organza môže obstáť v jednej skúške a byť citlivá v druhej. Pri porovnávaní výrobkov preto treba poznať metódu, vzorku, smer aj počet cyklov; samotný prívlastok odolná nepokrýva šev, ozdobu a hotový odev.",
    ],
    "source_intro": "Zdroje podporujú rozlíšenie vláknového zloženia, priesvitnej konštrukcie, farebnej stálosti, zachytávania, rozmerovej zmeny a symbolov. Neurčujú jednu teplotu pre všetky organzové výrobky.",
    "sources": [
        ("Museum at FIT: Fabric In Fashion a hodvábna organza", FIT_FABRIC),
        ("Museum at FIT: organza a konštrukcia módneho odevu", FIT_UNRAVELED),
        ("AATCC TM61: zrýchlená stálofarebnosť pri praní", AATCC_COLOR),
        ("ASTM D3939/D3939M-26: odolnosť proti zachyteniu", ASTM_SNAG),
        ("AATCC TM135-2025: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Bežný prací gél môže byť voľbou iba pri prateľnej nezdobenej organze, ak ho povoľuje presné vlákno a štítok. Pri hodvábe, výstuži, lepidle alebo profesionálnom čistení treba zvoliť určený postup.",
    "product_text": "Hypoalergénny prací gél z marseillského mydla je konkrétna možnosť pre kompatibilnú bežnú bielizeň. Pri organze použite presnú malú dávku pre veľkosť náplne a zabezpečte dôkladný oplach bez zbytočného trenia.",
    "product_limit": "Produkt nie je automaticky vhodný na hodvábnu, svadobnú, plisovanú, lepenú alebo bohato zdobenú organzu a nenahrádza profesionálne čistenie uvedené na etikete. Neopraví zatrhnutie ani poškodenú tuhú úpravu.",
    "category_intro": "Pri výbere gélu rozlišujte vláknové zloženie a hotový výrobok. Organza z polyesteru môže mať iné požiadavky než hodvábna vrstva s výšivkou, aj keď vyzerajú podobne.",
    "category_text": "V kategórii pracích gélov nájdete možnosti pre bežnú domácu bielizeň. Pred použitím porovnajte dávkovanie a určenie produktu so štítkom, farbou, podšívkou a všetkými ozdobami organzy.",
    "related": [
        ("Čo je šifón a ako sa oň starať", ARTICLE_CHIFFON),
        ("Čo je polyester a ako ho prať", ARTICLE_POLYESTER),
        ("Ako predchádzať zatrhávaniu textilu", ARTICLE_SNAGGING),
        ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
        ("Stálofarebnosť textilu", ARTICLE_COLOR),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
    ],
    "faq_title": "organza a organzové výrobky",
    "faq": [
        ("Čo je organza?", "Ľahká priesvitná tkanina s pevnejším omakom, ktorá drží objem. Môže byť hodvábna, polyesterová, polyamidová alebo zmesová."),
        ("Aký je rozdiel medzi organzou a šifónom?", "Organza býva tuhšia a tvarovejšia, šifón mäkší a splývavejší. Presné vlákno sa pri oboch môže líšiť."),
        ("Môže sa organza prať v práčke?", "Iba ak to povoľuje štítok celého výrobku. Použite ochranu pred zachytením, ľahkú náplň a povolený šetrný program."),
        ("Na koľko stupňov prať organzu?", "Univerzálna teplota neexistuje. Rozhoduje vlákno, farba, dokončenie, podšívka a najcitlivejšia ozdoba."),
        ("Ako vyžehliť organzu?", "Pri povolení etikety z rubu cez ochrannú tkaninu, s najnižšou vhodnou teplotou, krátkym kontaktom a bez silného tlaku."),
        ("Ako vyprať organzovú záclonu?", "Odstráňte kovanie a prach, chráňte ju pred zauzlením a perte iba podľa štítku oddelene od drsných textílií."),
        ("Ako vyčistiť organzové svadobné šaty?", "Pri vrstvení, kosticiach, lepidle, výšivke alebo neznámej farbe zvoľte profesionálne posúdenie. Vrchná organza neurčuje postup celých šiat."),
        ("Môže ísť organza do sušičky?", "Len pri výslovnom symbole. Teplo a prevaľovanie môžu zmeniť syntetické vlákno, tuhosť, lesk, ozdoby a švy."),
        ("Ako odstrániť záhyby bez žehličky?", "Kus voľne zaveste v suchom vetranom priestore. Ďalšiu vlhkosť alebo paru používajte len pri povolení a po skrytej skúške."),
    ],
}


SCUBA: dict[str, object] = {
    "title": "Čo je scuba úplet: pevný dvojitý úplet, pot a pranie",
    "link": "co-je-scuba-uplet-pevny-dvojity-uplet-pot-a-pranie",
    "meta": "Čo je scuba úplet, ako sa líši od neoprénu a ponte a ako prať scuba šaty, sukne, nohavice a športové kúsky bez zápachu a straty tvaru.",
    "short": "Scuba úplet je hustejší tvarovo pevný odevný úplet, často z polyesteru s elastanom. Nie je automaticky neoprénovou penou. Naučte sa rozlíšiť konštrukciu, chrániť pružnosť, odstrániť pot a sušiť bez tepelnej deformácie.",
    "name": "scuba úplet",
    "genitive": "scuba úpletu",
    "locative": "scuba úplete",
    "construction_summary": "hustý dvojitý alebo príbuzný viacvrstvový úplet s hladkými lícami, objemom a kontrolovanou pružnosťou, nie penové elastomérne jadro",
    "label_details": "percentá polyesteru, polyamidu, viskózy, bavlny a elastanu, typ dvojitého úpletu, smer pružnosti, potlač, lepené prvky, podšívku, výstuž pásu a teplotný limit",
    "residue_place": "hustom úplete, medzi dvoma lícami, v švoch, páse a miestach nasiaknutých potom alebo kozmetikou",
    "friction_risk": "hladké očká, elastické vlákna, potlač alebo vystupujúcu niť pri šve",
    "drying_advice": "Šaty, sukňu alebo nohavice vytvarujte na pôvodné švy a sušte naplocho alebo na širokej opore podľa etikety. Hrubý pás, preložený lem a dvojitú kapucňu otvorte pre prúdenie vzduchu; mokrú pružnú látku nenechajte visieť za úzky bod.",
    "heat_risk": "elastan, termoplastický polyester alebo polyamid, tepelnú fixáciu, potlač, lepidlo, objem vrstiev a návrat do tvaru",
    "failure_sign": "zvlnenie povrchu, presvitajúci elastan, stáčajúci sa šev alebo trvalo vytiahnutý úplet",
    "answer": "Scuba úplet je obchodné označenie hustejšieho, hladkého a tvarovo pevného odevného úpletu. Často ide o polyesterový dvojitý úplet s elastanom, no zloženie aj vnútorná stavba sa líšia. Na rozdiel od neoprénového obleku obyčajne nemá penové kaučukové jadro. Prateľný scuba odev obráťte naruby, oddeľte od zipsov a suchých zipsov, použite šetrný program a teplotu zo štítku, primeranú dávku a nízke mechanické zaťaženie. Po nosení ho nenechávajte spotený v taške. Dôkladne opláchnite a sušte bez prudkého tepla, pretože elastan a termoplastické vlákna môžu stratiť návrat alebo sa zdeformovať. Pri lepenom, potlačenom či penovom variante má prednosť presný návod výrobcu.",
    "intro": "Názov scuba zvádza k predstave neoprénu, no v móde často označuje úplne iný materiál: hustý dvojitý úplet bez penovej vrstvy. Používa sa na šaty, sukne, nohavice, saká aj voľnočasové oblečenie, pretože má hladký povrch, hrúbku a drží čistú siluetu. Jeho praktická slabina sa objaví po pote, nesprávnom sušení a vysokom teple. Hustota môže spomaliť oplach, polyester zachytáva mastné zvyšky a elastan neznáša každé prehrievanie. Bezpečná starostlivosť preto stojí na správnej identifikácii a nie na všeobecnom návode pre neoprén.",
    "quick": [
        "<strong>Scuba úplet nie je automaticky neoprén:</strong> odevný variant býva dvojitý úplet bez penového jadra.",
        "<strong>Obchodný názov nestačí:</strong> skontrolujte vlákna, smer pružnosti, vnútornú vrstvu a celý štítok.",
        "<strong>Pot nenechávajte zaschnúť v stlačenej látke:</strong> hneď po nosení odev rozložte a vyvetrajte.",
        "<strong>Primeraná dávka potrebuje dobrý oplach:</strong> hustý úplet môže zadržiavať maz aj nadbytok produktu.",
        "<strong>Teplo skracuje život pružnosti:</strong> sušičku, radiátor a horúcu žehličku použite len pri výslovnom povolení.",
        "<strong>Tvar kontrolujte až po vysušení:</strong> mokrá hmotnosť a elastická relaxácia môžu dočasne meniť rozmery.",
    ],
    "overview_heading": "Čo je scuba úplet a ako vzniká jeho pevný objem",
    "overview": [
        "Scuba úplet patrí medzi husté odevné pleteniny, pri ktorých sa dve strany vytvárajú a spájajú počas pletenia. Presná štruktúra môže byť interlocková, dvojlícna, spacerová alebo inak obchodne pomenovaná. CottonWorks vysvetľuje, že dvojité úplety vznikajú z dvoch prepojených pletených plôch, bývajú ťažšie, stabilnejšie a menej sa stáčajú než jednoduché úplety. Táto všeobecná vlastnosť pomáha pochopiť scuba, ale nenahrádza technický list konkrétnej látky.",
        "Typický módny scuba má hladké líce, pružnosť a mierne hubovitý omak bez gumovej peny. Polyester dodáva rýchle schnutie a tvarovú stabilitu, elastan pružnosť a návrat. Existujú však aj zmesi s viskózou, polyamidom alebo bavlnou. Hrúbka môže vznikať množstvom priadze, prepojením vrstiev alebo vnútornou vzduchovou štruktúrou; z pohľadu spotrebiteľa sa nedá spoľahlivo určiť iba stlačením medzi prstami.",
        "Názvy scuba, air layer a spacer sa v obchode používajú nejednotne. Spacer má definovanú tretiu spojovaciu sústavu medzi dvoma povrchmi, kým hustý dvojitý úplet môže byť spojený priamo očkami. Pre starostlivosť je podstatné, či medzi vrstvami zostáva voda, aké vlákno tvorí spoj a či bol materiál tepelne stabilizovaný. Pri pochybnosti žiadajte zloženie a návod, nie iba marketingový názov.",
    ],
    "table1_heading": "Scuba, neoprén, ponte a spacer: praktické rozdiely",
    "table1_intro": "Podobný objem a hladký vzhľad môžu skrývať odlišnú vnútornú stavbu. Rozlíšenie je dôležité pre pranie, sušenie aj žehlenie.",
    "table1_headers": ["Materiál", "Vnútorná stavba", "Typické použitie", "Najdôležitejšia hranica"],
    "table1_rows": [
        ("Scuba úplet", "Hustý dvojitý alebo príbuzný úplet, často polyester s elastanom.", "Šaty, sukne, nohavice, mikiny, ľahké saká.", "Chrániť pružnosť, potlač a tepelnú stabilitu; zaistiť úplný oplach."),
        ("Neoprén", "Penové elastomérne jadro laminované textilom, lepené a šité spoje.", "Potápačské obleky, návleky, bandáže, obaly.", "Neprenášať naň automaticky strojové pranie scuba úpletu."),
        ("Ponte di Roma", "Konkrétna stabilná dvojitá pletenina odvodená od interlocku.", "Nohavice, šaty, sukne a štruktúrované saká.", "Zloženie a elastan menia teplotu, žmolkovanie a návrat."),
        ("Spacer úplet", "Dve povrchové vrstvy spojené samostatnými vláknami alebo priadzami.", "Šport, obuv, výstuže a izolačné vrstvy.", "Voda a nečistota môžu zostať v priestore medzi lícami."),
    ],
    "sections": [
        {
            "heading": "Scuba úplet verzus neoprénový materiál",
            "paragraphs": [
                f"Neoprénový výrobok obsahuje penový elastomér a jeho vrstvy môžu byť laminované lepidlom. Scuba úplet je spravidla textilná pletenina bez tejto peny. Rozdiel rozoberá aj článok <a href=\"{ARTICLE_NEOPRENE}\">čo je neoprén a ako ho čistiť</a>. Ak medzi dvoma textilnými povrchmi vidíte súvislé gumové jadro, nepoužívajte automaticky návod pre odevný scuba úplet.",
                "Niektorí predajcovia používajú slovo neoprene pre módny scuba vzhľad a naopak. Rozhodujúce je fyzické zloženie, rez hrany, pružnosť a štítok. Penové jadro sa po stlačení vracia inak a môže mať lepené švy; dvojitý úplet ukazuje očká alebo prepojené textilné vrstvy. Pri hotovom kabátiku môže podšívka rez zakryť, preto sa nespoliehajte iba na dotyk.",
            ],
        },
        {
            "heading": "Scuba a ponte: dva pevné úplety, nie synonymá",
            "paragraphs": [
                "Ponte di Roma je konkrétna dvojitá konštrukcia s postupnosťou interlockových a jednostranných chodov. Scuba je širšie obchodné označenie hladkého objemového úpletu a nemusí mať rovnakú väzbu. Oba môžu obsahovať polyester, viskózu a elastan a držať tvar nohavíc, no líšia sa hrúbkou, vnútornou stavbou, pružnosťou a povrchovým dokončením.",
                f"Pri nákupe porovnajte rez hrany, líc a rub, návrat po natiahnutí a gramáž. Rozdiel medzi vláknom a konštrukciou v praxi vysvetľuje aj článok <a href=\"{ARTICLE_BLEND}\">polyester verzus bavlna</a>. Starostlivosť však aj pri rovnakom názve určuje hotový odev: pás, zips, lepená výstuž a potlač môžu vyžadovať nižšie teplo než samotná metráž.",
            ],
        },
        {
            "heading": "Ako prať scuba šaty a sukňu bez zvlnenia",
            "paragraphs": [
                "Pred praním skontrolujte bočné švy, pás, zips, podšívku, záševky a spodný lem. Odev obráťte naruby, ak tým nepoškodíte tvarovanú aplikáciu. Zips zatvorte, ostrý háčik prekryte a kus perte s podobne ľahkými hladkými textíliami. Hrubé uteráky a rifle pridávajú trenie aj tlakovú deformáciu a môžu vytiahnuť jemné povrchové očko.",
                "Použite program, teplotu a otáčky zo štítku. Scuba nepotrebuje prudký cyklus len preto, že pôsobí robustne. Po praní odev vyberte bez dlhého stlačenia, urovnajte švy na rovnej ploche a nenaťahujte zvlnený lem silou. Rozmer porovnávajte až po úplnom vysušení a ustálení elastických vlákien.",
            ],
        },
        {
            "heading": "Ako prať scuba nohavice, mikinu a športový kus",
            "paragraphs": [
                "Nohavice bývajú znečistené pri páse, rozkroku, kolenách a spodnom leme. Mikina zadržiava pot v podpazuší, manžetách a dvojitej kapucni. Tieto zóny lokálne predčistite kompatibilným prípravkom bez kefovania. Vrecká vyprázdnite, šnúrky zaistite a suché zipsy zatvorte tak, aby sa nedotýkali líca.",
                "Po športe kus čo najskôr rozložte. Mokrá hromada v taške predlžuje kontakt potu a kožného mazu s polyesterom a vytvára podmienky pre pach. Ak pranie odkladáte, nechajte obe strany voľne vetrať. Silná parfumácia neodstráni mastný film a pri nedostatočnom oplachu môže iba prekryť prvý dojem.",
            ],
        },
        {
            "heading": "Pot, mastný film a pach v hustom úplete",
            "paragraphs": [
                "Pot je prevažne voda a soli, no pri nosení sa mieša s kožným mazom, deodorantom a mikroorganizmami. Hydrofóbnejší polyester môže ľahko vyschnúť, ale mastné zvyšky ostávajú na povrchu priadze. Hustá konštrukcia navyše spomaľuje výmenu pracieho roztoku medzi vrstvami. Výsledkom môže byť odev, ktorý po uschnutí pôsobí čistý, no pri zahriatí znovu zapácha.",
                f"Riešením je primeraná dávka, dostatok priestoru, vhodný program, dôkladný oplach a rýchle úplné sušenie. Opakované predávkovanie môže vytvoriť ďalší film. Diagnostiku rozoberá článok <a href=\"{ARTICLE_ODOR}\">prečo oblečenie zapácha po praní</a>. Ak pach zostáva len v páse alebo výstuži, skontrolujte, či táto časť vôbec úplne vyschne.",
            ],
            "callout": {
                "title": "Rýchla diagnostika pachu scuba odevu",
                "items": [
                    "Zapácha odev už suchý, alebo až po zahriatí pri nosení?",
                    "Ostáva klzký, tuhý alebo nerovnomerne voňavý po oplachu?",
                    "Schne pás, kapucňa a preložený lem dlhšie než hladká plocha?",
                    "Ležal spotený v taške alebo koši pred praním?",
                    "Je bubon čistý a nebola náplň príliš plná na dôkladný oplach?",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Elastan, tepelná fixácia a strata návratu",
            "paragraphs": [
                "Malý podiel elastanu môže výrazne ovplyvniť pružnosť a tvar. Dlhé pôsobenie vysokého tepla, chlórového bielidla, olejov a mechanického namáhania urýchľuje jeho starnutie. Keď sa elastické vlákna poškodia, na povrchu môžu presvitať svetlé nite alebo látka zostane zvlnená. Ďalší horúci cyklus návrat neobnoví.",
                "Polyester sa pri výrobe často tepelne stabilizuje, aby si úplet držal rozmer. Domáca žehlička však môže lokálne prekročiť vhodnú hranicu a vytvoriť lesk alebo zvlnenie. Pri šve sa teplo prenáša inak než na voľnej ploche. Žehlite iba pri povolení z rubu cez ochrannú tkaninu, bez naťahovania látky pod žehličkou.",
            ],
        },
        {
            "heading": "Žmolky, oder a povrchové vytiahnutie očiek",
            "paragraphs": [
                "Hladký scuba povrch môže časom žmolkovať v podpazuší, medzi stehnami, pri kabelke alebo pod pásom. Žmolok vzniká uvoľnením vlákien, ich zamotaním a zotrvaním na povrchu; polyesterové vlákna môžu uzlík držať dlhšie. Odstránenie strojčekom zlepší vzhľad, ale zároveň odoberá materiál a nerieši zdroj trenia.",
                f"Suchý zips alebo poškodený necht môže vytiahnuť celé očko. Neodstrihujte ho naslepo a neťahajte. Viac o mechanizme nájdete v článkoch <a href=\"{ARTICLE_PILLING}\">prečo sa oblečenie žmolkuje</a> a <a href=\"{ARTICLE_SNAGGING}\">ako vzniká zatrhávanie</a>. Pri rozbiehajúcom sa šve treba najprv opravu, až potom ďalší cyklus.",
            ],
        },
        {
            "heading": "Sušenie hrubého úpletu bez vytiahnutia",
            "paragraphs": [
                "Mokrý scuba odev je ťažší a môže sa predĺžiť, ak visí za úzke ramienka alebo jednu štipku. Šaty a nohavice urovnajte naplocho alebo použite širokú oporu podľa etikety. Kapucňu, pás a vrecká otvorte. Kus počas sušenia otočte bez krútenia, aby prúd vzduchu dosiahol obe strany a vnútorné švy.",
                "Radiátor a horúci fén sušia nerovnomerne. Vonkajšia strana sa môže prehriať, kým medzi vrstvami ostane voda. Sušičku použite iba pri výslovnom symbole a po zvážení elastanu, potlače a lepidla. Ak je kus po vysušení zvlnený, nechajte ho najprv ustáliť pri izbovej teplote; ďalšie teplo môže deformáciu zafixovať.",
            ],
        },
        {
            "heading": "Ako žehliť alebo napariť scuba úplet",
            "paragraphs": [
                "Mnohé scuba odevy sa pri správnom sušení nemusia žehliť. Záhyb najprv uvoľnite urovnaním a časom. Ak etiketa povoľuje žehlenie, začnite na skrytom mieste, pracujte z rubu cez hladkú bavlnenú tkaninu a používajte nízke vhodné teplo. Netlačte šev do lesklej stopy a neposúvajte žehličku po napnutom úplete.",
                "Para pridáva teplo aj kondenzovanú vodu. Pri lepenom detaile, potlači alebo vnútornej vzduchovej vrstve môže pôsobiť nerovnomerne. Naparovač držte v bezpečnej vzdialenosti podľa návodu a odev počas zásahu nenaťahujte. Ak sa povrch začne vlniť, lesknúť alebo lepiť, okamžite prestaňte a nechajte materiál vychladnúť bez dotyku.",
            ],
        },
        {
            "heading": "Ako vybrať kvalitný scuba úplet",
            "paragraphs": [
                "Vzorku natiahnite v smere šírky aj dĺžky a sledujte, ako rýchlo a rovnomerne sa vráti. Pozrite sa proti svetlu, či pri napnutí nepresvitajú elastické nite. Prehnite ju a skontrolujte, či sa nevytvorí biela čiara. Rez hrany ukáže, či ide o textilný dvojitý úplet, spacer alebo penovú lamináciu. Tieto skúšky sú orientačné, ale odhalia nejasné označenie.",
                "Pri hotovom odeve skúšajte pohyb, sedenie a zaťaženie švov, nie iba statický vzhľad. Hrubá látka môže byť teplá a menej priedušná, veľmi pružná zas môže pri vreckách a kolenách povoľovať. Kvalitná etiketa má uviesť zloženie a starostlivosť. Prívlastky prémiový, technický alebo neoprene look nie sú náhradou za tieto údaje.",
            ],
        },
    ],
    "table2_heading": "Scuba úplet po praní: čo znamená zmena",
    "table2_intro": "Rovnaký prejav môže mať mechanickú, chemickú alebo tepelnú príčinu. Hodnoťte úplne suchý odev a porovnajte ho s miestom bez zaťaženia.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Odev zostal vytiahnutý", "Mokré zavesenie, poškodený elastan, preťaženie alebo rozmerová zmena.", "Švy, pás, smer pružnosti a stav po 24 hodinách.", "Uložiť naplocho; nepridávať teplo, ktoré môže poškodenie zafixovať."),
        ("Povrch sa vlní alebo leskne", "Lokálne teplo, tlak, potlač alebo rozdielna relaxácia vrstiev.", "Rub, miesto pri šve a symbol žehlenia.", "Nežehliť znova; nechať vychladnúť a zdokumentovať zmenu."),
        ("Odev po zahriatí zapácha", "Zvyšný maz, mikroorganizmy, predávkovanie alebo nedosušený pás.", "Dávku, náplň, oplach, čistotu práčky a hrubé miesta.", "Upraviť pranie a sušenie podľa štítku, nie prekrývať pach."),
        ("Na povrchu sú žmolky", "Trenie pri nosení, drsná náplň alebo uvoľnené vlákna.", "Miesta kontaktu s taškou, stolom, pásom a suchým zipsom.", "Obmedziť trenie; žmolky odstraňovať opatrne bez zásahu do očiek."),
        ("Dve vrstvy schnú nerovnako", "Hrubý alebo spacerový úplet, preloženie, podšívka či nedostatok vzduchu.", "Kapucňu, pás, vrecká, lem a vnútorné švy.", "Otvoriť vrstvy, otočiť kus a zvýšiť prúdenie bez prudkého tepla."),
    ],
    "steps_heading": "Ako bezpečne vyprať scuba úplet krok za krokom",
    "steps": [
        "Overte, že ide o textilný scuba úplet, nie penový neoprén alebo lepený laminát.",
        "Prečítajte zloženie, symboly, smer pružnosti a obmedzenia potlače, podšívky, pásu a zipsu.",
        "Odev vyvetrajte, lokálne ošetrite pot a mastnotu a oddeľte ho od drsných či ťažkých kusov.",
        "Zapnite bezpečné kovanie, obráťte odev podľa konštrukcie a zvoľte povolený šetrný program.",
        "Dávkujte podľa tvrdej vody, náplne a znečistenia a ponechajte priestor na prietok a oplach.",
        "Po cykle kus podoprite, urovnajte švy bez naťahovania a otvorte hrubé vrstvy.",
        "Sušte v tieni s prúdením vzduchu; sušičku, radiátor a žehlenie použite len pri povolení.",
        "Rozmer, pružnosť, pach a povrch posudzujte až po úplnom vysušení a ustálení.",
    ],
    "remember": [
        "Vidíte v reze očká a textilné vrstvy, alebo súvislé penové jadro?",
        "Aké vlákna a aký podiel elastanu uvádza etiketa?",
        "Je pružnosť rovnaká v oboch smeroch a vracia sa materiál bez zvlnenia?",
        "Obsahuje odev potlač, lepidlo, výstuž, podšívku alebo spacerovú vnútornú vrstvu?",
        "Má bubon dostatok priestoru na oplach hustého úpletu?",
        "Schnú pás, kapucňa, lem a vrecká rovnako rýchlo ako hladká plocha?",
    ],
    "mistakes": [
        "Zameniť scuba úplet s neoprénom a použiť nesprávny návod.",
        "Nechať spotený odev niekoľko dní stlačený v športovej taške.",
        "Predávkovať gél v preplnenom bubne a prekryť zvyšný pach vôňou.",
        "Prať hladký úplet so suchým zipsom, hrubým uterákom alebo otvoreným háčikom.",
        "Sušiť mokré šaty za úzke ramienka alebo priamo na radiátore.",
        "Žehliť zvlnenie vysokým teplom a trvalo poškodiť elastan alebo polyester.",
    ],
    "expert_heading": "Odbornejší pohľad: dvojitý úplet, rozmer a funkčné skúšky",
    "expert": [
        "CottonWorks opisuje dvojité úplety ako dve pletené plochy spojené počas výroby. Používajú viac priadze, bývajú stabilnejšie a menej sa stáčajú než jednoduchý jersey. To vysvetľuje tvarový charakter scuba, ale nie každý detail: konkrétne usporiadanie očiek, plating, spojovacia priadza a tepelné dokončenie musia byť známe z technického listu.",
        "Ponte di Roma je normálne rozpoznateľná dvojitá konštrukcia s konkrétnym sledom chodov, zatiaľ čo scuba zostáva širším obchodným názvom. Preto nemožno laboratórnu hodnotu jedného ponte preniesť na každý scuba odev. AATCC TM135 pomáha merať rozmerovú zmenu po definovanom praní, ASTM D3512 náhodné žmolkovanie a ASTM D3939 zachytenie; každá metóda hodnotí inú poruchu.",
        "Pri pachu je dôležité oddeliť rýchlosť schnutia vlákna od odstránenia kožného mazu. Polyester prijíma málo vody do vnútra vlákna, no olejové nečistoty sa môžu držať na povrchu. Hustá štruktúra mení prietok vody a čas sušenia medzi vrstvami. Dobrá domáca rutina preto kombinuje skoré vetranie, mechanicky primeraný cyklus, presnú dávku, oplach a prúdenie vzduchu.",
    ],
    "source_intro": "Zdroje podporujú opis dvojitých úpletov, rozdiel od ponte, normované meranie rozmerov, žmolkovania, zachytávania a význam štítku. Obchodný názov scuba sám neurčuje presnú konštrukciu.",
    "sources": [
        ("CottonWorks: jednoduché a dvojité úplety", COTTONWORKS_DOUBLE),
        ("CottonWorks: konštrukcia Ponte di Roma", COTTONWORKS_PONTE),
        ("AATCC TM135-2025: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("ASTM D3512/D3512M-22: náhodné žmolkovanie", ASTM_PILLING),
        ("ASTM D3939/D3939M-26: odolnosť proti zachyteniu", ASTM_SNAG),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Pri bežnom prateľnom scuba odeve môže byť prací gél vhodný, ak ho povoľujú vlákna, elastan, potlač a štítok. Penový neoprén, membrána alebo špeciálny spacer môže potrebovať iný prípravok.",
    "product_text": "Hypoalergénny prací gél z marseillského mydla je konkrétna voľba pre kompatibilnú bežnú bielizeň. Pri hustom scuba úplete dávkujte presne a nechajte dostatok priestoru na oplach potu, kožného mazu a produktu.",
    "product_limit": "Produkt nie je automaticky vhodný na penový neoprén, membránu, lepený laminát, nestálofarebnú potlač ani profesionálne čistený odev. Neobnoví poškodený elastan alebo zvlnenie po vysokom teple.",
    "category_intro": "Pri porovnaní gélov sledujte kompatibilitu s polyesterom, polyamidom, viskózou, bavlnou a elastanom. Silnejšia vôňa nie je náhradou za odstránenie mastného filmu a úplné sušenie.",
    "category_text": "V kategórii pracích gélov nájdete produkty pre bežnú domácu bielizeň. Vyberte ich až po kontrole štítku scuba odevu a používajte dávku primeranú vode, náplni a miere znečistenia.",
    "related": [
        ("Čo je neoprén a ako ho čistiť", ARTICLE_NEOPRENE),
        ("Polyester verzus bavlna", ARTICLE_BLEND),
        ("Čo je polyester a ako ho prať", ARTICLE_POLYESTER),
        ("Prečo oblečenie zapácha po praní", ARTICLE_ODOR),
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
    ],
    "faq_title": "scuba úplet a jeho starostlivosť",
    "faq": [
        ("Čo je scuba úplet?", "Hustý hladký a tvarovo pevný odevný úplet, často dvojitý a vyrobený z polyesteru s elastanom."),
        ("Je scuba to isté ako neoprén?", "Nie automaticky. Módny scuba býva textilný úplet bez penového jadra, kým neoprénový výrobok obsahuje elastomérnu penu."),
        ("Aký je rozdiel medzi scuba a ponte?", "Ponte di Roma je konkrétna dvojitá pletenina. Scuba je širšie obchodné označenie objemového hladkého úpletu."),
        ("Môže sa scuba prať v práčke?", "Áno iba pri povolení štítku. Zvoľte vhodný šetrný program, nízke trenie, primeranú dávku a dobrý oplach."),
        ("Na koľko stupňov prať scuba látku?", "Jedna teplota neexistuje. Rozhoduje presné zloženie, elastan, potlač, lepidlo a etiketa hotového odevu."),
        ("Prečo scuba odev zapácha?", "Príčinou môže byť pot, kožný maz na polyesteri, predávkovanie, preplnený bubon alebo nedosušený pás a švy."),
        ("Môže ísť scuba do sušičky?", "Len pri výslovnom symbole. Teplo môže poškodiť elastan, potlač a tepelnú stabilitu úpletu."),
        ("Ako vyžehliť scuba šaty?", "Najprv ich správne usušte. Ak je žehlenie povolené, pracujte z rubu cez ochrannú tkaninu s nízkym vhodným teplom."),
        ("Prečo sa scuba po praní vlní?", "Môže ísť o tepelné poškodenie, rozdielnu relaxáciu vrstiev, mokré vytiahnutie alebo poškodený elastan. Ďalšie teplo nepridávajte naslepo."),
    ],
}


PIQUE: dict[str, object] = {
    "title": "Čo je piké: reliéfna pletenina, pórovitosť a správne pranie",
    "link": "co-je-pike-reliefna-pletenina-porovitost-a-spravne-pranie",
    "meta": "Čo je piké, prečo má reliéfny povrch a ako prať piké polokošeľu, šaty či prikrývku bez zrazenia, žmolkov a zvlneného goliera.",
    "short": "Piké je reliéfna textilná konštrukcia známa najmä z polo tričiek. Najčastejší pletený variant kombinuje pletené a záchytné očká, no existuje aj tkané piké. Zistite, ako chrániť povrch, golier, rozmer a farbu pri praní a sušení.",
    "name": "piké",
    "genitive": "piké",
    "locative": "piké",
    "construction_summary": "reliéfnu štruktúru vytvorenú kombináciou očiek a záchytných očiek v pletenine alebo kordmi a výplňovými niťami v tkanom variante",
    "label_details": "či ide o pletené alebo tkané piké, percentá bavlny, polyesteru a elastanu, hustotu reliéfu, golier, manžety, výšivku, nášivku, gombíky a prípadnú protižmolkovú či nekrčivú úpravu",
    "residue_place": "jamkách reliéfu, medzi očkami, v golieri, manžetách, lége s gombíkmi a v hrubšej výšivke",
    "friction_risk": "vystupujúce slučky a rebrá, očká pri lége, výšivku alebo elastický okraj goliera",
    "drying_advice": "Polo tričko alebo šaty vytraste, urovnajte bočné švy, golier, légu a manžety a sušte naplocho alebo na primeranej opore podľa etikety. Ťažšiu prikrývku rozložte rovnomerne a počas sušenia ju otočte, aby reliéf a lemy nezostali vlhké.",
    "heat_risk": "bavlnenú rozmerovú zmenu, elastan, polyester, reliéfne očká, golierovú výstuž, výšivku a tvar bočných švov",
    "failure_sign": "vytiahnuté očko, zvlnený golier, stáčajúci sa bočný šev alebo deformovaná výšivka",
    "answer": "Piké je názov textilnej konštrukcie s drobným reliéfom, jamkami alebo rebrami. V polo tričkách ide najčastejšie o pleteninu, v ktorej sa striedajú pletené a záchytné očká; existuje však aj tkané piké. Materiál môže byť bavlnený, polyesterový alebo zmesový, takže pranie neurčuje samotný názov. Piké polokošeľu perte podľa štítku obrátenú naruby, so zapnutou alebo zaistenou légou, oddelene od suchých zipsov a drsných textílií. Použite primeranú dávku, neprepĺňajte bubon a po cykle urovnajte golier, manžety a bočné švy. Vysoké teplo môže zraziť bavlnu, poškodiť elastan a zafixovať zvlnenie. Reliéf nežehlite silným tlakom; pri povolení pracujte z rubu a nechajte štruktúre mäkkú podložku.",
    "intro": "Piké si väčšina ľudí spája s polokošeľou, no povrch s drobnými jamkami nie je jedno vlákno ani jediný typ látky. Štruktúra môže zlepšiť prúdenie vzduchu na povrchu a maskovať drobné záhyby, ale sama nezaručuje, že odev bude chladný, savý alebo odolný. O tom rozhoduje priadza, hustota, gramáž, zloženie a strih. Pri praní sú najcitlivejšie vystupujúce očká, golier, léga, výšivka a bočné švy. Praktický výsledok preto stojí na triedení náplne, správnej dávke, šetrnom sušení a vyrovnaní odevu ešte pred zaschnutím.",
    "quick": [
        "<strong>Piké je štruktúra, nie vlákno:</strong> môže byť bavlnené, polyesterové, zmesové, pletené aj tkané.",
        "<strong>Polo tričko používa najčastejšie pletené piké:</strong> reliéf vzniká kombináciou pletených a záchytných očiek.",
        "<strong>Pórovitý vzhľad nie je dôkaz priedušnosti:</strong> rozhoduje hustota, priadza, dokončenie a celý odev.",
        "<strong>Golier potrebuje tvarovanie po praní:</strong> vysoké otáčky a teplo môžu zvlnenie zhoršiť.",
        "<strong>Reliéf zachytáva trenie aj zvyšky:</strong> perte bez suchých zipsov a s dostatočným oplachom.",
        "<strong>Bavlna sa môže zraziť:</strong> teplota a sušička musia vychádzať zo štítku, nie z farby trička.",
    ],
    "overview_heading": "Čo je piké a ako vzniká jeho typický reliéf",
    "overview": [
        "CottonWorks opisuje piké ako látku s kordmi alebo rebrami a pri pletených variantoch uvádza kombináciu pletených a záchytných očiek. Jednoduché piké môže striedať tieto očká v určitých podávačoch, čím vzniknú drobné jamky a textúra. Pojem preto zahŕňa rodinu konštrukcií, nie jeden univerzálny recept. Single piqué, double cross-tuck a ďalšie varianty sa líšia hĺbkou reliéfu, stabilitou a spotrebou priadze.",
        "Vystupujúce a zapustené miesta menia kontakt s pokožkou aj vzhľad. Medzi telom a plochou môžu vznikať malé vzduchové priestory, no skutočnú priedušnosť určuje odpor celej látky proti prúdeniu vzduchu. Husté polyesterové piké môže byť menej vzdušné než ľahká bavlnená plátnová tkanina a ťažké bavlnené piké môže po nasiaknutí schnúť dlhšie než tenký jersey.",
        "Polo tričko pridáva golier, manžety, légu, gombíky a často výšivku. Tieto časti môžu byť upletené inou konštrukciou a z inej zmesi než telo odevu. Kvalitu preto nehodnoťte iba dotykom hlavnej plochy. Sledujte symetriu goliera, pevnosť légy, čistotu očiek, bočný šev a to, či sa reliéf po jemnom natiahnutí rovnomerne vráti.",
    ],
    "table1_heading": "Druhy piké a ich praktické použitie",
    "table1_intro": "Názov na etikete môže označovať odlišnú výrobnú cestu. Nasledujúce rozdelenie pomáha určiť, čo pri ošetrovaní sledovať.",
    "table1_headers": ["Variant", "Ako vzniká povrch", "Typické použitie", "Riziko pri praní"],
    "table1_rows": [
        ("Pletené polo piké", "Kombinácia pletených a záchytných očiek vytvára jamky.", "Polokošele, tričká, športové šaty.", "Vytiahnuté očko, žmolky, zrazenie a zvlnený golier."),
        ("Dvojité alebo viacpodávačové piké", "Komplexnejší sled očiek vytvára plnší a stabilnejší reliéf.", "Ťažšie polo tričká, mikiny, štruktúrované úplety.", "Dlhšie schnutie, zvyšky produktu a nerovnaká rozmerová zmena."),
        ("Tkané piké", "Kordy, rebrá alebo výplňové nite v tkanine.", "Šaty, vesty, formálnejšie textílie a bytové použitie.", "Posun nití, ostrý lom a rozdielna reakcia výplne."),
        ("Vafľový alebo voštinový vzhľad", "Iná pletená či tkaná bunková štruktúra, nie vždy pravé piké.", "Uteráky, prikrývky, župany a športové vrstvy.", "Zachytenie slučiek, zrazenie a deformácia buniek."),
    ],
    "sections": [
        {
            "heading": "Pletené piké verzus tkané piké",
            "paragraphs": [
                "Pletené piké sa skladá z očiek a spravidla pruží viac do šírky. Pri pohľade zblízka vidíte stĺpiky a slučky. Tkané piké má osnovné a útkové nite a reliéf vytvára väzba, kord alebo doplnková niť. Obe látky môžu mať jamkovitý vzhľad, ale mokrá rozmerová zmena, strapkanie okraja a oprava šva sa líšia.",
                "Pri hotovom výrobku nemusí byť typ uvedený slovom. Jemne roztiahnite skryté miesto, prezrite rub a rezervačný šev. Pletenina ukáže očká a väčšiu pružnosť, tkanina mriežku nití a obmedzenejší pohyb. Tento domáci pohľad je orientačný; presné zloženie a povolenú starostlivosť stále určuje etiketa.",
            ],
        },
        {
            "heading": "Bavlnené, polyesterové a zmesové piké",
            "paragraphs": [
                "Bavlna prijíma viac vody do vlákna, pôsobí príjemne pri pokožke a môže sa pri nevhodnom praní alebo sušení zraziť. Polyester schne rýchlejšie a môže držať tvar, no mastný film a pach sa na jeho povrchu niekedy odstraňujú ťažšie. Zmes kombinuje vlastnosti, ale výsledok nie je jednoduchý priemer; priadza, podiely a dokončenie menia omak aj životnosť.",
                f"Rozdiely rozoberá článok <a href=\"{ARTICLE_BLEND}\">polyester verzus bavlna pri nosení a praní</a>. Elastan v golieri alebo tele pridáva pružnosť, zároveň znižuje toleranciu voči vysokému teplu. Pri farebnom polo tričku sledujte aj farbivo a výšivku. Rovnaká teplota nemusí byť vhodná pre biele bavlnené a tmavé zmesové piké.",
            ],
        },
        {
            "heading": "Ako prať piké polo tričko",
            "paragraphs": [
                "Vyprázdnite vrecko, zapnite gombíky iba tak, aby sa léga voľne pohybovala, a ostrý okraj či kovový znak chráňte. Odev obráťte naruby, ak výšivka a nášivka umožňujú bezpečné otočenie. Golier neprekladajte do tvrdého lomu. Trieďte podľa farby, zloženia a mechanickej záťaže; nové sýte farby perte spočiatku oddelene.",
                "Použite povolenú teplotu a program. Bubon neprepĺňajte, aby voda prešla cez jamky, golier a légu. Vysoké otáčky môžu zafixovať pokrčenie goliera a namáhať výšivku. Po cykle tričko hneď vyberte, jemne pretrepte, urovnajte bočné švy, légu, manžety a golier bez vyťahovania mokrej pleteniny.",
            ],
        },
        {
            "heading": "Golier a manžety: prečo sa vlnia a krútia",
            "paragraphs": [
                "Golier býva rebrovaný, vystužený alebo upletený inou hustotou než telo trička. Pri praní môže meniť rozmer iným tempom. Vysoké teplo, prudké odstreďovanie, ťahanie pri obliekaní a dlhé zavesenie za špičky vytvárajú vlny. Ak sa mení iba jedna strana, skontrolujte aj šev a rovnomernosť prišitia.",
                "Po praní golier položte do prirodzenej polohy a hranou dlane ho jemne urovnajte bez rozťahovania. Nezaťažujte ho štipcami. Žehlenie používajte len pri povolení a cez mäkkú podložku; silný tlak môže sploštiť reliéf tela a vytvoriť lesk na polyesteri. Trvalo poškodené elastické priadze žehlička neopraví.",
            ],
        },
        {
            "heading": "Škvrny na golieri, podpazuší a pri gombíkoch",
            "paragraphs": [
                "Golier zachytáva kožný maz, opaľovací krém a make-up, podpazušie pot a deodorant. Pred praním miesto navlhčite iba podľa povolenia a naneste kompatibilný prostriedok v primeranom množstve. Reliéf nekefujte tvrdou kefou, pretože sa očká môžu vytiahnuť a povrch zbelie. Tlak prstov rozložte na väčšiu plochu.",
                "Pri lége skontrolujte zadnú stranu gombíka a výstuž. Kovový znak môže reagovať s bielidlom a farba výšivky môže pustiť. Fľak po deodorante nie je vždy iba soľ; môže obsahovať vosky, oleje a pigment. Ak sa farba pri skrytej skúške prenáša na bielu handričku, agresívnejší zásah zastavte.",
            ],
        },
        {
            "heading": "Priedušnosť a savosť: čo reliéf skutočne dokáže",
            "paragraphs": [
                "Jamky zmenšujú súvislú kontaktnú plochu s pokožkou a môžu vytvoriť malé vzduchové kanály. Samy však nezaručia vysokú priepustnosť vzduchu. Hustota očiek, hrúbka priadze, gramáž, dokončenie, potlač a strih môžu efekt zosilniť alebo potlačiť. Tesné polo z hrubého polyesterového piké môže byť teplejšie než voľné tričko z ľahkého jersey.",
                "Savosť opisuje prijímanie kvapaliny, priedušnosť prietok vzduchu a odvod vlhkosti transport po ploche alebo cez hrúbku. Ide o rôzne vlastnosti. Pri výbere na leto skúste odev na tele, zohľadnite farbu a strih a hľadajte merateľné údaje, ak výrobca tvrdí technický výkon. Reliéf je konštrukčný znak, nie laboratórny certifikát komfortu.",
            ],
            "callout": {
                "title": "Čo porovnať pri výbere polo trička",
                "items": [
                    "Presné zloženie tela, goliera, manžiet a prípadného elastického lemu.",
                    "Gramáž a hustotu, nie iba veľkosť jamiek na povrchu.",
                    "Návrat goliera a tela po jemnom natiahnutí.",
                    "Pevnosť légy, gombíkov, výšivky a bočných švov.",
                    "Teplotu, sušičku a žehlenie uvedené na štítku ešte pred nákupom.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Zrazenie, skrútený bočný šev a zmena dĺžky",
            "paragraphs": [
                "Bavlnená pletenina môže po uvoľnení výrobných napätí a pôsobení tepla zmeniť rozmer. Ak sa stĺpiky očiek alebo priadza stáčajú, bočný šev sa môže po praní posunúť dopredu. Sušička a vysoká teplota zmenu často zvýraznia. Rozmer merajte na suchom ustálenom tričku medzi rovnakými bodmi, nie na mokrom odeve.",
                f"Príčiny rozmerovej zmeny vysvetľuje článok <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie zráža po praní</a>. Jedným cyklom nemožno spoľahlivo odlíšiť chybu látky od nevhodnej starostlivosti bez údajov o pôvodnom rozmere a postupe. Pri novom kuse fotografujte štítok a meranie ešte pred ďalším pokusom s horúcou vodou.",
            ],
        },
        {
            "heading": "Žmolky, vytiahnuté očká a poškodený reliéf",
            "paragraphs": [
                "Vystupujúce časti piké sa dotýkajú okolia skôr než zapustené jamky. Trenie pod rukami, bezpečnostným pásom a batohom uvoľňuje vlákna a môže tvoriť žmolky. Suchý zips alebo trieska zachytí očko a vytiahne dlhšiu slučku. Perte preto s hladkými textíliami a pred cyklom skontrolujte bubon aj ostatné odevy.",
                f"Žmolok odstráňte až na suchom napnutom povrchu s minimálnym zásahom. Vytiahnuté očko neodstrihujte, kým neviete, kam vedie. Súvisiace mechanizmy rozoberajú návody <a href=\"{ARTICLE_PILLING}\">prečo vznikajú žmolky</a> a <a href=\"{ARTICLE_SNAGGING}\">ako predchádzať zatrhávaniu</a>. Rozbiehajúcu sa dierku opravte pred praním.",
            ],
        },
        {
            "heading": "Sušenie, vešanie a žehlenie reliéfu",
            "paragraphs": [
                "Polo tričko sušte podľa etikety naplocho alebo na širokej opore, aby sa ramená nevytiahli. Štipce nedávajte na golier a ťažký mokrý kus nenechajte visieť za spodný lem. Pri prikrývke rozložte hmotnosť na viac bodov a počas sušenia ju otočte. Hrubé švy a lem skontrolujte pred uložením.",
                "Ak je žehlenie povolené, otočte piké naruby a podložte ho mäkkou čistou tkaninou. Silný tlak na tvrdej doske sploští jamky a môže vytvoriť lesklé miesta. Golier žehlite samostatne podľa jeho zloženia a bez naťahovania. Para nie je vhodná, ak ju zakazuje výšivka, lepená nášivka alebo syntetická výstuž.",
            ],
        },
        {
            "heading": "Ako prať piké prikrývku alebo bytový textil",
            "paragraphs": [
                "Veľký kus nasaje viac vody a jeho reálna mokrá hmotnosť môže prekročiť možnosti malej práčky. Skontrolujte kapacitu bubna, švy, strapce a pokyny na sušenie. Prikrývka potrebuje priestor, aby sa prací roztok dostal cez reliéf a úplne opláchol. Ak sa v bubne iba pevne zvinie, väčšia dávka prostriedku výsledok nezlepší.",
                "Po praní ju dvíhajte s oporou oboch rúk a nekrúťte. Sušte rozloženú s prúdením vzduchu a pravidelne meňte kontaktnú plochu. Vafľový vzhľad nemusí byť pravé piké a môže mať iný stupeň zrazenia. Pri dekoratívnej výplni, lepení alebo kombinovanom leme sa riaďte najcitlivejšou časťou výrobku.",
            ],
        },
    ],
    "table2_heading": "Piké po praní: ako nájsť príčinu problému",
    "table2_intro": "Reliéf môže skryť zvyšok produktu aj zvýrazniť mechanickú chybu. Najprv odev úplne vysušte a porovnajte symetrické miesta.",
    "table2_headers": ["Prejav", "Možná príčina", "Kontrola", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Golier sa vlní", "Rozdielne zrazenie, poškodený elastan, prudké otáčky alebo mokré zavesenie.", "Šev, symetriu, zloženie goliera a teplotu.", "Urovnať za vlhka bez ťahu; nepridávať vysoké teplo."),
        ("Tričko je kratšie alebo širšie", "Relaxačné zrazenie, sušička, teplo alebo skrútenie pleteniny.", "Suchý rozmer medzi rovnakými bodmi a bočné švy.", "Zmeniť postup podľa štítku; nový kus zdokumentovať pred ďalším cyklom."),
        ("Povrch je tuhý", "Nadbytok produktu, tvrdá voda, nedostatočný oplach alebo presušenie.", "Dávku, náplň, jamky a rovnomernosť omaku.", "Pri povolení opláchnuť; nezvyšovať automaticky dávku ani vôňu."),
        ("Objavili sa žmolky", "Trenie pri nosení alebo pranie s drsnými kusmi.", "Podpazušie, pás, batoh a kontakt so suchým zipsom.", "Odstrániť zdroj trenia a povrch ošetrovať minimálne."),
        ("Očko je vytiahnuté", "Háčik, trieska, zips, prsteň alebo ostrý okraj bubna.", "Či niť pokračuje do okolitej štruktúry.", "Neodstrihovať; slučku stabilizovať alebo odborne zatiahnuť na rub."),
    ],
    "steps_heading": "Ako bezpečne vyprať piké krok za krokom",
    "steps": [
        "Určite, či ide o pletené alebo tkané piké, a prečítajte zloženie tela, goliera a manžiet.",
        "Skontrolujte škvrny, očká, légu, gombíky, výšivku a bočné švy a nový farebný kus oddeľte.",
        "Obráťte odev podľa konštrukcie, zabezpečte kovanie a neperte ho so suchými zipsami a uterákmi.",
        "Zvoľte povolenú teplotu, program a otáčky a nechajte bubon dostatočne voľný na oplach reliéfu.",
        "Po cykle odev ihneď vyberte a bez naťahovania urovnajte golier, manžety, légu a bočné švy.",
        "Sušte naplocho alebo na širokej opore podľa štítku; hrubé lemy a výšivku nechajte úplne vyschnúť.",
        "Pri povolenom žehlení pracujte z rubu cez mäkkú podložku a reliéf nesplošťujte silným tlakom.",
        "Suchý odev skontrolujte na symetriu, rozmer, pach, žmolky a vytiahnuté očká pred uložením.",
    ],
    "remember": [
        "Je povrch pletený z očiek alebo tkaný z osnovy a útku?",
        "Aké zloženie má telo, golier, manžety a elastický lem?",
        "Sú gombíky, výšivka, nášivka a léga bezpečné pre zvolený cyklus?",
        "Je nový sýtofarebný kus stálofarebný a oddelený od svetlej náplne?",
        "Má bubon priestor na pohyb vody cez reliéf a hrubšie časti?",
        "Sú golier, lem a výšivka úplne suché pred uložením?",
    ],
    "mistakes": [
        "Považovať piké za jedno vlákno a ignorovať bavlnu, polyester, elastan alebo tkaný variant.",
        "Prať polo tričko so suchými zipsami a vytiahnuť vystupujúce očká.",
        "Drhnúť golier tvrdou kefou a poškodiť reliéf alebo farbu.",
        "Preplniť bubon, predávkovať a nechať produkt v jamkách a golieri.",
        "Sušiť bavlnené piké vysokým teplom a potom násilne naťahovať rozmer.",
        "Sploštiť reliéf silnou žehličkou alebo vytiahnuť mokré ramená úzkym vešiakom.",
    ],
    "expert_heading": "Odbornejší pohľad: tuck očká, prúdenie vzduchu a skúšky",
    "expert": [
        "Pri záchytnom očku ihla drží staré očko a prijme novú priadzu bez okamžitého odhodenia. Tým sa v štruktúre zhromaždí viac priadze, susedné stĺpiky sa stiahnu a vznikne reliéf. Zmena počtu a rozloženia tuck očiek mení hmotnosť, šírku, pórovitosť a stabilitu. Preto sa dva úplety predávané ako piké nemusia správať rovnako.",
        "Priedušnosť sa meria pri definovanom tlakovom rozdiele a ploche; domáci pohľad cez jamky ju nenahradí. Savosť, transport kvapaliny a čas schnutia sú ďalšie samostatné vlastnosti. Bavlnené vlákno môže nasať viac vody, polyesterová konštrukcia ju zas rozviesť po povrchu. Komfort vzniká z kombinácie materiálu, štruktúry, strihu a podmienok nosenia.",
        "AATCC TM135 sleduje rozmerovú zmenu, AATCC TM61 zmenu farby a povrchu pri definovanom praní a ASTM D3512 žmolkovanie. Výsledky sa nedajú nahradiť jedným hodnotením mäkkosti. Pre polo tričko je navyše dôležitý rozdiel medzi telom, golierom a výšivkou. Spotrebiteľský návod preto pracuje s najcitlivejším komponentom hotového odevu.",
    ],
    "source_intro": "Zdroje podporujú definíciu pleteného a tkaného piké, hodnotenie rozmerovej zmeny, stálofarebnosti, žmolkovania a význam etikety. Reliéf sám nepreukazuje priedušnosť ani univerzálnu kvalitu.",
    "sources": [
        ("CottonWorks: textilná encyklopédia piké", COTTONWORKS_PIQUE),
        ("CottonWorks: jednoduché a dvojité úplety", COTTONWORKS_DOUBLE),
        ("AATCC TM135-2025: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("AATCC TM61: zrýchlená stálofarebnosť pri praní", AATCC_COLOR),
        ("ASTM D3512/D3512M-22: náhodné žmolkovanie", ASTM_PILLING),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Pri bežnom prateľnom bavlnenom alebo zmesovom piké môže byť prací gél vhodný, ak ho povoľuje štítok, farba, výšivka a elastické časti. Dávka a oplach ovplyvňujú omak reliéfu.",
    "product_text": "Hypoalergénny prací gél z marseillského mydla je konkrétna možnosť pre kompatibilné polo tričko alebo inú bežnú bielizeň. Použite dávku podľa vody a náplne a po praní hneď vytvarujte golier a švy.",
    "product_limit": "Produkt nenahrádza špeciálne ošetrenie nestálofarebnej výšivky, lepeného znaku, technickej povrchovej úpravy alebo odevu určeného na profesionálne čistenie. Neopraví vytiahnuté očko či poškodený golier.",
    "category_intro": "Pri piké vyberajte gél podľa zloženia a farby. Bavlna, polyester a elastan majú odlišné hranice, hoci reliéf na povrchu vyzerá rovnako.",
    "category_text": "V kategórii pracích gélov nájdete možnosti pre bežnú domácu bielizeň. Pred použitím skontrolujte štítok tela, goliera, manžiet a výšivky a neprekračujte odporúčanú dávku.",
    "related": [
        ("Čo je bavlna a ako sa o ňu starať", ARTICLE_COTTON),
        ("Čo je polyester a ako ho prať", ARTICLE_POLYESTER),
        ("Polyester verzus bavlna", ARTICLE_BLEND),
        ("Prečo sa oblečenie zráža", ARTICLE_SHRINKAGE),
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
    ],
    "faq_title": "piké, polo tričká a reliéfne úplety",
    "faq": [
        ("Čo je piké?", "Textilná konštrukcia s reliéfom, jamkami alebo rebrami. Najznámejšie polo piké je pletené, existujú však aj tkané varianty."),
        ("Je piké vždy bavlna?", "Nie. Môže byť bavlnené, polyesterové, zmesové a môže obsahovať elastan. Názov opisuje najmä štruktúru."),
        ("Je piké priedušné?", "Reliéf môže vytvárať vzduchové priestory, ale priedušnosť určuje aj hustota, priadza, gramáž, dokončenie a strih."),
        ("Ako prať piké polo tričko?", "Podľa štítku, obrátené a chránené pred suchými zipsami, v primeranej náplni. Po cykle urovnajte golier, manžety a švy."),
        ("Na koľko stupňov prať piké?", "Jedna teplota neexistuje. Rozhoduje zloženie, farba, elastan, výšivka a symbol na hotovom výrobku."),
        ("Prečo sa golier po praní vlní?", "Môže sa líšiť jeho zloženie a zrazenie od tela, alebo ho poškodili otáčky, teplo, mokré zavesenie či unavený elastan."),
        ("Môže ísť piké do sušičky?", "Iba pri výslovnom symbole. Vysoké teplo môže zraziť bavlnu, poškodiť elastan a zdeformovať golier."),
        ("Ako žehliť piké?", "Z rubu cez mäkkú podložku pri povolenej teplote a bez silného tlaku, ktorý by sploštil reliéf."),
        ("Ako odstrániť žmolky z piké?", "Opatrne na suchom povrchu a bez zásahu do očiek. Zároveň odstráňte zdroj trenia pri nosení alebo praní."),
    ],
}


LUREX: dict[str, object] = {
    "title": "Čo je lurex: metalická priadza, zatrhávanie a správna starostlivosť",
    "link": "co-je-lurex-metalicka-priadza-zatrhavanie-a-spravna-starostlivost",
    "meta": "Čo je Lurex a metalická priadza, z čoho vzniká lesk a ako prať, sušiť, žehliť a uložiť lesklý sveter, šaty či pančuchy bez zatrhnutia.",
    "short": "Lurex je registrovaná značka metalických a efektných priadzí, nie všeobecný názov jedného vlákna. Lesklá niť môže obsahovať metalizovaný polymérny film, podpornú priadzu aj ochranné vrstvy. Zistite, ako ju chrániť pred trením, teplom a agresívnou chémiou.",
    "name": "lurexová priadza",
    "genitive": "lurexovej priadze",
    "locative": "lurexovej priadzi",
    "construction_summary": "metalickú alebo efektnú priadzu, pri ktorej môže byť tenký metalizovaný polymérny pásik použitý samostatne, podopretý textilnou priadzou, ovinutý okolo jadra alebo zapletený do širšej konštrukcie",
    "label_details": "vláknové zloženie nosného úpletu či tkaniny, podiel metalickej priadze, typ lesklého filmu, výšivku, podšívku, flitre, koráliky, elastan, lepidlo a povolené žehlenie",
    "residue_place": "medzerách pri lesklej priadzi, pod ovinutím, v hustej pletenine, pri výšivke, v švoch a medzi dekoráciou a základnou látkou",
    "friction_risk": "tenký efektný pásik, ovinutie jadra, vystupujúcu slučku, výšivku alebo okraj flitra",
    "drying_advice": "Lesklý sveter, top alebo šaty vytvarujte naplocho, aby mokrá hmotnosť neťahala efektnú priadzu a elastické očká. Pančuchy a jemné doplnky rozložte bez kolíkov cez lesklé zóny; viacvrstvové ozdoby nechajte vetrať z oboch strán.",
    "heat_risk": "polymérny film, metalizovanú a farebnú vrstvu, lepidlo, podpornú priadzu, elastan, flitre a lesklý povrch",
    "failure_sign": "odlupovanie lesklej vrstvy, štiepenie priadze, posunuté ovinutie alebo vytiahnuté očko",
    "answer": "Lurex je registrovaná značka metalických a efektných priadzí. V bežnej reči sa týmto slovom označuje lesklá niť v svetri, šatách, pančuchách alebo výšivke, no konkrétna konštrukcia môže byť veľmi rozdielna. Lesk často vytvára tenký polymérny film s kovovou vrstvou, ktorý môže byť podopretý inou priadzou alebo ovinutý okolo jadra. Starostlivosť preto určuje celý výrobok a jeho štítok. Prateľný kus obráťte naruby, chráňte vo vhodnom vrecku, oddeľte od zipsov a suchých zipsov, použite nízke mechanické zaťaženie a kompatibilný prostriedok. Nekrúťte ho a sušte naplocho mimo prudkého tepla. Žehličku neprikladajte priamo na metalickú niť; použite ju len pri výslovnom povolení, z rubu a cez ochrannú tkaninu. Pri odlupovaní, černení alebo ostrých koncoch pranie zastavte.",
    "intro": "Lesklá nitka vyzerá ako kov, no nemusí byť z plného kovu. Moderná efektná priadza môže vzniknúť metalizáciou veľmi tenkého filmu, jeho zafarbením, narezaním na pásiky a spojením s textilným jadrom. Iný výrobok použije kovom potiahnuté vlákno, fóliu, flitre alebo iba pigmentovú potlač. Všetky sa pri svetle trblietajú, ale odlišne znášajú ohyb, pot, prací roztok a teplo. Dobrá starostlivosť preto nezačína otázkou koľko stupňov na lurex, ale určením nosnej látky, dekorácie a najcitlivejšej vrstvy.",
    "quick": [
        "<strong>Lurex je značka priadzí:</strong> nie každá metalická niť je výrobok rovnakej konštrukcie alebo odolnosti.",
        "<strong>Lesk môže vytvárať veľmi tenká vrstva:</strong> trenie, ohyb, chlór a teplo ju môžu meniť skôr než základný sveter.",
        "<strong>Celý odev určuje pranie:</strong> vlna, viskóza, polyester, elastan, flitre a lepidlo nemajú rovnaký limit.",
        "<strong>Ochrana pred zachytením je zásadná:</strong> zips, suchý zips a prsteň môžu vytiahnuť efektnú niť.",
        "<strong>Žehlička nepatrí priamo na lesk:</strong> pracujte len pri povolení z rubu cez ochrannú vrstvu.",
        "<strong>Škriabanie môže znamenať poškodenie:</strong> ostrý koniec priadze najprv stabilizujte, neodstrihujte ho bez posúdenia.",
    ],
    "overview_heading": "Čo je Lurex a z čoho vzniká metalický lesk",
    "overview": [
        "Lurex na svojej oficiálnej stránke uvádza históriu značky metalických priadzí od roku 1946 a opisuje využitie vákuovej a polymérnej technológie. Produktové portfólio zahŕňa nepodopreté, podopreté, jemné, pletacie, vyšívacie, gimpa a retiazkové priadze. Už tento zoznam ukazuje, že slovo neoznačuje jednu nemennú niť. Konštrukcia musí zodpovedať spôsobu tkania, pletenia, výšivky aj následného farbenia.",
        "Typická moderná metalická priadza môže používať polyesterový film s veľmi tenkou hliníkovou alebo inou metalickou vrstvou a ochranným či farebným lakom. Film sa reže na úzke pásiky a môže sa používať samostatne alebo s nosnou priadzou. Iné produkty kombinujú viskózu, polyamid, polyester alebo recyklované komponenty. Pri pohľade voľným okom sa však zloženie spoľahlivo určiť nedá.",
        "V hotovej látke tvorí metalická priadza často iba menšiu časť. Môže byť pravidelne vpletená do svetra, vytvárať pruh, lem, výšivku alebo celoplošný trblietavý efekt. Základná látka nesie hmotnosť a prijíma vodu, efektná priadza určuje časť vzhľadu a môže byť najcitlivejšia na trenie. Bezpečný cyklus musí vyhovovať obom.",
    ],
    "table1_heading": "Lesklé textílie, ktoré vyzerajú podobne, ale nie sú rovnaké",
    "table1_intro": "Pred čistením rozlíšte, či lesk vytvára priadza, plošná fólia, flitre alebo potlač. Domáci postup pre jednu skupinu sa neprenáša automaticky na druhú.",
    "table1_headers": ["Efekt", "Možná konštrukcia", "Ako ho spozorovať", "Hlavné riziko"],
    "table1_rows": [
        ("Lurexová alebo metalická priadza", "Tenký metalizovaný pásik samostatne, s oporou alebo ovinutý okolo jadra.", "Lesklá línia sleduje jednotlivé očká, osnovu, útok alebo výšivku.", "Zachytenie, odlupovanie vrstvy, posun ovinutia a teplo."),
        ("Lamé alebo celoplošná metalická látka", "Husté použitie kovových či metalizovaných priadzí alebo povrchový efekt.", "Veľká plocha pôsobí kovovo a môže byť tuhšia.", "Ostré zlomy, oder veľkej plochy a citlivá podšívka."),
        ("Flitre a fóliové aplikácie", "Samostatné plastové diely, transferová fólia alebo lepený motív.", "Lesk tvorí plošný prvok nad základnou textíliou.", "Lepidlo, poškriabanie, praskanie a odpadnutie."),
        ("Metalická potlač", "Pigment alebo vrstva nanesená na povrch.", "Lesk neprechádza ako samostatná niť cez rub.", "Oder, pranie z líca, vysoké teplo a chemická citlivosť."),
    ],
    "sections": [
        {
            "heading": "Značka Lurex verzus všeobecná metalická priadza",
            "paragraphs": [
                "Lurex je chránené obchodné označenie konkrétneho výrobcu, podobne ako iné značky priadzí. V bežnej reči sa názov používa širšie, ale pri technickom hodnotení treba rozlišovať značkový produkt od neurčenej metalickej nite. Dve priadze s rovnakým zlatým vzhľadom môžu mať odlišný film, kov, lak, podporné jadro a odolnosť pri farbení.",
                "Oficiálny výrobca ponúka aj špeciálne varianty odolné voči konkrétnym farbiacim a mokrým procesom. Existencia takýchto variantov zároveň dokazuje, že odolnosť nemožno predpokladať pri každej priadzi. Údaj z priemyselného technického listu sa navyše vzťahuje na spracovanie priadze, nie automaticky na domáci cyklus hotového svetra s vlnou a elastanom.",
            ],
        },
        {
            "heading": "Ako prať lurexový sveter bez vytiahnutia nití",
            "paragraphs": [
                "Skontrolujte zloženie základnej pleteniny, rebrované lemy, ramená a miesta, kde lesklá niť vystupuje na povrch. Všetky zipsy a háčiky v náplni zatvorte alebo oddeľte. Sveter obráťte naruby, vložte do dostatočne veľkého jemného vrecka a perte iba pri povolení etikety. Vrecko nesmie kus stlačiť do tvrdej gule.",
                "Vlnený alebo viskózový základ môže vyžadovať iný prípravok než bežná syntetika. Mokré očká netrite a sveter nezdvíhajte za rukáv. Po oplachu ho podoprite, vodu nechajte odtiecť a sušte naplocho v pôvodnom tvare. Lesklé pásiky nerozťahujte prstami, aj keď sa zdajú po namočení voľnejšie.",
            ],
        },
        {
            "heading": "Ako prať lurexové šaty, top a sukňu",
            "paragraphs": [
                "Šaty môžu kombinovať metalickú niť s podšívkou, zipsom, kostrou ramien a lepeným lemom. Najprv si prezrite rub pri dennom svetle a nájdite miesto, kde lesklá priadza prechádza švom. Ak sa konce uvoľňujú, pranie odložte do opravy. Tesný strih zvyšuje trenie pri podpazuší, bokoch a sede, preto porovnajte opotrebované a chránené zóny.",
                "Pri povolenom praní použite krátky šetrný cyklus s nízkym mechanickým zaťažením, nie preplnený bubon. Odev vyberte hneď a sušte podľa tvaru. Dlhé šaty podoprite na viacerých miestach; mokrá hmotnosť môže vyťahovať šev aj efektnú priadzu. Profesionálne čistenie z etikety neprepisujte iba preto, že základ obsahuje polyester.",
            ],
        },
        {
            "heading": "Pančuchy, ponožky a spodné vrstvy s metalickou niťou",
            "paragraphs": [
                "Jemné pančuchy a ponožky obsahujú elastan a tenké očká, ktoré sa zachytia ešte ľahšie než svetrovina. Pred praním odstráňte šperky, zapilujte ostrý necht a každý pár vložte do jemného vrecka. Neperte ho s podprsenkou s voľným háčikom, zipsom ani suchým zipsom. Vysoké otáčky a teplo skracujú návrat elastanu.",
                "Ak metalická niť škriabe pokožku, zistite, či ide o pôvodný omak alebo o uvoľnený ostrý koniec. Poškodené miesto nenoste priamo na podráždenej koži. Odstrihnutie môže uvoľniť ďalší úsek ovinutia. Jemnú slučku stabilizujte z rubu alebo zverte oprave; pri detskom odeve posudzujte aj riziko uvoľneného malého prvku.",
            ],
        },
        {
            "heading": "Pot, parfum, kozmetika a zmena metalického vzhľadu",
            "paragraphs": [
                "Pot prináša vodu, soli a kyslé zložky, kozmetika oleje, alkoholy, pigmenty a emulgátory. Na metalickej vrstve a jej ochrannom laku môžu pôsobiť inak než na nosnej priadzi. Odev po nosení vyvetrajte a pot nenechajte dlho koncentrovaný v podpazuší alebo pri golieri. Parfum nestriekajte priamo na lesklú plochu bez pokynu výrobcu.",
                "Matné alebo stmavnuté miesto nemusí byť pravá korózia kovu. Môže ísť o oder farebného laku, zvyšok produktu, mastný film, odlúčenie vrstvy alebo zmenu nosnej priadze. Najprv porovnajte rub, neopotrebované miesto a správanie pri jemnom svetle. Chemické leštenie kovu na textíliu nepatrí; môže rozpustiť film, farbu aj okolité vlákna.",
            ],
        },
        {
            "heading": "Zatrhnutie, rozmotanie a poškodené ovinutie",
            "paragraphs": [
                "Metalická priadza býva tuhšia než okolité textilné vlákna a na povrchu vytvára body, ktoré ľahšie zachytí drsný predmet. Keď sa vytiahne slučka, napätie sa prenesie do susedných očiek. Odev položte naplocho, slučku neťahajte a nesnažte sa ju zatlačiť ihlou z líca bez znalosti smeru pletenia.",
                f"Pri ovinutej priadzi sa môže metalický pásik posunúť a odhaliť jadro. Odstrihnutie voľného konca môže spustiť ďalšie rozmotávanie. Všeobecný mechanizmus zachytenia rozoberá článok <a href=\"{ARTICLE_SNAGGING}\">prečo vznikajú vytiahnuté očká</a>. Ak poškodenie leží v nosnom šve, odev pred ďalším nosením a praním opravte.",
            ],
            "callout": {
                "title": "Kedy lesklú niť neodstrihovať",
                "items": [
                    "Slučka pokračuje do viacerých očiek alebo cez šev.",
                    "Metalický pásik je ovinutý okolo textilného jadra a začína sa rozmotávať.",
                    "Poškodenie drží výšivku, lem, fliter alebo nosnú časť odevu.",
                    "Nie je jasné, či ide o voľný koniec, alebo o pretrhnutú súčasť priadze.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Teplo, žehlička, para a sušička",
            "paragraphs": [
                "Polymérny film a ochranný lak môžu mäknúť, zmršťovať sa alebo meniť lesk pri teplote, ktorú bavlnený základ ešte znáša. Žehličku preto neprikladajte priamo na metalickú priadzu. Ak etiketa povoľuje žehlenie, pracujte z rubu cez čistú ochrannú tkaninu pri najnižšom vhodnom stupni a najprv skúste vnútorný lem.",
                "Para môže kondenzovať pod ovinutím a pri lepených ozdobách. Sušička pridáva aj prevaľovanie, ktoré lesklú niť opakovane ohýba. Použite ju iba pri jednoznačnom symbole pre celý výrobok. Radiátor a fén nie sú kontrolovaná náhrada. Keď sa lesk začne zvlňovať alebo lepiť, materiál nechajte vychladnúť bez ďalšieho dotyku.",
            ],
        },
        {
            "heading": "Bielenie, kyseliny, zásady a nevhodné domáce pokusy",
            "paragraphs": [
                "Chlórové bielidlo môže poškodiť farbu, kovovú vrstvu, ochranný lak aj nosné vlákno. Silné kyseliny a zásady majú odlišný vplyv na vlnu, viskózu, polyamid a polyester. Oficiálny výrobca uvádza špeciálne priadze navrhnuté na odolnosť voči konkrétnym farbiacim procesom; z toho nemožno vyvodiť odolnosť bežného odevu voči domácej chémii.",
                "Ocot, sóda, peroxid, alkohol alebo odlakovač nepoužívajte ako univerzálnu opravu zašlého lesku. Môžu vytvoriť matnú mapu, odlepiť povrch alebo zmeniť okolité farbivo. Pri škvrne najprv identifikujte základnú látku a najcitlivejšiu dekoráciu, urobte skrytú skúšku a pri hodnotnom kuse zvoľte odborné čistenie.",
            ],
        },
        {
            "heading": "Ako skladovať lurexový sveter a spoločenské šaty",
            "paragraphs": [
                "Sveter ukladajte čistý a úplne suchý naplocho, bez ťažkého predmetu na lesklom povrchu. Medzi kusy vložte hladkú priedušnú vrstvu, aby zips alebo fliter susedného odevu nezachytil priadzu. Nevešajte ťažkú pleteninu dlhodobo za ramená. Miesto preloženia občas zmeňte, aby tenký film nebol stále lámaný v tej istej línii.",
                "Spoločenské šaty zaveste na širokú oporu so závesnými pútkami, ak to hmotnosť povoľuje, alebo ich uložte naplocho. Ozdoby oddeľte od metalickej plochy. Plastový vak môže uzavrieť vlhkosť a tlak; na dlhodobé uloženie zvoľte čistý priedušný obal. Pred nosením odev vyberte s predstihom a záhyby neuvoľňujte náhlym horúcim naparovaním.",
            ],
        },
        {
            "heading": "Ako vybrať lesklý odev, ktorý nebude škriabať",
            "paragraphs": [
                "Prejdite dlaňou po líci aj rube a jemne ohnite látku. Ostrý omak môže vytvárať hrubší metalický pásik, poškodené ovinutie alebo konce pri šve. Podšívka znižuje priamy kontakt, ale pridáva ďalší materiál s vlastným zrazením. Pri tesnom odeve skúste pohyb ramien, sedenie a trenie v podpazuší, kde sa priadza najviac namáha.",
                "Pozrite sa na povrch pod bočným svetlom. Kvalitná efektná priadza má pôsobiť rovnomerne bez voľných slučiek a odlupujúcich sa miest. Skontrolujte čitateľnú etiketu a spôsob prania ešte pred nákupom. Označenie metalický vzhľad nehovorí, či ide o značkovú priadzu, fóliu, pigment alebo flitre, preto má technický popis väčšiu hodnotu než samotný názov farby.",
            ],
        },
    ],
    "table2_heading": "Metalická priadza po praní: diagnostika zmeny",
    "table2_intro": "Lesk sa môže zmeniť pre zvyšok produktu, oder, teplo, chemickú reakciu alebo mechanické poškodenie. Nepoužívajte leštenie kovu na textilný výrobok.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Lesk je matný iba na namáhanom mieste", "Oder laku alebo filmu pri nosení či praní.", "Podpazušie, bok, pás, šev a porovnanie s rubom.", "Obmedziť trenie; nečistiť leštidlom ani tvrdou kefou."),
        ("Na povrchu je klzká alebo biela mapa", "Nadbytok produktu, nedostatočný oplach alebo zmena lepidla.", "Dávku, náplň, stav po úplnom vysušení a rub.", "Pri povolení opláchnuť; pri lepení zásah zastaviť."),
        ("Priadza vytvára ostrý koniec", "Pretrhnutie pásika, posun ovinutia alebo poškodenie šva.", "Kam niť pokračuje a či nesie okolité očká.", "Neodstrihovať naslepo; stabilizovať z rubu alebo opraviť."),
        ("Lesk sa zvlňuje alebo lepí", "Príliš vysoké teplo, rozpúšťadlo alebo degradácia polymérneho filmu.", "Žehličku, sušičku, radiátor a použitú chémiu.", "Nechať vychladnúť, nepridávať teplo a zdokumentovať poškodenie."),
        ("Farba lesku sa prenáša", "Nestálofarebný lak, pigment, chemická reakcia alebo oder.", "Skrytú skúšku bielou handričkou a etiketu.", "Oddeliť od inej bielizne a zvoliť odborné posúdenie."),
    ],
    "steps_heading": "Ako bezpečne vyprať odev s lurexovou priadzou krok za krokom",
    "steps": [
        "Určite základnú látku, podiel a umiestnenie metalickej priadze, podšívku, elastan, flitre a lepidlo.",
        "Prečítajte všetky symboly a skontrolujte voľné slučky, ostré konce, švy a prenos farby na skrytom mieste.",
        "Oddeľte odev od zipsov, háčikov, suchých zipsov a drsných textílií a použite primerane veľké ochranné vrecko.",
        "Zvoľte iba povolené čistenie a kompatibilný prostriedok podľa najcitlivejšieho vlákna a dekorácie.",
        "Použite nízke mechanické zaťaženie, presnú dávku a dôkladný oplach bez krútenia alebo kefovania lesku.",
        "Mokrý kus vyberte s rovnomernou oporou, vytvarujte ho a sušte naplocho mimo prudkého tepla.",
        "Žehlite len pri výslovnom povolení z rubu cez ochrannú tkaninu a nikdy priamo na metalickú niť.",
        "Úplne suchý odev uložte bez ostrého preloženia a oddeľte ho od kovania a drsných ozdôb.",
    ],
    "remember": [
        "Je lesk tvorený priadzou, fóliou, flitrami alebo potlačou?",
        "Aké vlákno tvorí základ, podšívku a podporu metalickej priadze?",
        "Sú pri švoch voľné slučky, ostré konce alebo odlupujúca sa vrstva?",
        "Povoľuje etiketa vodu, konkrétny prostriedok, sušičku, paru a žehlenie?",
        "Je odev chránený pred zipsami, suchými zipsami, šperkami a drsným povrchom?",
        "Je pred uložením suchá aj podšívka, výšivka a priestor pod ozdobami?",
    ],
    "mistakes": [
        "Považovať každý metalický vzhľad za rovnakú priadzu s rovnakou odolnosťou.",
        "Prať lesklý sveter s otvoreným zipsom, suchým zipsom alebo háčikmi podprsenky.",
        "Odstrihnúť vytiahnutú lesklú slučku a spustiť ďalšie rozmotávanie.",
        "Použiť chlórové bielidlo, ocot, sódu alebo leštidlo bez overenia konštrukcie.",
        "Priložiť žehličku priamo na metalickú niť alebo fliter.",
        "Zavesiť mokrú ťažkú pleteninu za ramená a uložiť ju vlhkú pri drsných ozdobách.",
    ],
    "expert_heading": "Odbornejší pohľad: metalizovaný film, podporná priadza a skúšky",
    "expert": [
        "Vákuová metalizácia umožňuje naniesť veľmi tenkú kovovú vrstvu na polymérny film. Následný lak alebo farba upravia odtieň a ochranu, film sa nareže a môže sa skombinovať s textilnou priadzou. Mechanické vlastnosti preto závisia od hrúbky filmu, šírky pásika, typu kovu, adhézie vrstiev a podpory. Vzhľad podobný plnému kovu nevypovedá o tepelnej odolnosti.",
        "Oficiálne portfólio Lurex rozlišuje nepodopreté, podopreté, jemné, pletacie, vyšívacie a ďalšie priadze. Výrobca ponúka aj varianty určené pre špecifické mokré a farbiace procesy. Odborný záver je preto opačný než univerzálne tvrdenie: kompatibilita sa musí dokázať pre konkrétnu priadzu a proces. Spotrebiteľ sa riadi štítkom hotového odevu.",
        "ASTM D3939 hodnotí zachytávanie a AATCC TM61 zmenu farby a povrchu pri definovanom praní. Ani jedna metóda sama neurčuje komfort na koži, adhéziu každej metalickej vrstvy alebo život šva. Pri lesklom odeve treba oddelene posudzovať mechanické zachytenie, farebnú zmenu, teplo, základnú textíliu a dekorácie. Jediný údaj odolný voči praniu bez metódy a počtu cyklov je neúplný.",
    ],
    "source_intro": "Zdroje podporujú históriu a rozmanitosť Lurex priadzí, existenciu špecifických procesne odolných variantov, normované hodnotenie zachytenia, farby a význam štítku. Nezaručujú rovnakú starostlivosť pre každý lesklý odev.",
    "sources": [
        ("Lurex: história značky a metalických priadzí", LUREX_ABOUT),
        ("Lurex: prehľad konštrukcií efektných priadzí", LUREX_PRODUCTS),
        ("Lurex: priadze navrhnuté pre špecifické mokré procesy", LUREX_DYE),
        ("ASTM D3939/D3939M-26: odolnosť proti zachyteniu", ASTM_SNAG),
        ("AATCC TM61: zrýchlená stálofarebnosť pri praní", AATCC_COLOR),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "product_intro": "Bežný prací gél môže byť vhodný iba pre prateľný odev s kompatibilnou základnou látkou a metalickou priadzou. Vlna, hodváb, flitre, fólia, lepidlo alebo profesionálne čistenie môžu vyžadovať iný produkt.",
    "product_text": "Hypoalergénny prací gél z marseillského mydla je konkrétna možnosť pre kompatibilnú bežnú bielizeň. Pri metalickej priadzi použite presnú dávku, nízke trenie a dostatočný oplach bez kontaktu s drsnými kusmi.",
    "product_limit": "Produkt nie je automaticky vhodný na vlnu, hodváb, fóliovú potlač, lepené flitre, nestálofarebný lak alebo profesionálne čistený odev. Neobnoví odlúpnutú metalickú vrstvu ani rozmotanú priadzu.",
    "category_intro": "Pri výbere gélu má prednosť základné vlákno a najslabšia dekorácia. Lesk nie je dôvod na silnejšiu chémiu ani vyššiu dávku.",
    "category_text": "V kategórii pracích gélov nájdete produkty pre bežnú domácu bielizeň. Pred použitím porovnajte ich určenie so štítkom nosnej látky, metalickej priadze, výšivky a elastických častí.",
    "related": [
        ("Ako predchádzať zatrhávaniu textilu", ARTICLE_SNAGGING),
        ("Stálofarebnosť textilu", ARTICLE_COLOR),
        ("Čo je polyester a ako ho prať", ARTICLE_POLYESTER),
        ("Prečo sa oblečenie žmolkuje", ARTICLE_PILLING),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Ako vybrať prací gél", ARTICLE_GEL),
    ],
    "faq_title": "Lurex a metalické priadze",
    "faq": [
        ("Čo je Lurex?", "Registrovaná značka metalických a efektných priadzí. V bežnej reči sa názov používa aj širšie pre lesklé nite."),
        ("Je lurex kov?", "Lesk často vytvára veľmi tenká kovová vrstva na polymérnom filme, ktorý môže byť spojený s textilnou nosnou priadzou."),
        ("Môže sa lurex prať v práčke?", "Iba ak to povoľuje štítok celého odevu. Potrebná je ochrana pred zachytením, nízke trenie a kompatibilný prostriedok."),
        ("Na koľko stupňov prať lurex?", "Jedna teplota neexistuje. Rozhoduje základné vlákno, film, lak, podporná priadza, elastan, ozdoby a etiketa."),
        ("Môže ísť lurex do sušičky?", "Len pri výslovnom symbole. Teplo a prevaľovanie môžu poškodiť film, lak, elastan aj nosnú pleteninu."),
        ("Ako žehliť metalickú priadzu?", "Nikdy nie priamo. Pri povolení pracujte z rubu cez ochrannú tkaninu s najnižším vhodným teplom a krátkym kontaktom."),
        ("Prečo lurex po praní škriabe?", "Mohol sa pretrhnúť pásik, posunúť ovinutie alebo poškodiť šev. Ostrý koniec neodstrihujte bez posúdenia pokračovania priadze."),
        ("Prečo metalický lesk stmavol?", "Môže ísť o oder laku, zvyšok produktu, mastnotu, chemickú zmenu alebo odlupovanie. Nepoužívajte leštidlo na kov."),
        ("Ako skladovať lurexový sveter?", "Úplne suchý naplocho, bez ostrého preloženia a oddelený hladkou vrstvou od zipsov, flitrov a iných drsných ozdôb."),
    ],
}


ARTICLES: list[dict[str, object]] = [ORGANZA, SCUBA, PIQUE, LUREX]


def preflight_links(articles: list[dict[str, object]]) -> dict[str, object]:
    target_urls = {f"{BASE}/n/{article['link']}" for article in articles}
    outgoing_urls = {
        urljoin(BASE, href) if href.startswith("/") else href
        for article in articles
        for href in article_hrefs(str(article["long"]))
    }
    with ThreadPoolExecutor(max_workers=6) as executor:
        checks = list(executor.map(fetch_status, sorted(target_urls | outgoing_urls)))
    for check in checks:
        if check["url"] in target_urls:
            check["expected_status"] = 404
            check["ok"] = check["status"] == 404
    report = {
        "batch": "batch-47",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "target_count": len(target_urls),
        "outgoing_count": len(outgoing_urls),
        "check_count": len(checks),
        "failure_count": sum(not check["ok"] for check in checks),
        "checks": checks,
    }
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    candidate_titles = [line.strip() for line in CANDIDATES.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    article_by_title = {str(article["title"]): article for article in ARTICLES}
    if len(article_by_title) != len(ARTICLES) or set(candidate_titles) != set(article_by_title):
        raise SystemExit("Candidate titles and article definitions do not match exactly")
    slugs = [str(article["link"]) for article in ARTICLES]
    if len(slugs) != len(set(slugs)):
        raise SystemExit("Batch contains duplicate slugs")

    rendered: list[dict[str, object]] = []
    metrics: list[dict[str, object]] = []
    for article in ARTICLES:
        body = render_article(article)
        old_damage = f"Prach alebo piesok možno často odstrániť nasucho, škvrna vyžaduje lokálne ošetrenie, pach potrebuje odstrániť zdroj a {article['failure_sign']} je mechanická chyba."
        new_damage = f"Prach alebo piesok možno často odstrániť nasucho, škvrna vyžaduje lokálne ošetrenie a pach potrebuje odstrániť zdroj. Prejav ako {article['failure_sign']} je mechanické poškodenie."
        old_stop = f"Zásah zastavte, ak sa uvoľňuje farba, povrch sa lepí, vrstva sa oddeľuje, {article['failure_sign']} alebo etiketa vyžaduje odborný postup."
        new_stop = f"Zásah zastavte, ak sa uvoľňuje farba, povrch sa lepí alebo vrstva sa oddeľuje, ak vidíte {article['failure_sign']}, alebo ak etiketa vyžaduje odborný postup."
        body = body.replace(old_damage, new_damage).replace(old_stop, new_stop)
        body = body.replace(f"Pri {article['genitive']}", f"Pri {article['locative']}")
        body = body.replace(f"pri {article['genitive']}", f"pri {article['locative']}")
        body = body.replace(f"Pri {article['name']}", f"Pri {article['locative']}")
        body = body.replace(f"pri {article['name']}", f"pri {article['locative']}")
        body = body.replace("scuba úpletee", "scuba úplete")
        public_text = f"{article['title']} {article['short']} {body}"
        visible = visible_text(body)
        if FORBIDDEN_PUBLIC_RE.search(public_text):
            raise SystemExit(f"Forbidden public wording in {article['title']}")
        if FIXED_PRICE_RE.search(visible_text(public_text)):
            raise SystemExit(f"Fixed price found in {article['title']}")
        one_character_paragraphs = [
            visible_text(value).strip()
            for value in re.findall(r"<p\b[^>]*>(.*?)</p>", body, flags=re.IGNORECASE | re.DOTALL)
            if len(visible_text(value).strip()) == 1
        ]
        metric = {
            "title": article["title"],
            "slug": article["link"],
            "words": len(WORD_RE.findall(visible)),
            "h2": len(re.findall(r"<h2\b", body, re.IGNORECASE)),
            "tables": len(re.findall(r"<table\b", body, re.IGNORECASE)),
            "responsive_tables": len(re.findall(r'<div\b[^>]*style="[^"]*overflow-x:\s*auto', body, re.IGNORECASE)),
            "styled_blocks": len(re.findall(r"<div\b[^>]*style=", body, re.IGNORECASE)),
            "action_buttons": len(re.findall(r'<a\b[^>]*style="[^"]*display:\s*inline-block', body, re.IGNORECASE)),
            "one_character_paragraphs": len(one_character_paragraphs),
        }
        if metric["words"] < 2800:
            raise SystemExit(f"Article is too short: {article['title']} ({metric['words']} words)")
        if metric["h2"] < 24 or metric["tables"] < 2 or metric["responsive_tables"] != metric["tables"]:
            raise SystemExit(f"Article structure is incomplete: {article['title']} ({metric})")
        if metric["styled_blocks"] < 10 or metric["action_buttons"] < 2 or metric["one_character_paragraphs"]:
            raise SystemExit(f"Article visual blocks are incomplete: {article['title']} ({metric})")
        metrics.append(metric)
        rendered.append({
            "title": article["title"],
            "short": article["short"],
            "long": body,
            "link": article["link"],
            "date_posted": PUBLISH_DATE,
            "time_posted": "15:00:00",
            "commenting": False,
            "title_tag": article["title"],
            "description": article["meta"],
        })

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(rendered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = preflight_links(rendered)
    if report["failure_count"]:
        failed = [check for check in report["checks"] if not check["ok"]]
        print(json.dumps({"failed_links": failed}, ensure_ascii=False, indent=2))
        raise SystemExit("Batch 47 link preflight failed")
    print(json.dumps({"article_count": len(rendered), "metrics": metrics, "link_preflight": True}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
